"""Part 24: what the post claims about metering, the cost model and the three optimisations.

Needs `make up-trino` (Trino as user `lab`), SeaweedFS's metrics port (compose: 9327), `make dbt-install` and the
tables of Parts 16-23. The module fixture meters storage, one daily run (upload day 4 October, which Part 19's runs
already built: re-running it changes nothing) and the read workload, then runs optimisations 1 and 2 and the model.
Everything is saved under data/lab/part24/test-*, never over the post's measurements. The sweep (optimisation 3) only
lists in the fixture; one test makes it refuse to delete, two check the leaks it found are closed.
"""

import json
import sys
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from scripts import part09_catalog as cat9
from scripts import part24_cost as lab
from tests.conftest import needs_batch

dbt_v1 = pytest.mark.skipif(not (Path(sys.executable).parent / "dbt").exists(), reason="needs `make dbt-install`")
pytestmark = [needs_batch, dbt_v1]

DAY = date(2026, 10, 4)
FOCUS_COLUMNS = {  # FOCUS 1.4 Cost and Usage columns the rows use; anything else must carry the x_ prefix
    "BillingPeriodStart", "BillingPeriodEnd", "ChargePeriodStart", "ChargePeriodEnd", "ChargeCategory",
    "ChargeDescription", "ServiceProviderName", "ServiceName", "ServiceCategory", "ResourceId", "ConsumedQuantity",
    "ConsumedUnit", "PricingQuantity", "PricingUnit", "ListCost", "ContractedCost", "BilledCost", "EffectiveCost",
    "BillingCurrency", "Tags",
}  # fmt: skip


@pytest.fixture(scope="module", autouse=True)
def own_labels():
    lab.PREFIX = "test-"
    yield
    lab.PREFIX = ""


@pytest.fixture(scope="module")
def run() -> dict:
    try:
        lab.s3_requests()
    except OSError:
        pytest.skip("needs SeaweedFS's metrics port 9327 (recreate it: docker compose --profile core up -d seaweedfs)")
    out = {"storage": lab.storage(), "daily": lab.daily(DAY), "reads": lab.reads()}
    out["pruning"] = lab.pruning()
    out["incremental"] = lab.incremental(DAY)
    out["model"] = lab.model(after=False)
    out["sweep"] = lab.sweep(apply=False)
    return out


def test_storage_classes_add_up_to_the_buckets(run):
    total = sum(n for _, n, _ in lab.bucket_objects())
    assert abs(sum(run["storage"]["bytes"].values()) - total) < 0.01 * total  # the daily run wrote a little since


def test_daily_run_is_metered_per_step_and_model(run):
    steps = {s["step"]: s for s in run["daily"]["steps"]}
    assert set(steps) == {"ingest", "dbt build", "checks", "publish"}
    build = steps["dbt build"]["per_owner"]
    assert {"fct_sales_line", "rpt_daily_net_revenue_v2", "tests"} <= set(build)
    assert build["tests"]["read_bytes"] > build["fct_sales_line"]["read_bytes"]  # the tests read all of history


def test_local_date_function_reads_every_file_and_date_key_prunes(run):
    p = run["pruning"]
    assert p["same"] and p["stores"] > 150
    assert p["after"]["read_bytes"] * 5 < p["before"]["read_bytes"]
    assert p["after"]["cpu_s"] < p["before"]["cpu_s"]


def test_incremental_report_rebuilds_exactly_its_window(run):
    o = run["incremental"]
    assert o["diff_after_rebuild"] == []  # the rebuild equals production's report
    c = o["incremental_check"]
    assert 0 < c["still_damaged"] == c["outside_window"] < c["rows"]  # the window was put right, nothing else touched
    assert o["report_incremental"]["trino"]["read_bytes"] * 5 < o["report_rebuilt"]["trino"]["read_bytes"]


def test_tests_scoped_to_the_run_read_less(run):
    o = run["incremental"]
    assert o["tests_window"]["trino"]["read_bytes"] * 3 < o["tests_all"]["trino"]["read_bytes"]


def test_model_writes_focus_columns(run):
    table = pq.read_table(lab.LAB / "test-focus-before.parquet")
    names = set(table.column_names)
    assert names - FOCUS_COLUMNS == {"x_ModelLabel"}
    rows = table.to_pylist()
    assert {r["ChargeCategory"] for r in rows} == {"Usage"}
    assert {r["ServiceCategory"] for r in rows} <= {"Storage", "Analytics"}
    assert all(isinstance(json.loads(r["Tags"]), dict) for r in rows)
    assert abs(sum(r["EffectiveCost"] for r in rows) - run["model"]["total_usd"]) < 1e-6


def test_prices_are_inputs(run):
    prices = json.loads((lab.COST / "prices.json").read_text())["prices"]
    prices["scan_tb"]["usd"] *= 2
    doubled = lab.model(after=False, prices=prices, write=False)
    scan = sum(x[4] for x in run["model"]["lines"] if x[0].startswith("scan"))
    assert abs(doubled["total_usd"] - run["model"]["total_usd"] - scan) < 1e-6


def test_sweep_lists_only_folders_no_live_table_uses(run):
    folders = lab.load("storage-before-sweep")["ownerless_folders"]
    tables, views = lab.catalog_objects()
    assert not set(folders) & (set(tables) | set(views))


def test_dropping_a_trino_table_removes_its_folder():
    lab.trino("CREATE SCHEMA IF NOT EXISTS lakehouse.part24")
    lab.trino("DROP TABLE IF EXISTS lakehouse.part24.probe")
    lab.trino("CREATE TABLE lakehouse.part24.probe AS SELECT 1 AS x")
    folder = cat9.call("GET", f"{cat9.CAT}/namespaces/part24/tables/probe")[1]["metadata"]["location"]
    folder = folder.removeprefix("s3://")
    assert "probe-" in folder  # Trino's folder name: table name + UUID
    cat9.drop_table("part24", "probe", cat9.token())
    fs = cat9.lake_fs()
    fs.invalidate_cache()
    files = [k for k, v in fs.find(folder, detail=True).items() if v["type"] == "file"]
    assert files == []  # SeaweedFS may keep the empty folder itself as a directory entry: no object, no bytes


def test_sweep_refuses_to_delete_when_a_reference_query_fails(monkeypatch):
    real = lab.trino

    def failing(sql, *a, **k):  # as if OPA denied listing the gold tables' current files
        if "$files" in sql and ".gold." in sql:
            raise SystemExit("Trino: Access Denied: Cannot select from table (simulated)")
        return real(sql, *a, **k)

    def no_deletes(*a, **k):
        raise AssertionError("the sweep tried to delete")

    fs = cat9.lake_fs()
    assert cat9.lake_fs() is fs  # one shared file system object, so the patch below covers the sweep's
    monkeypatch.setattr(lab, "trino", failing)
    monkeypatch.setattr(fs, "rm", no_deletes)
    with pytest.raises(SystemExit, match="not sweeping"):
        lab.sweep(apply=True)


def test_dropping_a_dev_view_removes_its_folder():
    from scripts import part18_dbt as p18

    lab.trino("CREATE OR REPLACE VIEW lakehouse_dev.dev.p24_probe AS SELECT 1 AS x", user="dbt_dev")
    view = cat9.call("GET", f"{cat9.CAT}/namespaces/dev/views/p24_probe")[1]
    folder = view["metadata-location"].removeprefix("s3://").rsplit("/", 2)[0]
    p18.empty_dev(["p24_probe"])
    fs = cat9.lake_fs()
    fs.invalidate_cache()
    assert [k for k, v in fs.find(folder, detail=True).items() if v["type"] == "file"] == []


def test_history_trino_cant_read_comes_from_pyiceberg(run):
    if "part08.daily_store_sales" not in lab.catalog_objects()[0].values():
        pytest.skip("needs Part 8's table")
    store = run["storage"]
    assert store["failures"] == []
    assert any(f.startswith("part08.daily_store_sales") for f in store["history_read_with_pyiceberg"])
