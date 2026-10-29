"""Part 22: know before they do. The 06:00 rule, the SLO SQL and its error budget on hand-made inputs, and the event
adapter for OpenMetadata run without a stack. The event-file tests check what each OpenLineage integration emitted
(they skip until `spark-lineage`, `dbt-lineage` and a DAG run have written the files); the lineage test needs
OpenMetadata (`make up-lineage`, then `om-setup` and `replay`)."""

import socket
from collections import Counter
from datetime import UTC, date, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from scripts import part22_observe as lab

E = date(2026, 10, 4)  # the upload day the 5 October 06:00 check expects


def test_expected_day_follows_the_london_clock():
    london = lab.LONDON
    assert lab.expected_day(datetime(2026, 10, 5, 5, 59, tzinfo=london)) == date(2026, 10, 3)  # before the deadline
    assert lab.expected_day(datetime(2026, 10, 5, 6, 0, tzinfo=london)) == E
    assert lab.expected_day(datetime(2026, 10, 5, 5, 0, tzinfo=UTC)) == E  # 06:00 BST
    assert lab.expected_day(datetime(2026, 10, 29, 6, 0, tzinfo=UTC)) == date(2026, 10, 28)  # 06:00 GMT, after DST


def slo_inputs(publish_today: str, fail_today: bool, recorded: list[tuple], events_today: int) -> list[tuple]:
    """28 days of history: every day published and every control passing, except what the arguments change."""
    first = E - timedelta(days=27)
    days = [first + timedelta(days=i) for i in range(28)]
    con = duckdb.connect()
    con.execute(f"SET VARIABLE expected_day = DATE '{E}'")
    con.execute(f"SET VARIABLE first_day = DATE '{first}'")
    through = E if publish_today == "success" else E - timedelta(days=1)  # the published snapshot's upload day
    con.execute(f"SET VARIABLE published_through = DATE '{through}'")
    pubs = [{"upload_date": d, "state": publish_today if d == E else "success"} for d in days]
    con.register("publishes", pa.Table.from_pylist(pubs))
    controls = []
    for d in days:
        status = "FAIL" if (d == E and fail_today) else "pass"
        controls.append({"run_day": d, "check_name": "ledger_vs_gold_sales", "scope_date": d - timedelta(days=1),
                         "country": "GB", "source_count": 10, "rule": "equal", "status": status})  # fmt: skip
        n = events_today if d == E else 6000
        controls.append({"run_day": d, "check_name": "raw_vs_dispositions", "scope_date": d, "country": "GB",
                         "source_count": n, "rule": "equal", "status": "pass"})  # fmt: skip
    con.register("controls", pa.Table.from_pylist(controls))
    rec = [{"expected_day": d, "sli": s, "status": st} for d, s, st in recorded]
    con.register("recorded", pa.Table.from_pylist(rec, schema=lab.RECORDED))
    cols = [{"column_name": c, "expected": t, "actual": t} for c, t in lab.contract_columns().items()]
    con.register("contract", pa.Table.from_pylist(cols))
    return con.sql((lab.ROOT / "sql/part22/slo.sql").read_text()).fetchall()


def by_sli(rows: list[tuple]) -> dict:
    return {r[0]: r for r in rows}


def test_a_held_publish_breaches_freshness_and_completeness_but_not_volume():
    rows = by_sli(slo_inputs("upstream_failed", True, [], 5990))
    assert rows["freshness"][2:6] == ("FAIL", 27, 28, 0)  # status, good days, window, budget left
    assert rows["completeness"][2:6] == ("FAIL", 27, 28, 0)
    assert "ledger_vs_gold_sales" in rows["completeness"][7]
    assert rows["volume"][2] == "pass"  # -0.2%: a single till is noise to a volume check
    assert rows["schema"][2] == "pass"


def test_a_recorded_breach_stays_in_the_budget_after_the_fix():
    """The day was fixed (published now), but its 06:00 check had recorded a FAIL: the budget keeps it."""
    rows = by_sli(slo_inputs("success", False, [(E, "freshness", "FAIL")], 6000))
    assert rows["freshness"][2:7] == ("pass", 27, 28, 0, 1)  # passing now, one day spent, one day measured
    assert rows["completeness"][2:6] == ("pass", 28, 28, 1)


def test_volume_flags_a_missing_region():
    assert by_sli(slo_inputs("success", False, [], 4500))["volume"][2] == "FAIL"  # -25%


def test_contract_columns_are_version_2():
    assert lab.contract_columns() == {
        "country": "varchar", "business_date": "date", "sales_minor": "bigint", "refunds_minor": "bigint",
        "net_minor": "bigint", "report_currency": "varchar", "is_provisional": "boolean",
    }  # fmt: skip


def test_normalise_adapts_what_openmetadata_207_refuses():
    fixes: Counter = Counter()
    e = {
        "eventTime": "2026-10-29T07:01:02.251526+00:00",
        "inputs": [{"name": "x", "facets": {"schema": {"fields": [{"name": "a"}]}}}],
        "outputs": [{"name": "y", "facets": {"schema": {"fields": [{"name": "b", "type": "bigint"}]}}}],
    }
    out = lab.normalise(e, fixes)
    assert out["eventTime"] == "2026-10-29T07:01:02.251526Z"
    assert "schema" not in out["inputs"][0]["facets"] and "schema" in out["outputs"][0]["facets"]
    assert sum(fixes.values()) == 2


def needs_events(*names: str):
    missing = [n for n in names if not lab.events(n)]
    return pytest.mark.skipif(bool(missing), reason=f"no event file yet: {', '.join(missing)}")


@needs_events("dbt.jsonl")
def test_dbt_ol_column_lineage_stops_at_ctes_and_skips_microbatch_models():
    evs = lab.events("dbt.jsonl")
    with_cll = {d["name"].split(".")[-1] for e in evs for d in e.get("outputs", []) if "columnLineage" in d["facets"]}
    outputs = {d["name"].split(".")[-1] for e in evs for d in e.get("outputs", [])}
    assert {"fct_sales_line", "fct_basket"} <= outputs - with_cll  # microbatch models: no column lineage
    net = lab.column_lineage(evs, "rpt_daily_net_revenue_v2")["net_minor"]["inputFields"]
    assert {f["name"] for f in net} == {"money", "lakehouse_pipeline.gold.dim_fx_rate"}  # a CTE, named as a dataset
    roots = {e["run"]["facets"]["parent"]["root"]["job"]["name"] for e in evs if "parent" in e["run"]["facets"]}
    assert roots == {"larkspur_daily_revenue"}  # every dbt-ol run hangs under the Airflow DAG run


@needs_events("spark.jsonl")
def test_spark_listener_on_spark_41_emits_iceberg_lineage():
    evs = lab.events("spark.jsonl")
    assert {e["run"]["facets"]["processing_engine"]["version"] for e in evs} == {"4.1.3"}
    out = next(
        d for e in evs if e["eventType"] == "COMPLETE" for d in e.get("outputs", []) if "columnLineage" in d["facets"]
    )
    assert out["facets"]["symlinks"]["identifiers"][0]["name"] == "gold.fct_pos_line"
    kinds = {t["type"] for f in out["facets"]["columnLineage"]["dataset"] for t in f["transformations"]}
    assert kinds == {"INDIRECT"}  # filters, joins and windows: dataset-level
    assert "version" in out["facets"]  # an Iceberg snapshot ID


def openmetadata_up() -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("localhost", 8585)) == 0


@pytest.mark.skipif(not openmetadata_up(), reason="needs `make up-lineage`, then `om-setup` and `replay`")
def test_openmetadata_traces_stock_to_the_raw_snapshots():
    ups = lab.upstream("gold.fct_stock_daily", lab.om_token())
    assert "raw.stock_snapshots" in {s for edges in ups.values() for s, _, _ in edges}
    cll = [c for _, _, cl in ups["silver.stg_stock_snapshots"] for c in cl]
    assert any(c["toColumn"].endswith("stg_stock_snapshots.on_hand_qty") for c in cll)
