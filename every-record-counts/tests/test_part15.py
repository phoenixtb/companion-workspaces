"""Part 15: what the post claims about ingesting supplier files and the sellers API with dlt into Iceberg.

The module fixtures reset Part 15's tables and dlt state, replay the post's sequence (two supplier runs to
2026-08-19, the renamed column refused, the alias run to 2026-10-04, the checks), then two pulls of the mock sellers
API with and without a lag window and the reconciliation. Every count is compared with the generator's manifest
or with the files on disk, not with constants copied from the post.
"""

import csv
from datetime import date

import openpyxl
import pytest

from datagen import sellers as world
from datagen.suppliers import DMY_SUPPLIERS, RENAME_FROM, RENAMING_SUPPLIER
from scripts import part15_ingest as lab
from scripts import part15_sellers_api as api
from tests.conftest import ROOT, needs_stack

pytestmark = [needs_stack, pytest.mark.filterwarnings("ignore:Delete operation did not match any records")]
CUTOFF = date(2026, 8, 19)
FILES = ROOT / "data/raw/suppliers/deliveries"


def rows_in(path) -> int:
    if path.suffix == ".csv":
        with path.open(newline="") as f:
            return sum(1 for _ in csv.reader(f)) - 1
    return openpyxl.load_workbook(path, read_only=True).active.max_row - 1


@pytest.fixture(scope="module")
def on_disk(lab_manifest) -> dict:
    files = sorted(FILES.glob("*/*"))
    early = [f for f in files if date.fromisoformat(f"{f.stem[5:9]}-{f.stem[9:11]}-{f.stem[11:13]}") <= CUTOFF]
    return {
        "files": len(files),
        "rows": sum(rows_in(f) for f in files),
        "early_files": len(early),
        "early_rows": sum(rows_in(f) for f in early),
    }


@pytest.fixture(scope="module")
def runs() -> dict:
    lab.reset()
    out = {
        "first": lab.suppliers(CUTOFF),
        "again": lab.suppliers(CUTOFF),
        "renamed": lab.suppliers(RENAME_FROM, ignore_aliases=True),
    }
    out["after_failure"] = lab.table_totals("raw", ["supplier_files", "supplier_deliveries"])
    out["rest"] = lab.suppliers()
    out["checks"] = lab.checks()
    out["loads"] = lab.loads()
    return out


@pytest.fixture(scope="module")
def pulls(runs) -> dict:
    server = api.serve()
    try:
        out = {
            "first": lab.sellers("first"),
            "first_nolag": lab.sellers("first", lag=0),
            "second": lab.sellers("second"),
            "second_nolag": lab.sellers("second", lag=0),
        }
        out["reconcile"] = {k: {f: n for f, n, _ in v} for k, v in lab.reconcile().items()}
    finally:
        server.shutdown()
    return out


def test_first_run_lands_every_file_through_the_cutoff(runs, on_disk):
    first = runs["first"]
    assert (first["files"], first["rows"]) == (on_disk["early_files"], on_disk["early_rows"])
    assert first["supplier_deliveries"] == {"rows": on_disk["early_rows"], "snapshots": 1}


def test_rerun_loads_nothing(runs):
    again = runs["again"]
    assert again["load_ids"] == [] and again["rows"] == 0
    assert again["supplier_deliveries"] == runs["first"]["supplier_deliveries"]


def test_renamed_column_stops_the_load_and_nothing_lands(runs):
    assert runs["renamed"] == {"failed": "NormalizeJobFailed"}
    assert runs["after_failure"] == {k: runs["first"][k] for k in ("supplier_files", "supplier_deliveries")}


def test_every_file_and_row_landed_once(runs, on_disk, lab_manifest):
    rest = runs["rest"]
    assert rest["supplier_files"]["rows"] == on_disk["files"]
    assert rest["supplier_deliveries"]["rows"] == on_disk["rows"] == lab_manifest["volumes"]["supplier_feed_rows"]
    assert rest["supplier_deliveries"]["snapshots"] == 2  # one per successful load


def test_accounting_matches_the_manifest(runs, lab_manifest):
    acc = {what.strip(): n for what, n in runs["checks"]["accounting"]}
    d = lab_manifest["defects"]
    landed = lab_manifest["volumes"]["supplier_feed_rows"]
    repeats = acc["repeats in the same file"] + acc["repeats in a later file"]
    assert acc["landed in raw.supplier_deliveries"] == landed
    assert acc["first copies, quarantined"] == d["supplier_feed_malformed_rows"]
    assert repeats == d["supplier_feed_duplicate_rows"]
    assert acc["first copies, valid -> silver"] + acc["first copies, quarantined"] + repeats == landed
    assert acc["valid PO lines delivered twice with different values"] == 0


def test_quarantine_and_silver(runs, lab_manifest):
    reasons = {issue: (rows, distinct) for issue, rows, distinct, *_ in runs["checks"]["quarantine_reasons"]}
    assert sum(d for _, d in reasons.values()) == lab_manifest["defects"]["supplier_feed_malformed_rows"]
    assert set(reasons) == {
        "missing PO number",
        "quantity not a number",
        "negative quantity",
        "date not valid in the declared format",
    }
    acc = {what.strip(): n for what, n in runs["checks"]["accounting"]}
    assert runs["checks"]["supplier_deliveries"] == acc["first copies, valid -> silver"]
    assert runs["checks"]["supplier_delivery_quarantine"] == sum(r for r, _ in reasons.values())


def test_headers_as_sent_show_the_rename(runs, lab_manifest):
    by_header = {h: (files, suppliers, first) for h, files, suppliers, first, _ in runs["checks"]["headers"]}
    renamed = by_header["PO Number | PO Line | SKU | Qty Delivered | Delivery Date | DC"]
    assert renamed == (lab_manifest["defects"]["supplier_files_with_renamed_column"], 1, RENAME_FROM)
    assert {r["supplier_id"] for r in lab.scan("raw.supplier_files").to_pylist() if "Qty Delivered" in r["header"]} == {
        RENAMING_SUPPLIER
    }


def test_declared_date_formats_match_the_suppliers():
    declared = {k for k, v in lab.FEED["date_format"].items() if v == "%d/%m/%Y"}
    assert declared == DMY_SUPPLIERS


def test_each_load_is_one_snapshot_with_its_id_on_every_row(runs):
    loads = runs["loads"]
    assert len(loads) == 2
    assert sum(r[3] for r in loads) == runs["rest"]["supplier_deliveries"]["rows"]
    assert all(rows == added for *_, rows, _first, _last, _snap, added in loads)


def test_first_pull_pages_and_retries(pulls):
    first = pulls["first"]
    assert first["rows_fetched"] == world.N_SELLERS == first["table_rows"]
    assert (first["pages"], first["throttled"]) == (12, 2)


def test_lag_catches_the_slow_clock_and_upsert_keeps_one_row_per_seller(pulls):
    lag, nolag = pulls["second"], pulls["second_nolag"]
    changed = world.N_NEW + world.N_UPDATED
    assert nolag["rows_fetched"] == changed
    assert lag["rows_fetched"] > changed + world.N_SKEWED  # the window re-reads rows it already had
    assert lag["table_rows"] == nolag["table_rows"] == world.N_SELLERS + world.N_NEW
    assert lag["full_rows"] == world.N_SELLERS + world.N_NEW - world.N_DELETED


def test_upsert_commits_per_thousand_rows_and_rewrites_matched_files(pulls):
    assert pulls["first"]["snapshots"] == ["append +1,000", "append +200"]
    changed = pulls["second"]["rows_fetched"] - world.N_NEW  # re-read rows count too: their _dlt_load_id changed
    assert pulls["second"]["snapshots"][2:] == [
        f"overwrite -{world.N_SELLERS:,} +{world.N_SELLERS - changed:,}",
        f"append +{changed}",
        f"append +{world.N_NEW}",
    ]


def test_reconciliation_finds_what_the_cursor_missed(pulls):
    rec = pulls["reconcile"]
    gone = "gone from the API (deleted at source)"
    silent = "stale: changed without touching updated_at"
    assert rec["raw (lag 900 s)"] == {gone: world.N_DELETED, silent: world.N_SILENT}
    assert rec["part15 (no lag)"] == {
        gone: world.N_DELETED,
        silent: world.N_SILENT,
        "stale: missed a change, updated_at moved": world.N_SKEWED,
    }


def test_a_resent_file_lands_again_as_a_new_version(runs):
    """A supplier re-sends a corrected file under the same name: new ETag, new load, old rows kept (raw is history)."""
    fs = lab.cat9.lake_fs()
    path = "larkspur-raw/raw/suppliers/deliveries/S024/S024_20261005.csv"
    original = fs.cat_file(path)
    before = lab.table_totals("raw", ["supplier_deliveries"])["supplier_deliveries"]["rows"]
    try:
        corrected = original + original.splitlines(keepends=True)[-1]  # one more line than the first version
        fs.pipe_file(path, corrected)
        resent = lab.suppliers()  # nothing else changed in the landing zone
        assert (resent["files"], resent["rows"]) == (1, len(corrected.splitlines()) - 1)
        assert resent["supplier_deliveries"]["rows"] == before + resent["rows"]
    finally:
        fs.pipe_file(path, original)
