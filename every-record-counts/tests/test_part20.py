"""Part 20: what the post claims about data contracts and Write-Audit-Publish.

Contracts need `make up-core` and datacontract-cli (`make contracts-install`); the fixture lands the clickstream and
builds the sessions only if they are missing (~15 s). WAP needs `make up-spark` (~2 minutes: an initial load, then one
upload day published twice, with and without the dedup bug). The Trino test needs `make up-trino`.
"""

import pytest

from scripts import part09_catalog as cat9
from scripts import part20_contracts as contracts
from scripts import part20_wap as wap
from tests.conftest import ROOT, needs_batch, needs_stack

has_cli = pytest.mark.skipif(not contracts.DATACONTRACT.exists(), reason="needs `make contracts-install`")
C = ROOT / "contracts"
JULY = C / "history/clickstream_events-1.0.0.odcs.yaml"
CONSENT = C / "proposals/clickstream_events-add-consent.odcs.yaml"
RENAME = C / "proposals/clickstream_events-rename-page.odcs.yaml"


@has_cli
def test_contracts_are_valid_odcs():
    for path in sorted(C.rglob("*.odcs.yaml")):
        assert contracts.datacontract("lint", str(path)).returncode == 0, path


@has_cli
def test_ci_lets_additive_through_and_blocks_breaking():
    assert contracts.ci_one(JULY, CONSENT)  # new optional fields
    assert not contracts.ci_one(CONSENT, RENAME)  # page removed in a minor version
    assert contracts.ci_one(CONSENT, C / "clickstream_events.odcs.yaml")  # breaking, but as major version 2


@pytest.fixture(scope="module")
def lake() -> None:
    cat = cat9.pyiceberg("pipeline")
    if not cat.table_exists(contracts.EVENTS):
        contracts.land()
    if not cat.table_exists(contracts.SESSIONS):
        contracts.sessions()


@needs_stack
@has_cli
def test_contracts_hold_on_the_lakehouse(lake):
    for name in ("supplier_deliveries", "clickstream_events", "clickstream_sessions"):
        assert contracts.test(C / f"{name}.odcs.yaml")["result"] == "passed", name


@needs_stack
@has_cli
def test_july_contract_catches_the_rename(lake):
    out = contracts.test(JULY)
    assert [(c["field"], c["reason"]) for c in out["failed"]] == [
        ("page", "Actual missing_count(page) was 2675104, expected = 0")
    ]


@needs_stack
def test_contracted_conversion_is_one_number(lake):
    assert contracts.conversion() == {"sessions": 2161516, "converted": 134224, "conversion_pct": 6.21}


@needs_batch
def test_trino_484_cannot_write_branches():
    _, err = cat9.trino(f"CREATE BRANCH audit IN TABLE lakehouse_pipeline.{wap.TABLE}")
    assert err == "This connector does not support creating branches"


needs_spark = pytest.mark.skipif(not cat9.spark_up(), reason="needs `make up-spark`")


@needs_stack
@needs_spark
def test_wap_keeps_bad_data_off_main():
    assert wap.submit(["--init", "--until", "2026-09-29"]) == 0
    before = wap.refs()["main"]
    bad = wap.publish("2026-09-30", "skip-dedup")
    assert bad["exit"] == 1 and bad["main_seen"] == [before["snapshot"]]  # the reader only ever saw the old main
    after_bad = wap.refs()
    assert after_bad["main"] == before and after_bad["audit"]["duplicate_lines"] > 0
    good = wap.publish("2026-09-30", None)
    final = wap.refs()
    assert good["exit"] == 0 and good["main_seen"] == [before["snapshot"], final["main"]["snapshot"]]
    assert final["main"] == final["audit"] and final["main"]["duplicate_lines"] == 0
    assert final["main"]["lines"] > before["lines"]
