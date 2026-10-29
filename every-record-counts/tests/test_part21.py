"""Part 21: what the post claims about completeness and correctness engineering.

Core only (`make up-core`), and Part 21's tables: the module fixture lands the PSP's settlement files and decides every
upload day's POS events if `scripts.part21_reconcile` hasn't yet (about a minute the first time; a no-op after). Needs
Part 16's CDC replicas and Part 17-19's gold tables.
"""

import pytest

from scripts import part21_reconcile as lab
from tests.conftest import needs_stack

pytestmark = needs_stack

DAY = lab.date(2026, 10, 2)  # an upload day outside Q3 and outside Part 19's tests: the re-send changes no Q3 figure


@pytest.fixture(scope="module")
def ready():
    cat = lab.catalog()
    if not cat.table_exists("raw.orders") or not cat.table_exists("gold.fct_basket"):
        pytest.skip("needs Part 16's CDC replicas and Part 17-19's gold tables")
    lab.land_psp()
    if not cat.table_exists(lab.DISPOSITIONS) or len(lab.decided_days(cat.load_table(lab.DISPOSITIONS))) < len(
        lab.upload_days()
    ):
        lab.accept(None)
    return cat


def test_waterfall_closes_the_gap_to_the_penny(ready, lab_manifest):
    w = lab.waterfall()
    m = lab_manifest["uk_net_revenue_q3"]
    assert w["legacy"] == m["legacy_board_deck_minor"] == 2_999_923_243
    assert w["correct"] == m["correct_minor"] == 2_979_067_406
    assert w["steps"] == [s["amount_minor"] for s in m["steps"]]
    assert w["unexplained"] == 0 and sum(w["steps"]) == 20_855_837


def test_settlement_bridge_explains_every_record(ready):
    b = lab.bridge()
    assert b["bugs"] == 0 and not any(line.startswith("9") for line in b["lines"])
    assert b["supported"] == b["gold"] == 2_979_067_406
    evidence = lab.board_deck_records()
    assert evidence["step 1: re-sent baskets"][:3] == (2455, 2455, 0)  # one PSP sale per re-sent basket
    assert evidence["step 2: polled, then deleted orders"] == (1176, 0, 1176, 1176)  # no sale, payment voided


def test_settlement_landing_is_idempotent(ready):
    again = lab.land_psp()
    assert again["files_landed"] == 0 and again["files_seen"] == 267


def test_injected_resend_is_counted_once_and_not_double_counted(ready):
    def decided():
        t = lab.catalog().load_table(lab.DISPOSITIONS)
        rows = t.scan(row_filter=lab.EqualTo("upload_date", DAY.isoformat())).to_arrow().to_pylist()
        return rows

    def land():  # Part 19's ingest without its Trino read-back: replace the upload day in raw.pos_uploads
        con = lab.connect()
        folder = lab.RAW / f"pos/events/upload_date={DAY}"
        data = con.sql(
            f"SELECT * EXCLUDE (schema_version) FROM read_json('{folder}/*.jsonl.gz', hive_partitioning = true)"
        )
        from scripts import part23_privacy as privacy

        table = lab.catalog().load_table("raw.pos_uploads")
        table.overwrite(
            privacy.suppress(
                data.to_arrow_table().cast(table.schema().as_arrow()), tombstone={"loyalty_id": "loyalty_id"}
            ),
            overwrite_filter=lab.EqualTo("upload_date", DAY.isoformat()),
        )

    before = decided()
    try:
        sent = lab.inject("resend", DAY)
        land()
        # raw changed, dispositions didn't: the day is stale, and the landing control says so
        assert lab.stale_days() == [DAY]
        assert any(f["check"] == "raw_vs_dispositions" for f in lab.control(DAY))
        decided_now = [r["upload_date"] for r in lab.accept(DAY)]
        assert decided_now[0] == str(DAY) and len(decided_now) > 1  # the window's following days are re-decided too
        assert lab.stale_days() == []
        after = decided()
        new = [r for r in after if r["event_id"] == sent["new_event_id"]]
        assert len(after) == len(before) + 1
        assert new and new[0]["disposition"] == "resend" and new[0]["first_event_id"] == sent["first_event_id"]
        accepted = lambda rows: sorted(r["event_id"] for r in rows if r["disposition"] == "accepted")  # noqa: E731
        assert accepted(after) == accepted(before)  # nothing new counts
        failed = lab.control(DAY)
        assert not failed
        rows = (
            lab.catalog().load_table(lab.CONTROLS).scan(row_filter=lab.EqualTo("run_day", DAY.isoformat())).to_arrow()
        )
        resends = {r["country"]: r["target_count"] for r in rows.to_pylist() if r["check_name"] == "resends"}
        assert resends["GB"] == sum(1 for r in after if r["disposition"] == "resend" and r["country"] == "GB")
    finally:
        lab.inject("restore", DAY)
        land()
        lab.accept(DAY)
    assert sorted(r["event_id"] for r in decided()) == sorted(r["event_id"] for r in before)
