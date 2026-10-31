"""Part 35: the quarter close. Exceptions are counted per reconciliation, read-only, and a recorded erasure can be
re-applied only where a replay brought the person back.

Host-only tests check the counting rules on made-up inputs. Stack tests need core (`make up-core`) with Parts 16-25's
tables; the served check needs `make up-cube`; the re-apply test needs Trino (`make up-trino`).
"""

import json
import socket

import pytest

from scripts import part23_privacy as lab23
from scripts import part25_review as lab25
from scripts import part35_close as lab
from tests.conftest import needs_batch, needs_stack, stack_up, trino_up

PACK = json.loads(lab25.BOARD_PACK.read_text())
WANT = PACK["uk_net_revenue_q3_minor"]


def cube_up() -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("localhost", 4000)) == 0 and stack_up() and trino_up()


def inputs(**change) -> dict:
    """A clean close: nothing unexplained, no BUG lines, no failed control, every report version on the pack."""
    base = {
        "waterfall": {"unexplained": 0, "correct": WANT},
        "bridge": {
            "lines": {"00 PSP net settled": (1, 10), "09 matched to the penny": (0, 8), "03 timing": (5, 2)},
            "supported": WANT,
            "gold": WANT,
        },
        "controls": (934, 29, 0),
        "refs": {"q3_2026_reconciled": WANT, "published": WANT, "main": WANT},
        "restated": {"status": "approved", "restated_minor": WANT, "as_published_minor": PACK["board_deck_minor"]},
        "pack": PACK,
    }
    return base | change


def test_a_clean_close_has_no_exceptions_and_counts_classified_records():
    found = lab.count_exceptions(**inputs())
    assert sum(n for _, n, _ in found) == 0
    assert found[1][2] == "10 records classified, 0 on BUG lines"  # lines 00-02 are PSP totals, not records


def test_each_bug_record_failed_verdict_and_stray_version_is_one_exception():
    lines = {"09 matched": (0, 8), "90 BUG: amounts differ": (12, 2), "92 BUG: PSP row overdue": (40, 3)}
    found = lab.count_exceptions(**inputs(
        bridge={"lines": lines, "supported": WANT, "gold": WANT},
        controls=(934, 29, 4),
        refs={"q3_2026_reconciled": WANT, "published": WANT, "main": WANT + 1},
    ))  # fmt: skip
    counts = [n for _, n, _ in found]
    assert counts == [0, 5, 4, 1, 0]  # BUG records, not BUG lines


def test_an_unexplained_penny_or_an_unapproved_restatement_blocks_the_close():
    found = lab.count_exceptions(**inputs(
        waterfall={"unexplained": 1, "correct": WANT},
        restated={"status": "proposed", "restated_minor": WANT, "as_published_minor": PACK["board_deck_minor"]},
    ))  # fmt: skip
    assert [n for _, n, _ in found] == [1, 0, 0, 0, 1]


@pytest.mark.parametrize(
    ("change", "row"),
    [
        ({"waterfall": {"unexplained": 0, "correct": WANT - 1}}, 0),  # waterfall's correct figure off the pack
        ({"bridge": {"lines": {"09 matched": (0, 8)}, "supported": WANT - 1, "gold": WANT}}, 1),  # PSP support off
        ({"bridge": {"lines": {"09 matched": (0, 8)}, "supported": WANT, "gold": WANT + 1}}, 1),  # gold off
        ({"restated": {"status": "approved", "restated_minor": WANT - 1,
                       "as_published_minor": PACK["board_deck_minor"]}}, 4),  # restated figure off
        ({"restated": {"status": "approved", "restated_minor": WANT,
                       "as_published_minor": PACK["board_deck_minor"] + 1}}, 4),  # as published off the deck
    ],
)  # fmt: skip
def test_a_total_off_the_board_pack_is_one_exception(change, row):
    found = lab.count_exceptions(**inputs(**change))
    assert [n for _, n, _ in found] == [int(i == row) for i in range(5)]


def test_served_differences_count_per_country_day_and_figure():
    served = {"rows": [("rev", "", True), ("stock", "", False), ("conv", "", True), ("days", "", False)],
              "country_days": 378, "differing": 2}  # fmt: skip
    found = lab.count_exceptions(**inputs(served=served))
    assert found[-1][1] == 3


@needs_stack
def test_the_quarter_closes_with_zero_exceptions():
    r = lab.close(served=cube_up())
    assert r["exceptions"] == 0, [f for f in r["found"] if f[1]]
    assert r["revenue"] == WANT == 2_979_067_406
    assert r["conversion"]["conversion_pct"] == PACK["conversion_pct"] == 6.21


@needs_stack
def test_reapply_refuses_a_subject_never_erased():
    with pytest.raises(SystemExit, match="no erasure"):
        lab.reapply("CZZ9999999")


@needs_batch
def test_reapply_writes_nothing_where_the_subject_is_still_gone():
    case = lab23.CASES / "CGB0000113.json"
    if not case.exists():
        pytest.skip("needs Part 23's documented erasure (its case file)")
    if any(r["rows"] for r in lab23.find("CGB0000113", quiet=True)):
        pytest.skip("CGB0000113 is back (Part 16's replay): run `part35_close reapply CGB0000113` first")
    assert lab.reapply("CGB0000113") == []
