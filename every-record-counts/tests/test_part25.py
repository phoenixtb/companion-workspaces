"""Part 25: what the design review and the sign-off claim.

Core only (`make up-core`) and the tables of Parts 16-24 (Part 21's restatement included). The approval tests work on a
copy of the restatement (`gold.test_restatements_p25`, re-created as `proposed` for each test and dropped after), so
the documented approval in `gold.restatements` is never touched by a test run.
"""

import pyarrow as pa
import pytest

from scripts import part09_catalog as cat9
from scripts import part21_reconcile as lab21
from scripts import part25_review as lab
from tests.conftest import needs_stack

pytestmark = needs_stack

COPY = "gold.test_restatements_p25"


@pytest.fixture(scope="module")
def found():
    cat = lab21.catalog()
    if not cat.table_exists(lab21.RESTATEMENTS) or not cat.table_exists(lab21.CONTROLS):
        pytest.skip("needs Part 21's restatement and control tables")
    return lab.checks()


@pytest.fixture
def copy(found):
    """The real restatement row, as proposed, in a table of its own."""
    cat = lab21.catalog()
    row = lab.restatement() | {"status": "proposed"}
    real = cat.load_table(lab21.RESTATEMENTS).schema().as_arrow()
    cat9.drop_table(*COPY.split("."), cat9.token())
    t = cat.create_table(COPY, schema=real)
    t.append(pa.Table.from_pylist([row], schema=real))
    yield COPY
    cat9.drop_table(*COPY.split("."), cat9.token())


def test_board_pack_recomputes_from_the_lakehouse(found):
    """Every figure Farah signs, recomputed: revenue three ways, controls, conversion, stock and fill."""
    assert all(ok for *_, ok in found), [f for f in found if not f[2]]
    assert len(found) == 8
    assert lab21.q3_by_ref(lab21.QUARTER_TAG)["GB"] == lab21.q3_by_ref(None)["GB"] == 2_979_067_406
    what = dict((name, text) for name, text, _ in found)
    assert what["conversion (sessions contract)"] == "134,223 / 2,161,515 = 6.21%"  # as Part 23 left it


def test_approval_is_one_commit_and_never_twice(found, copy):
    t = lab21.catalog().load_table(copy)
    before = len(t.snapshots())
    first = lab.approve(found, "test approver", copy)
    t = lab21.catalog().load_table(copy)
    after = len(t.snapshots())
    assert first["new_commit"] and after > before
    assert lab.restatement(copy)["status"] == "approved"
    assert lab.restatement(copy)["restated_minor"] == 2_979_067_406
    props = t.current_snapshot().summary.additional_properties
    assert props["larkspur.approved-by"] == "test approver"
    # the proposal stays readable in the table's history
    proposed = t.scan(snapshot_id=t.snapshots()[before - 1].snapshot_id).to_arrow().column("status").to_pylist()
    assert proposed == ["proposed"]
    again = lab.approve(found, "someone else", copy)
    assert not again["new_commit"] and len(lab21.catalog().load_table(copy).snapshots()) == after


def test_approval_refused_when_any_check_fails(found, copy):
    broken = [*found[:-1], (found[-1][0], "93.0% / 77.7%", False)]
    with pytest.raises(SystemExit, match="not approved: 1 check"):
        lab.approve(broken, "test approver", copy)
    assert lab.restatement(copy)["status"] == "proposed"


def test_rerunning_part21_restate_does_not_reopen_an_approval(found, copy):
    # restate() tags the real report if the quarter's tag is missing; it must exist, so the test never moves anything
    report = lab21.catalog().load_table(lab21.REPORT)
    assert lab21.QUARTER_TAG in report.metadata.refs
    tagged = report.metadata.refs[lab21.QUARTER_TAG].snapshot_id
    lab.approve(found, "test approver", copy)
    n = len(lab21.catalog().load_table(copy).snapshots())
    lab21.restate(copy)
    assert lab.restatement(copy)["status"] == "approved"
    assert len(lab21.catalog().load_table(copy).snapshots()) == n
    assert lab21.catalog().load_table(lab21.REPORT).metadata.refs[lab21.QUARTER_TAG].snapshot_id == tagged


def test_an_approval_stands_when_checks_drift_later(found, copy, capsys):
    lab.approve(found, "test approver", copy)
    n = len(lab21.catalog().load_table(copy).snapshots())
    broken = [*found[:-1], (found[-1][0], "93.0% / 77.7%", False)]
    r = lab.approve(broken, "someone else", copy)
    assert r["drift"] == [found[-1][0]] and not r["new_commit"]
    assert "approved earlier" in capsys.readouterr().out
    assert lab.restatement(copy)["status"] == "approved"
    assert len(lab21.catalog().load_table(copy).snapshots()) == n


def test_estimate_scales_the_lab_by_fifty_and_five_hundred():
    rows = {r["source"]: r for r in lab.estimate()}
    pos = rows["POS events (raw)"]
    assert pos["days"] == 128 and pos["full_rows_per_day"] == pytest.approx(50 * pos["lab_rows_per_day"], abs=50)
    stock = rows["stock snapshot rows"]
    assert stock["full_rows_per_day"] == stock["lab_rows_per_day"]  # snapshots scale with store-SKUs, not baskets
    assert max(rows.values(), key=lambda r: r["ten_x_mib_per_day"])["source"] == "clickstream events"
