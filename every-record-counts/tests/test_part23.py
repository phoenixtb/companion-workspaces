"""Part 23: what the post claims about access control, masking and erasure.

Needs `make up-trino` (Trino asks OPA, which starts with it) and Parts 16-21's tables. The module fixture creates the
Polaris objects, reloads the CRM (people on the suppression list stay out) and re-tags the tables.

The erasure test never touches a real (generated) customer: it seeds a reserved synthetic subject, `CZZ0000001`, into
every location with dates outside every published window (1 January 2025), erases and proves it, then removes what it
seeded and its register and suppression rows, so published numbers are unchanged. The documented erasure (the post's
request ER-2026-1029-001, customer CGB0000113) must survive every reload Parts 6-21 run.
"""

import gzip
import json
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from scripts import part09_catalog as cat9
from scripts import part23_privacy as lab
from tests.conftest import needs_batch

pytestmark = needs_batch

DOCUMENTED = "CGB0000113"  # the post's subject; request ER-2026-1029-001
TEST = {"customer_id": "CZZ0000001", "loyalty_id": "LZZ0000001", "anonymous_id": "a-CZZ0000001",
        "email": "zz.test.0000001@example.com", "phone": "+44 7700 900000"}  # fmt: skip
TEST_ORDER, TEST_BASKET, TEST_REQUEST = "OZZ0000001", "ZZ-0000-20250101-00001", "ER-TEST-CZZ"
DAY = date(2025, 1, 1)
AT = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
RAW_FILE = f"larkspur-raw/raw/clickstream/events/event_date={DAY}/zz-test.jsonl.gz"


@pytest.fixture(scope="module")
def governed():
    if not cat9.pyiceberg("root", vended=False).table_exists("raw.orders_changes"):
        pytest.skip("needs Part 16's CDC replicas")
    lab.setup()
    lab.customers()
    lab.tag()
    return lab.policy_data()  # OPA reloads the file it writes (--watch)


def rows(user: str, sql: str) -> list:
    _, out, err = lab.trino(sql, user)
    assert err is None, err
    return out


def test_dev_cannot_write_or_drop_production(governed):
    out = {(u, sql): r for u, sql, r in lab.access()}
    assert out[("dbt_dev", "DROP TABLE lakehouse_pipeline.gold.fct_basket")].startswith("denied: Cannot drop table")
    assert out[("dbt_dev", "CREATE TABLE lakehouse_pipeline.gold.p23_probe AS SELECT 1 AS x")].startswith("denied")
    assert out[("dbt_dev", "CREATE TABLE lakehouse_dev.dev.p23_probe AS SELECT 1 AS x")] == "ok [1]"
    assert out[("dbt_dev", "SELECT count(*) FROM lakehouse_pipeline.gold.fct_basket")].startswith("ok")  # --defer reads
    assert out[("analyst_gb", "SELECT count(*) FROM lakehouse_pipeline.raw.customers")].startswith("denied")
    assert out[("intern", "SELECT 1")] == "denied: Cannot execute query"


def test_analysts_get_their_country_masked(governed):
    sql = "SELECT country, customer_id, full_name, email, postcode FROM governed.customer.dim_customer"
    mine = rows("analyst_gb", sql + " ORDER BY customer_since, city LIMIT 50")
    assert {r[0] for r in mine} == {"GB"}
    assert all(r[2] is None and r[3] is None and len(r[1]) == 16 and len(r[4]) == 3 for r in mine)
    clear = rows("dpo", "SELECT count(*) FROM governed.customer.dim_customer")[0][0]
    gb = rows("dpo", "SELECT count(*) FROM governed.customer.dim_customer WHERE country = 'GB'")[0][0]
    assert rows("analyst_gb", "SELECT count(*) FROM governed.customer.dim_customer")[0][0] == gb < clear
    # the pseudonym is the same in every table, so analysts can still join and count people
    q = "SELECT count(DISTINCT anonymous_id), count(DISTINCT session_id) FROM governed.gold.web_sessions"
    assert rows("analyst_gb", q) == rows("dpo", q + " WHERE country = 'GB'")


def test_the_pseudonym_key_never_reaches_the_analyst(governed):
    plan = "\n".join(r[0] for r in rows("analyst_gb", "EXPLAIN SELECT customer_id FROM governed.customer.dim_customer"))
    assert "hmac_sha256(to_utf8(customer_id), key)" in plan and "pseudonym_key" in plan
    assert "larkspur-lab-pseudonym-key" not in plan and "bGFya3NwdXIt" not in plan  # neither clear nor base64
    _, _, err = lab.trino("SELECT * FROM governed.customer.pseudonym_key", "analyst_gb")
    assert err.startswith("Access Denied: Cannot select from columns [key]")


def test_unclassified_tables_are_refused(governed):
    lab.register_table()  # exists, readable by Trino's principal, deliberately not classified
    _, _, err = lab.trino(f"SELECT count(*) FROM governed.{lab.REGISTER}", "analyst_gb")
    assert err.startswith("Access Denied: Cannot select from columns")


def test_masks_only_guard_trinos_door(governed):
    doors = lab.doors()
    masked = doors["Trino as analyst_gb, gold.web_sessions"]
    raw = doors["PyIceberg as analyst, gold.web_sessions"]
    assert "a-C" not in masked and "a-CGB" in raw  # same row, other door: Part 9's analyst principal reads files
    assert doors["PyIceberg as analyst, customer.dim_customer"].startswith("ForbiddenError")


def test_discovery_finds_an_identifier_hidden_in_another_column(governed):
    found = {(t, c): (n, v) for t, c, n, v in lab.discover()}
    assert found[("gold.web_sessions", "session_id")] == ("-", "anonymous id")  # no name rule would catch it
    assert ("part06.clicks", "anonymous_id") in found  # a lab copy outside the governed namespaces


# ---------- the reserved test subject ----------


def template(cat, table: str, **changes) -> pa.Table:
    """One existing row of `table`, with the identifying and date columns replaced: the schema stays exact."""
    t = cat.load_table(table)
    row = t.scan(limit=1).to_arrow().to_pylist()[0] | changes
    return pa.Table.from_pylist([row], schema=t.schema().as_arrow())


def seed() -> None:
    cat = cat9.pyiceberg("root", vended=False)
    c, ids = TEST["customer_id"], TEST
    seeds = {
        "raw.customers": dict(customer_id=c, loyalty_id=ids["loyalty_id"], full_name="Zed Test", email=ids["email"],
                              phone=ids["phone"], country="GB", created_at=AT),
        "customer.dim_customer": dict(customer_id=c, loyalty_id=ids["loyalty_id"], full_name="Zed Test",
                                      email=ids["email"], phone=ids["phone"], country="GB", customer_since=DAY,
                                      orders=1, first_order=DAY),
        "raw.orders": dict(order_id=TEST_ORDER, customer_id=c, country="GB", created_at=AT, updated_at=AT),
        "raw.orders_changes": dict(order_id=TEST_ORDER, customer_id=c, country="GB", created_at=AT, updated_at=AT),
        "raw.pos_uploads": dict(event_id="zz-pos-1", basket_id=TEST_BASKET, loyalty_id=ids["loyalty_id"],
                                event_ts=AT, uploaded_at=AT, upload_date=DAY),
        "raw.pos_events": dict(event_id="zz-pos-1", basket_id=TEST_BASKET, loyalty_id=ids["loyalty_id"],
                               event_ts=AT, uploaded_at=AT),
        "raw.clickstream_events": dict(event_id="zz-click-1", anonymous_id=ids["anonymous_id"], user_id=c,
                                       event_ts=AT, event_date=DAY, country="GB"),
        "gold.web_sessions": dict(session_id=ids["anonymous_id"] + "#1", anonymous_id=ids["anonymous_id"],
                                  country="GB", started_at=AT, ended_at=AT, converted=False),
        "part06.clicks": dict(event_id="zz-click-1", anonymous_id=ids["anonymous_id"], event_ts=AT, country="GB"),
    }  # fmt: skip
    for table, changes in seeds.items():
        cat.load_table(table).append(template(cat, table, **changes))
    with lab.psycopg.connect(lab.PG, autocommit=True) as pg:
        pg.execute("INSERT INTO crm.customers (customer_id, loyalty_id, full_name, email, phone, country, created_at) "
                   "VALUES (%s, %s, 'Zed Test', %s, %s, 'GB', %s)",
                   (c, ids["loyalty_id"], ids["email"], ids["phone"], AT))  # fmt: skip
        if pg.execute("SELECT to_regclass('shop.orders')").fetchone()[0]:
            pg.execute("INSERT INTO shop.orders VALUES (%s, %s, 'GB', 'GBP', 'web', 'placed', 1000, 'card', %s, %s, "
                       "NULL)", (TEST_ORDER, c, AT, AT))  # fmt: skip
    event = {"event_id": "zz-click-1", "anonymous_id": ids["anonymous_id"], "user_id": c, "event_type": "page_view",
             "event_ts": AT.isoformat(), "platform": "web", "user_agent": "test", "country": "GB"}  # fmt: skip
    cat9.lake_fs().pipe_file(RAW_FILE, gzip.compress((json.dumps(event) + "\n").encode()))
    lab.CASES.joinpath(f"{c}.json").unlink(missing_ok=True)


def unseed() -> None:
    """Remove the tombstoned records the test seeded, and its register and suppression rows: net effect zero."""
    for table in ("raw.orders", "raw.orders_changes"):
        lab.must(f"DELETE FROM {lab.PIPELINE}.{table} WHERE order_id = '{TEST_ORDER}'", "erasure")
    for table in ("raw.pos_uploads", "raw.pos_events"):
        lab.must(f"DELETE FROM {lab.PIPELINE}.{table} WHERE basket_id = '{TEST_BASKET}'", "erasure")
    for table in ("raw.orders", "raw.orders_changes", "raw.pos_uploads", "raw.pos_events"):
        lab.clean(table)
    with lab.psycopg.connect(lab.PG, autocommit=True) as pg:
        if pg.execute("SELECT to_regclass('shop.orders')").fetchone()[0]:
            pg.execute("DELETE FROM shop.orders WHERE order_id = %s", (TEST_ORDER,))
    cat = cat9.pyiceberg("pipeline")
    for table in (lab.REGISTER, lab.SUPPRESSION):
        if cat.table_exists(table):
            cat.load_table(table).delete(f"request_id == '{TEST_REQUEST}'")
    lab.suppression_list(refresh=True)
    lab.CASES.joinpath(f"{TEST['customer_id']}.json").unlink(missing_ok=True)


@pytest.fixture
def seeded(governed):
    seed()
    yield TEST["customer_id"]
    unseed()


def test_erasure_reaches_every_byte_and_tags_hold_it_back(seeded):
    subject = seeded
    assert lab.prove(subject, "before")["hits"] >= 10
    lab.pin("raw.orders", "p23_test_close")
    try:
        out = lab.erase(subject, TEST_REQUEST)
        done = {loc: n for loc, _, n in out["actions"]}
        assert done["raw.customers"] == done["gold.web_sessions"] == 1 and done["raw.orders"] == 1
        held = lab.prove(subject, "tag kept")
        assert held["hits"] > 0 and all(k.startswith("raw.orders ") for k in held["by"])  # only the tagged snapshot
    finally:
        lab.untag("raw.orders", "p23_test_close")
    lab.clean("raw.orders")
    assert lab.prove(subject, "after")["hits"] == 0
    assert all(r["rows"] == 0 for r in lab.find(subject, quiet=True))
    assert not cat9.lake_fs().exists(RAW_FILE)  # the raw file held only the subject: deleted, not left empty


def test_documented_erasure_survives_every_reload(governed):
    """make ci re-lands raw.pos_uploads (Part 17), raw.clickstream_events (Part 20), part06.clicks (Part 6) and the
    CRM before this runs: each loader applies the suppression list, so the post's subject stays erased."""
    case = lab.CASES / f"{DOCUMENTED}.json"
    if not case.exists():  # the case file is the privacy team's; rebuild it from the generator's rule if missing
        r = lab.customer_row(DOCUMENTED)
        ids = {"customer_id": DOCUMENTED, "anonymous_id": f"a-{DOCUMENTED}"} | {
            k: r[k] for k in ("loyalty_id", "email", "phone")
        }
        case.parent.mkdir(parents=True, exist_ok=True)
        case.write_text(json.dumps(ids, indent=1) + "\n")
    # Part 16's CDC replicas are rebuilt from a regenerated source database when Part 16's tests run: not covered
    cdc = {"raw.orders", "raw.orders_changes"}
    left = lab.prove(DOCUMENTED, "documented erasure")["by"]
    assert all(k.split()[0] in cdc for k in left), left
    assert all(r["rows"] == 0 for r in lab.find(DOCUMENTED, quiet=True) if r["table"] not in cdc)


def test_purge_is_contained_by_drop_privilege(governed, capsys):
    lab.drops()
    out = capsys.readouterr().out
    assert "drop-with-purge.enabled = true" in out
    holders = {line.split()[1] for line in out.splitlines() if line.strip().startswith("TABLE_DROP")}
    assert holders == {"developers", "pipelines"}  # no reader, analyst or gateway principal can drop (and purge)
