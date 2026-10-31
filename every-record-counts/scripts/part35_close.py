"""Part 35: the quarter close. Every reconciliation the platform runs for Q3 2026, counted as exceptions (items a
person must explain before the period closes), read-only; and the re-application of a recorded erasure where a replay
brought the person back.

uv run python -m scripts.part35_close close [--served]   # exceptions per reconciliation; exit 1 if any
uv run python -m scripts.part35_close reapply CUSTOMER_ID  # Part 23's steps again, only where the subject reappeared

`close` needs core (`make up-core`); `--served` adds Part 31's Cube-vs-dbt check (`make up-cube`). `reapply` needs
`make up-trino`. `close` writes nothing; it never approves or restates (Part 25 approved Q3's restatement).
"""

import argparse
import json
import sys
import time

import psycopg

from scripts import part20_contracts as lab20
from scripts import part21_reconcile as lab21
from scripts import part23_privacy as lab23
from scripts import part25_review as lab25

BUG = "9"  # Part 21's bridge numbers its unexplained lines 90-93 ("BUG: ...")


def count_exceptions(waterfall: dict, bridge: dict, controls: tuple[int, int, int], refs: dict, restated: dict,
                     pack: dict, served: dict | None = None) -> list[tuple[str, int, str]]:  # fmt: skip
    """(reconciliation, exceptions, what was compared). An exception is one item someone must explain: an
    unexplained amount or a total off the board pack (waterfall, bridge), a record on a BUG line, a failed control
    verdict, a report version off the pack, a restatement unapproved or off the pack, a country-day or board-pack
    figure the served metric gets wrong. Each mismatch counts once."""
    want = pack["uk_net_revenue_q3_minor"]
    bug_records = sum(n for line, (_, n) in bridge["lines"].items() if line.startswith(BUG))
    records = sum(n for line, (_, n) in bridge["lines"].items() if line[:2] not in ("00", "01", "02"))
    rows, run_days, failures = controls
    off = [ref for ref, v in refs.items() if v != want]
    deck = pack["board_deck_minor"]
    out = [
        ("waterfall: board deck -> correct, five causes",
         int(waterfall["unexplained"] != 0) + int(waterfall["correct"] != want),
         f"correct {lab21.money(waterfall['correct'])}, unexplained {lab21.money(waterfall['unexplained']) or '0.00'}"),
        ("bridge: PSP settlement -> gold, per record",
         bug_records + int(bridge["supported"] != want) + int(bridge["gold"] != want),
         f"{records:,} records classified, {bug_records} on BUG lines"),
        ("daily controls: latest verdict per check", failures, f"{rows:,} verdicts over {run_days} run days"),
        ("report: quarter tag, published, main", len(off), f"{len(refs)} versions = {lab21.money(want)}"),
        ("restatement 2026-Q3 GB",
         int(restated["status"] != "approved") + int(restated["restated_minor"] != want)
         + int(restated["as_published_minor"] != deck),
         f"{restated['status']}, {lab21.money(restated['as_published_minor'])} -> "
         f"{lab21.money(restated['restated_minor'])}"),
    ]  # fmt: skip
    if served is not None:
        out.append(("served: Cube metric vs dbt report, per country-day",
                    served["differing"] + sum(not ok for *_, ok in served["rows"][:3]),
                    f"{served['country_days']} country-days, 3 board-pack figures"))  # fmt: skip
    return out


def close(served: bool = False) -> dict:
    started = time.monotonic()
    pack = json.loads(lab25.BOARD_PACK.read_text())
    w = lab25.quiet(lab21.waterfall)
    b = lab25.quiet(lab21.bridge)
    refs = {ref or "main": lab21.q3_by_ref(ref)["GB"] for ref in (lab21.QUARTER_TAG, "published", None)}
    c = lab25.quiet(lab20.conversion)
    s = None
    if served:
        from scripts import part31_serve as lab31  # needs Cube (make up-cube)

        s = lab25.quiet(lab31.check)
    found = count_exceptions(w, b, lab25.latest_controls(), refs, lab25.restatement(), pack, s)
    total = sum(n for _, n, _ in found)
    print(f"Q3 2026 close, read-only: reconciliation exceptions ({pack['period']})")
    for name, n, what in found:
        print(f"{n:>4}  {name:<52} {what}")
    print(f"{total:>4}  exceptions; {time.monotonic() - started:.1f} s")
    print(f"UK net revenue Q3 {lab21.money(w['correct'])} (board deck {lab21.money(pack['board_deck_minor'])}, "
          f"restated); conversion {c['converted']:,} / {c['sessions']:,} = {c['conversion_pct']:.2f}%")  # fmt: skip
    return {"exceptions": total, "found": found, "revenue": w["correct"], "conversion": c}


def reapply(customer_id: str) -> list[tuple[str, str, int]]:
    """Part 23's own erase steps (tombstone or delete, then `clean`) for a subject already in the erasure register,
    only in the locations where rows came back. No register rows: the request was answered; this repairs a copy a
    replay rebuilt (Part 16's CDC replay rebuilds `shop.orders` and the replicas from regenerated source data)."""
    if lab23.subject_hash(customer_id) not in set(
        lab23.register_table().scan(selected_fields=("subject_hmac",)).to_arrow()["subject_hmac"].to_pylist()
    ):
        raise SystemExit(f"{customer_id} has no erasure in {lab23.REGISTER}: run Part 23's erase, not a re-apply")
    if not (lab23.CASES / f"{customer_id}.json").exists():
        raise SystemExit(f"no case file for {customer_id}: its identifiers are gone, nothing to search for")
    ids, actions = lab23.identifiers(customer_id), []
    with psycopg.connect(lab23.PG, autocommit=True) as pg:
        n = pg.execute("DELETE FROM crm.customers WHERE customer_id = %s", (customer_id,)).rowcount
        if n:
            actions.append(("postgres crm.customers", "delete", n))
        if pg.execute("SELECT to_regclass('shop.orders')").fetchone()[0]:
            n = pg.execute(
                "UPDATE shop.orders SET customer_id = %s WHERE customer_id = %s", (lab23.TOMBSTONE, customer_id)
            ).rowcount
            if n:
                actions.append(("postgres shop.orders", "tombstone", n))
    back = {r["table"] for r in lab23.find(customer_id, quiet=True) if r["rows"]}
    for table, column, action, catalog in lab23.LOCATIONS:
        if table not in back:
            continue
        signed_in = column != "anonymous_id" or lab23.has_column(catalog, table, "user_id")
        cond = lab23.where(column, ids) if signed_in else f"anonymous_id = '{ids['anonymous_id']}'"
        days = None
        if table in lab23.PARTITION:
            days = [r[0] for r in lab23.must(f"SELECT DISTINCT CAST({lab23.PARTITION[table]} AS varchar) FROM "
                                             f"{catalog}.{table} WHERE {cond}", "erasure")]  # fmt: skip
        sql = (f"DELETE FROM {catalog}.{table} WHERE {cond}" if action == "delete" else
               f"UPDATE {catalog}.{table} SET {column} = '{lab23.TOMBSTONE}' WHERE {cond}")  # fmt: skip
        actions.append((table, action, lab23.must(sql, "erasure")[0][0]))
        lab23.clean(table, catalog, days)
    for loc, action, n in actions:
        print(f"{loc:<34} {action:<10} {n:>5}")
    print(f"{len(actions)} location(s) repaired; register unchanged")
    return actions


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("close").add_argument("--served", action="store_true")
    sub.add_parser("reapply").add_argument("customer_id")
    a = p.parse_args()
    if a.cmd == "close":
        sys.exit(1 if close(a.served)["exceptions"] else 0)
    reapply(a.customer_id)


if __name__ == "__main__":
    main()
