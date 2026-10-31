"""Part 34: the lake gate of the platform's CI. A change is built in `dev` (Part 18's slim CI), then every reconciled
figure it touches is recomputed from what it built and compared with production and with the signed board pack.

uv run python -m scripts.part34_platform smoke        # reconcile what `dev` holds now; exit 1 on a difference
uv run python -m scripts.part34_platform lake-check   # what `make check-lake` runs: empty dev, slim build, smoke
uv run python -m scripts.part34_platform inject       # the Hands-On's defect: refunds dated by purchase in report v1
uv run python -m scripts.part34_platform restore      # undo it

Needs `make up-trino` and Parts 17-25's production tables. Read-only outside `dev`: it never writes production, never
approves or restates anything (that stays Part 25's sign-off, after a deploy).
"""

import argparse
import json
import sys
import urllib.request
from datetime import date
from pathlib import Path

from scripts import part17_dbt as p17
from scripts import part18_dbt as p18
from scripts.part21_reconcile import money

ROOT = Path(__file__).resolve().parents[1]
BOARD_PACK = json.loads((ROOT / "governance/board-pack-2026-q3.json").read_text())
CHECK_SQL = ROOT / "sql/part17/check.sql"
TRINO = "http://localhost:8080"
USER = "dbt_dev"  # CI's identity (Part 18): reads everything, writes only lakehouse_dev (Part 23's OPA policy)
DEV, PROD = "lakehouse_dev.dev", "lakehouse_pipeline.gold"
REPORTS = ["rpt_daily_net_revenue", "rpt_daily_net_revenue_v2"]  # versions 1 and 2 (Part 18), both read by people
WINDOWED = {"fct_sales_line", "fct_basket"}  # microbatch: a slim build holds `make dbt-ci`'s event-time window only
# `make dbt-ci` builds event times 2 Oct 00:00 to 5 Oct 00:00 UTC; the business dates inside it in all three time zones
# (London UTC+1, India UTC+5:30, Singapore UTC+8 in October) are 3 and 4 October.
WINDOW_DAYS = (date(2026, 10, 3), date(2026, 10, 4))
Q3 = (date(2026, 7, 1), date(2026, 9, 30))
MODEL = ROOT / "dbt/larkspur/models/marts/rpt_daily_net_revenue_v1.sql"
GOOD, BAD = "select refund_date_key as date_key", "select purchase_date_key as date_key"


def trino(sql: str) -> list:
    req = urllib.request.Request(f"{TRINO}/v1/statement", data=sql.encode(), headers={"X-Trino-User": USER})
    page, rows = json.load(urllib.request.urlopen(req)), []
    while True:
        rows += page.get("data", [])
        if "error" in page:
            raise SystemExit(f"Trino: {page['error']['message']}")
        if "nextUri" not in page:
            return rows
        page = json.load(urllib.request.urlopen(page["nextUri"]))


def in_dev() -> set[str]:
    return {r[0] for r in trino(f"SHOW TABLES FROM {DEV}")}


def report_vs_production(report: str, windowed: bool) -> tuple[bool, list[str]]:
    """Every country-day of the report as the change built it, against production's. A full build also gives Q3 GB,
    which must equal the board pack Farah signed. A windowed build is compared on the window's whole business days."""
    lo, hi = WINDOW_DAYS if windowed else (date(2000, 1, 1), date(2100, 1, 1))
    rows = trino(f"""
        SELECT coalesce(d.country, p.country), count(*),
               count(*) FILTER (WHERE d.net_minor IS DISTINCT FROM p.net_minor),
               sum(coalesce(d.net_minor, 0) - coalesce(p.net_minor, 0)),
               sum(d.net_minor) FILTER (WHERE d.business_date BETWEEN DATE '{Q3[0]}' AND DATE '{Q3[1]}')
        FROM (SELECT * FROM {DEV}.{report} WHERE business_date BETWEEN DATE '{lo}' AND DATE '{hi}') d
        FULL JOIN (SELECT * FROM {PROD}.{report} WHERE business_date BETWEEN DATE '{lo}' AND DATE '{hi}') p
          ON p.country = d.country AND p.business_date = d.business_date
        GROUP BY 1 ORDER BY 1""")
    days, differ = sum(r[1] for r in rows), sum(r[2] for r in rows)
    scope = f"{lo:%d %b}-{hi:%d %b} (windowed build)" if windowed else "all dates"
    lines = [f"{report}: {days} country-days, {scope}: {differ} differ from production"]
    lines += [f"    {c}  {'+' if delta >= 0 else '-'}{money(abs(delta))}" for c, _, n, delta, _ in rows if n]
    ok = differ == 0
    if not windowed:
        gb = next((r[4] for r in rows if r[0] == "GB"), None)
        want = BOARD_PACK["uk_net_revenue_q3_minor"]
        lines.append(f"    Q3 GB net revenue {money(gb)}, board pack {money(want)}")
        ok = ok and gb == want
    return ok, lines


def figures(dev: set[str]) -> tuple[bool, list[str]]:
    """Part 17's stock and supplier checks, reading the change's tables where it rebuilt them, production's elsewhere
    (what --defer does for dbt), against the board pack."""
    _, stock_sql, fill_sql = p17.statements(CHECK_SQL)
    for table in ("fct_stock_daily", "fct_po_line", "dim_date"):
        where = DEV if table in dev else PROD
        stock_sql = stock_sql.replace(f"lakehouse_analyst.gold.{table}", f"{where}.{table}")
        fill_sql = fill_sql.replace(f"lakehouse_analyst.gold.{table}", f"{where}.{table}")
    (_, at_close), (fill, otif) = trino(stock_sql)[0], trino(fill_sql)[0]
    got = (round(100 * at_close, 1), round(100 * fill, 1), round(100 * otif, 1))
    want = tuple(BOARD_PACK[k] for k in ("stock_available_at_close_pct", "supplier_fill_pct", "supplier_otif_pct"))
    show = " / ".join(f"{v}%" for v in got), " / ".join(f"{v}%" for v in want)
    return got == want, [f"stock at close / fill / OTIF: {show[0]}, board pack {show[1]}"]


def smoke() -> dict:
    dev = in_dev()
    print(f"dev holds: {', '.join(sorted(dev)) or 'nothing'}")
    results: list[tuple[str, bool]] = []
    windowed = bool(dev & WINDOWED)
    for report in REPORTS:
        if report in dev:
            ok, lines = report_vs_production(report, windowed)
            print("\n".join(lines))
            results.append((report, ok))
    if dev & {"fct_stock_daily", "fct_po_line", "dim_date"}:
        ok, lines = figures(dev)
        print("\n".join(lines))
        results.append(("stock and supplier figures", ok))
    if not results:
        print("nothing this change built feeds a reconciled figure")
    failed = [name for name, ok in results if not ok]
    print(f"reconciliation smoke: {'FAIL ' + ', '.join(failed) if failed else 'pass'}")
    return {"dev": sorted(dev), "results": results, "ok": not failed}


def lake_check() -> int:
    build = p18.ci()
    if build["returncode"]:
        print("dbt build in dev failed: nothing to reconcile")
        return 1
    return 0 if smoke()["ok"] else 1


def edit(old: str, new: str) -> None:
    text = MODEL.read_text()
    if old not in text:
        raise SystemExit(f"{MODEL.relative_to(ROOT)}: {old!r} not found (already done?)")
    MODEL.write_text(text.replace(old, new))
    print(f"{MODEL.relative_to(ROOT)}: {old!r} -> {new!r}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["smoke", "lake-check", "inject", "restore"])
    a = p.parse_args()
    if a.command == "smoke":
        sys.exit(0 if smoke()["ok"] else 1)
    elif a.command == "lake-check":
        sys.exit(lake_check())
    elif a.command == "inject":
        edit(GOOD, BAD)
    else:
        edit(BAD, GOOD)


if __name__ == "__main__":
    main()
