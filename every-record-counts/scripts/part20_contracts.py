"""Part 20: data contracts (ODCS 3.2) for the supplier feed and the clickstream, tested with datacontract-cli against
the lakehouse, and a local CI check that refuses breaking contract changes.

uv run python -m scripts.part20_contracts land        # raw.clickstream_events: the SDK's events as landed (once)
uv run python -m scripts.part20_contracts sessions    # gold.web_sessions, built by the session contract's rules
uv run python -m scripts.part20_contracts conversion  # the one contracted conversion rate
uv run python -m scripts.part20_contracts test CONTRACT [--server lakehouse] [--checks quality]
uv run python -m scripts.part20_contracts ci [--base FILE --head FILE]   # what `make contract-ci` runs

datacontract-cli lives in its own environment, .venv-contracts (`make contracts-install`): its DuckDB extra pins
DuckDB 1.5, the lab's environment runs 2.0. The lakehouse server type reads each table with PyIceberg (Part 9's
principals, vended credentials) and runs the checks in that DuckDB.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb

from scripts import part09_catalog as cat9
from scripts.part17_dbt import raw_views

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
DATACONTRACT = ROOT / ".venv-contracts/bin/datacontract"
SESSIONS_SQL = ROOT / "sql/part20/web_sessions.sql"
EVENTS = "raw.clickstream_events"
SESSIONS = "gold.web_sessions"
# Who tests what: the producer's events and the supplier tables as the pipeline principal; the sessions as the
# analyst, who reads gold only (Part 9) — the consumer checks exactly what it is allowed to read.
PRINCIPAL = {"supplier_deliveries": "pipeline", "clickstream_events": "pipeline", "clickstream_sessions": "analyst"}


# ---------- the data the contracts describe ----------


def land() -> int:
    """Every clickstream event as sent, into an Iceberg table partitioned by the event_date the files carry."""
    cat = cat9.pyiceberg("pipeline")
    con = raw_views()
    started = time.monotonic()
    data = con.sql("SELECT * FROM raw_clickstream").to_arrow_table()
    from scripts import part23_privacy as privacy  # Part 23: people who asked to be erased stay erased

    data = privacy.suppress(data, delete={"anonymous_id": "anonymous_id", "user_id": "customer_id"})
    cat9.drop_table("raw", "clickstream_events", cat9.token())  # re-runs start from an empty table
    table = cat.create_table(EVENTS, schema=data.schema)
    with table.update_spec() as spec:
        spec.add_identity("event_date")
    table.append(data)
    signed_in = data.num_rows - data.column("user_id").null_count  # the sessions contract keys on anonymous_id
    print(f"{EVENTS}: {data.num_rows:,} events ({100 * signed_in / data.num_rows:.1f}% with a user_id), "
          f"{time.monotonic() - started:.1f} s")  # fmt: skip
    return data.num_rows


def sessions() -> dict:
    """gold.web_sessions from raw.clickstream_events by the rules in sql/part20/web_sessions.sql; replaced whole."""
    cat = cat9.pyiceberg("pipeline")
    started = time.monotonic()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    cols = ("event_id", "event_type", "event_ts", "anonymous_id", "platform", "country", "user_agent")
    con.register("events", cat.load_table(EVENTS).scan(selected_fields=cols).to_arrow())
    con.execute(SESSIONS_SQL.read_text())
    data = con.sql("SELECT * FROM web_sessions ORDER BY anonymous_id, started_at").to_arrow_table()
    if cat.table_exists(SESSIONS):
        cat.load_table(SESSIONS).overwrite(data)  # one commit; readers see the old table or the new one
    else:
        cat.create_table(SESSIONS, schema=data.schema).append(data)
    out = {"sessions": data.num_rows, "seconds": round(time.monotonic() - started, 1)}
    print(json.dumps(out))
    return out


def conversion() -> dict:
    """The contracted rate, read the way any consumer reads it: as the analyst, from gold.web_sessions."""
    con = duckdb.connect()
    con.register("rows", cat9.pyiceberg("analyst").load_table(SESSIONS).scan(selected_fields=("converted",)).to_arrow())
    sessions, converted = con.execute("SELECT count(*), count(*) FILTER (WHERE converted) FROM rows").fetchone()
    out = {"sessions": sessions, "converted": converted, "conversion_pct": round(100 * converted / sessions, 2)}
    print(json.dumps(out))
    return out


# ---------- datacontract-cli ----------


def env(contract: Path) -> dict:
    principal = PRINCIPAL[contract.name.split(".")[0].split("-")[0]]
    return os.environ | {
        "DATACONTRACT_ICEBERG_CREDENTIAL": f"{principal}:{cat9.PRINCIPALS[principal]}",
        "DATACONTRACT_ICEBERG_PROPERTIES": "scope=PRINCIPAL_ROLE:ALL,s3.region=us-east-1",
        "DATACONTRACT_ICEBERG_S3_ENDPOINT": "http://localhost:8333",
        "DATACONTRACT_TRINO_USERNAME": principal,
        "DATACONTRACT_S3_ACCESS_KEY_ID": "larkspur",  # the landing server only: the raw bucket's static lab key
        "DATACONTRACT_S3_SECRET_ACCESS_KEY": "larkspur-lab-secret",
        "DATACONTRACT_S3_REGION": "us-east-1",
        "COLUMNS": "200",
    }


def datacontract(*args: str, contract: Path | None = None) -> subprocess.CompletedProcess:
    if not DATACONTRACT.exists():
        sys.exit("datacontract-cli is missing: make contracts-install")
    return subprocess.run(
        [str(DATACONTRACT), *args],
        cwd=ROOT,
        env=env(contract) if contract else os.environ | {"COLUMNS": "200"},
        capture_output=True,
        text=True,
    )


def test(contract: Path, server: str = "lakehouse", checks: str | None = None) -> dict:
    """Run every check the contract implies on one server; print a summary and every check that didn't pass."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "result.json"
        args = ["test", str(contract), "--server", server, "--output-format", "json", "--output", str(out)]
        if checks:
            args += ["--checks", checks]
        started = time.monotonic()
        run = datacontract(*args, contract=contract)
        result = json.loads(out.read_text()) if out.exists() else {"checks": [], "result": "error"}
    counts: dict[str, int] = {}
    for c in result["checks"]:
        c["result"] = c.get("result") or "not run"
        counts[c["result"]] = counts.get(c["result"], 0) + 1
    summary = {"contract": contract.name, "server": server, "result": result["result"], "checks": counts}
    print(json.dumps(summary) + f"  ({time.monotonic() - started:.0f} s)")
    for c in result["checks"]:
        if c["result"] not in ("passed", "not run"):
            print(f"  {c['result']:<7} {c.get('field') or c.get('model') or ''}: {c['name']}: {c.get('reason') or ''}")
    if run.returncode and not result["checks"]:
        print(run.stdout[-2000:], run.stderr[-2000:])
    return summary | {"failed": [c for c in result["checks"] if c["result"] != "passed"]}


def version(path: Path) -> str:
    return re.search(r"^version: *(\S+)", path.read_text(), re.M).group(1)


def major(path: Path) -> int:
    return int(version(path).split(".")[0])


def ci_one(base: Path | None, head: Path) -> bool:
    """Lint the new contract; refuse a breaking change unless it comes as a new major version."""
    lint = datacontract("lint", str(head))
    if lint.returncode:
        print(f"FAIL {head.relative_to(ROOT)}: not valid ODCS\n{lint.stdout}")
        return False
    if base is None:
        print(f"ok   {head.relative_to(ROOT)}: new contract")
        return True
    diff = datacontract("breaking", str(base), str(head))
    lines = [line for line in diff.stdout.splitlines() if line.startswith("│")]
    rows = [[c.strip() for c in line.strip("│").split("│")] for line in lines]
    summary = [" ".join(r) for r in rows if len(r) == 3 and r[0] in ("ERROR", "WARNING")]  # the summary table
    if diff.returncode == 0:
        print(f"ok   {head.relative_to(ROOT)}: no breaking change")
        return True
    if major(head) > major(base):
        print(f"ok   {head.relative_to(ROOT)}: breaking, published as major version {major(head)} (consumers sign off)")
        return True
    print(f"FAIL {head.relative_to(ROOT)}: breaking change in version {version(head)}")
    for line in summary:
        print(f"       {line}")
    return False


def base_ref(ref: str) -> str:
    """`auto`: where this branch left origin/main (a pull request's base), else HEAD (local, uncommitted edits)."""
    if ref != "auto":
        return ref
    fork = subprocess.run(["git", "merge-base", "HEAD", "origin/main"], cwd=ROOT, capture_output=True, text=True)
    return fork.stdout.strip() if fork.returncode == 0 else "HEAD"


def ci(base: Path | None, head: Path | None, ref: str = "auto") -> bool:
    """Every contract in contracts/ (working tree) against its version at the base ref, or one explicit pair."""
    if head:
        return ci_one(base, head)
    ref = base_ref(ref)
    print(f"base: {ref}")
    ok = True
    for path in sorted(CONTRACTS.glob("*.odcs.yaml")):
        rel = path.relative_to(ROOT)
        old = subprocess.run(["git", "show", f"{ref}:./{rel}"], cwd=ROOT, capture_output=True, text=True)
        if old.returncode:
            ok &= ci_one(None, path)
            continue
        with tempfile.NamedTemporaryFile("w", suffix=".odcs.yaml") as f:
            f.write(old.stdout)
            f.flush()
            ok &= ci_one(Path(f.name), path)
    return ok


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["land", "sessions", "conversion", "test", "ci"])
    p.add_argument("contract", nargs="?", type=Path)
    p.add_argument("--server", default="lakehouse")
    p.add_argument("--checks")
    p.add_argument("--base", type=Path)
    p.add_argument("--head", type=Path)
    p.add_argument("--ref", default="auto")
    a = p.parse_args()
    if a.command == "land":
        land()
    elif a.command == "sessions":
        sessions()
    elif a.command == "conversion":
        conversion()
    elif a.command == "test":
        sys.exit(0 if test(a.contract, a.server, a.checks)["result"] == "passed" else 1)
    elif a.command == "ci":
        sys.exit(0 if ci(a.base and ROOT / a.base, a.head and ROOT / a.head, a.ref) else 1)


if __name__ == "__main__":
    main()
