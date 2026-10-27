import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
SMALL = ["--scale", "0.2", "--store-fraction", "0.2"]


def generate(out: Path, *extra: str) -> dict:
    subprocess.run(
        [sys.executable, "-m", "datagen", *SMALL, "--out", str(out), *extra], cwd=ROOT, check=True, capture_output=True
    )
    return json.loads((out / "_manifest.json").read_text())


@pytest.fixture(scope="session")
def small_raw(tmp_path_factory) -> tuple[Path, dict]:
    out = tmp_path_factory.mktemp("raw")
    return out, generate(out)


def raw_connection(raw_root: Path | str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET VARIABLE raw_root = '{raw_root}'")
    con.execute((ROOT / "sql/raw/views.sql").read_text())
    return con


def stack_up() -> bool:
    import socket

    for port in (8333, 8181):
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("localhost", port)):
                return False
    return True


needs_stack = pytest.mark.skipif(not stack_up(), reason="needs `make up-core` and `make upload`")


@pytest.fixture(scope="session")
def lab_manifest() -> dict:
    path = ROOT / "data/raw/_manifest.json"
    if not path.exists():
        pytest.skip("run `make data` first")
    return json.loads(path.read_text())


@pytest.fixture(scope="session")
def bucket() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute((ROOT / "sql/raw/s3_secret.sql").read_text())
    con.execute("SET VARIABLE raw_root = 's3://larkspur-raw/raw'")
    con.execute((ROOT / "sql/raw/views.sql").read_text())
    return con


def trino_up() -> bool:
    import socket

    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("localhost", 8080)) == 0


needs_batch = pytest.mark.skipif(not (stack_up() and trino_up()), reason="needs `make up-trino` and `make upload`")


def trino_rows(sql: str) -> list:
    import urllib.request

    req = urllib.request.Request(
        "http://localhost:8080/v1/statement", data=sql.encode(), headers={"X-Trino-User": "tests"}
    )
    page = json.load(urllib.request.urlopen(req))
    rows = []
    while True:
        rows += page.get("data", [])
        if "error" in page:
            raise RuntimeError(page["error"]["message"])
        if "nextUri" not in page:
            return rows
        page = json.load(urllib.request.urlopen(page["nextUri"]))
