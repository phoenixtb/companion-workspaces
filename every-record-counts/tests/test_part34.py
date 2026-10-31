"""Part 34: what the post claims about the platform's checks.

Host-only (run on any machine and in GitHub Actions): the LAB_HOST_ONLY switch, the workflow's pins and scope, the
pre-commit config, dbt parse catching a broken ref. With `make up-trino`: the reconciliation smoke test catches a
refund-dating change that dbt's tests pass, and passes after the fix (about a minute). With `make up-core` and OpenTofu
on the PATH: the source-domain kit is applied, a hand-made grant shows up as drift and is corrected, then destroyed.
"""

import os
import re
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from scripts import part17_dbt as p17
from scripts import part34_platform as lab
from tests.conftest import needs_batch, needs_stack

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT.parent / ".github/workflows/every-record-counts.yml"
dbt_v1 = pytest.mark.skipif(not (Path(sys.executable).parent / "dbt").exists(), reason="needs `make dbt-install`")
has_tofu = pytest.mark.skipif(shutil.which("tofu") is None, reason="needs OpenTofu on the PATH")


def test_host_only_skips_service_tests_and_keeps_tool_tests():
    env = os.environ | {"LAB_HOST_ONLY": "1"}
    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-rs", "tests/test_part01.py", "tests/test_part20.py"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, run.stdout[-2000:]
    # with or without the lab running: Part 1's Trino tests skipped, Part 20's two contract checks marked only
    # `needs make contracts-install` (a host tool) ran
    assert "2 passed, 11 skipped" in run.stdout, run.stdout[-500:]


def test_workflow_is_pinned_scoped_and_read_only():
    wf = yaml.safe_load(WORKFLOW.read_text())
    on = wf[True]  # YAML 1.1 reads the key `on` as a boolean
    assert on["push"]["branches"] == ["main"]
    for event in ("push", "pull_request"):
        assert "every-record-counts/**" in on[event]["paths"]
    assert wf["permissions"] == {"contents": "read"}
    steps = wf["jobs"]["check"]["steps"]
    for step in steps:
        if "uses" in step:
            assert re.fullmatch(r"[\w-]+/[\w-]+@[0-9a-f]{40}", step["uses"]), step["uses"]  # a commit, not a tag
    assert wf["jobs"]["check"]["runs-on"] == "ubuntu-24.04-arm"  # what the steps were validated on
    runs = [s["run"] for s in steps if "run" in s]
    assert runs == ["make dbt-install contracts-install", "make check", "make infra-validate"]
    assert steps[0]["with"]["fetch-depth"] == 0  # contract-ci reads the base commit


def test_pre_commit_ruff_matches_the_lock():
    text = (ROOT / ".pre-commit-config.yaml").read_text()
    config = yaml.safe_load(text)
    rev = next(r["rev"] for r in config["repos"] if r["repo"].endswith("ruff-pre-commit"))
    assert re.fullmatch(r"[0-9a-f]{40}", rev)  # frozen to a commit, like the workflow's actions
    frozen = re.search(rf"rev: {rev} # frozen: v(\S+)", text).group(1)
    locked = re.search(r'\[\[package\]\]\nname = "ruff"\nversion = "([^"]+)"', (ROOT / "uv.lock").read_text()).group(1)
    assert frozen == locked
    hooks = {h["id"]: h for r in config["repos"] for h in r["hooks"]}
    assert hooks["check-lake"]["stages"] == ["pre-push"] and "stages" not in hooks["contract-ci"]


def test_window_days_follow_make_dbt_ci():
    m = re.search(r"--event-time-start (\S+) --event-time-end (\S+)", (ROOT / "Makefile").read_text())
    start, end = date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2))
    # whole local business days inside [start 00:00 UTC, end 00:00 UTC) for UTC+1, +5:30 and +8
    assert lab.WINDOW_DAYS == (start + timedelta(days=1), end - timedelta(days=1))


@dbt_v1
def test_dbt_parse_catches_a_broken_ref(tmp_path):
    project = tmp_path / "larkspur"
    shutil.copytree(p17.PROJECT, project, ignore=shutil.ignore_patterns("target", "logs", "state", "dbt_packages"))
    (project / "dbt_packages").symlink_to(p17.PROJECT / "dbt_packages")
    model = project / "models/marts/rpt_daily_net_revenue_v1.sql"
    model.write_text(model.read_text().replace("ref('fct_refund')", "ref('fct_refunds')"))
    run = p17.dbt("parse", "--target", "dev", "--no-partial-parse", project=project, echo=False)
    assert run.returncode != 0
    assert "fct_refunds" in run.stdout and "was not found" in run.stdout


@pytest.fixture
def defect():
    lab.edit(lab.GOOD, lab.BAD)
    try:
        yield
    finally:
        if lab.BAD in lab.MODEL.read_text():
            lab.edit(lab.BAD, lab.GOOD)


@needs_batch
@dbt_v1
def test_the_smoke_test_catches_what_dbt_tests_pass(defect, capsys):
    assert lab.lake_check() == 1
    out = capsys.readouterr().out
    assert "Done. PASS=2 WARN=0 ERROR=0" in out  # the model and its grain test: dbt is satisfied
    assert "Q3 GB net revenue 29,832,649.44, board pack 29,790,674.06" in out
    assert "reconciliation smoke: FAIL rpt_daily_net_revenue" in out
    lab.edit(lab.BAD, lab.GOOD)
    assert lab.lake_check() == 0


@needs_stack
@has_tofu
def test_tofu_applies_the_kit_and_corrects_drift():
    def make(target: str) -> str:
        run = subprocess.run(["make", "-s", target], cwd=ROOT, capture_output=True, text=True)
        assert run.returncode == 0, run.stdout[-2000:] + run.stderr[-2000:]
        return run.stdout

    def psql(sql: str) -> None:
        subprocess.run(
            ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "larkspur", "-d", "larkspur", "-c", sql],
            cwd=ROOT,
            check=True,
            capture_output=True,
        )

    try:
        make("infra-plan")
        assert "Apply complete! Resources: 12 added" in make("infra-apply")
        assert "No changes." in make("infra-plan")
        psql("GRANT CREATE ON SCHEMA loyalty TO loyalty_reader")  # a hand-made "fix" outside the code
        plan = make("infra-plan")
        assert "module.loyalty.postgresql_grant.usage will be updated in-place" in plan
        assert "Plan: 0 to add, 1 to change, 0 to destroy." in plan
        make("infra-apply")
        assert "No changes." in make("infra-plan")
    finally:
        make("infra-destroy")
