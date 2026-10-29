# SPDX-License-Identifier: Apache-2.0
"""`python -m cleanroom_sql policy check <file>`: exit 0 on a valid policy, 1 with every error listed, 2 on a missing file."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from test_policy import _policy

ROOT = Path(__file__).resolve().parents[1]


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-m", "cleanroom_sql", *args], check=False, cwd=ROOT, capture_output=True, text=True)


def test_check_valid_policy_exits_zero(tmp_path):
    f = tmp_path / "ok.yaml"
    f.write_text(_policy())
    r = _run("policy", "check", str(f))
    assert r.returncode == 0, r.stderr
    assert "OK: nyc_taxi, 1 consumers" in r.stdout


def test_check_invalid_policy_lists_every_error(tmp_path):
    def m(d):
        d["dataset"]["tables"]["trips"]["order_by"] = "fare_amount"
        d["consumers"]["analytics-team"]["aggregates"] = ["count", "max"]
        d["consumers"]["analytics-team"]["budget"]["epsilon"] = 11
    f = tmp_path / "bad.yaml"
    f.write_text(_policy(m))
    r = _run("policy", "check", str(f))
    assert r.returncode == 1
    lines = [l for l in r.stderr.splitlines() if ": " in l]
    assert len(lines) == 3
    assert any(l.startswith("dataset.tables.trips.order_by: ") for l in lines)
    assert any(l.startswith("consumers.analytics-team.aggregates: ") for l in lines)
    assert any(l.startswith("consumers.analytics-team.budget.epsilon: ") for l in lines)


def test_check_missing_file_exits_two(tmp_path):
    r = _run("policy", "check", str(tmp_path / "nope.yaml"))
    assert r.returncode == 2 and "nope.yaml" in r.stderr


def test_console_script_is_declared():
    text = (ROOT / "pyproject.toml").read_text()
    assert 'cleanroom-sql = "cleanroom_sql.__main__:main"' in text
