# SPDX-License-Identifier: Apache-2.0
"""examples/quickstart.sh: empty directory to a noisy count in ten commands or fewer."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "examples" / "quickstart.sh"


def test_quickstart_runs_from_an_empty_directory(tmp_path):
    env = {**os.environ, "PYTHON": sys.executable, "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]}
    r = subprocess.run(["bash", str(SCRIPT)], check=False, cwd=tmp_path, capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    last = r.stdout.strip().splitlines()[-1]
    assert last.lstrip("-").isdigit(), last  # the noisy count


def test_quickstart_has_at_most_ten_commands():
    lines = [l.strip() for l in SCRIPT.read_text().splitlines()]
    commands = [l for l in lines if l and not l.startswith("#") and not l.startswith("set ") and not l.startswith("cd ")]
    assert 1 <= len(commands) <= 10, commands
