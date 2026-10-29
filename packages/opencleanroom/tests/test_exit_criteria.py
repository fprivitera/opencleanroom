# SPDX-License-Identifier: Apache-2.0
"""The serving guarantees, as tests: no overspend under concurrency, crash consistency, audit reconciliation."""
from __future__ import annotations

import subprocess
import sys
import threading
from fractions import Fraction
from pathlib import Path

import duckdb
import pytest
import yaml
from cleanroom_sql import BudgetExhaustedException
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from opencleanroom.runtime import Runtime

TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()


def _policy_text(**changes) -> str:
    doc = yaml.safe_load(TAXI_YAML)
    for dotted, value in changes.items():
        node = doc
        *parents, last = dotted.split(".")
        for p in parents:
            node = node[p]
        node[last] = value
    return yaml.safe_dump(doc, sort_keys=False)


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=50)
    con.close()
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "duckdb-dev", "duckdb", str(db))
    catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epsilon": "5.0", "consumers.analytics-team.budget.epsilon": "5.0",
                                              "consumers.analytics-team.max_epsilon_per_query": 0.1}))
    return cat, catalog.create_consumer(cat, "nyc_taxi", "analytics-team")


def _spend(args):
    cat, key, n = args
    rt = Runtime(cat)
    ok = 0
    for _ in range(n):
        try:
            rt.query(key, "SELECT COUNT(*) FROM trips", 0.1)
            ok += 1
        except BudgetExhaustedException:
            pass
    return ok


WORKER = """
import sys
from cleanroom_sql import BudgetExhaustedException
from opencleanroom.runtime import Runtime
rt = Runtime(sys.argv[1]); ok = 0
for _ in range(int(sys.argv[3])):
    try:
        rt.query(sys.argv[2], "SELECT COUNT(*) FROM trips", 0.1); ok += 1
    except BudgetExhaustedException:
        pass
print(ok)
"""


def test_100_parallel_queries_never_overspend(env):
    """50 threads in this process and 2 other processes × 25 queries race on one file: exactly 50 of 100 succeed."""
    cat, key = env
    results = []
    threads = [threading.Thread(target=lambda: results.append(_spend((cat, key, 1)))) for _ in range(50)]
    procs = [subprocess.Popen([sys.executable, "-c", WORKER, str(cat), key, "25"], stdout=subprocess.PIPE, text=True,
                              cwd=Path(__file__).resolve().parents[1]) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for p in procs:
        out, _ = p.communicate(timeout=120)
        results.append(int(out.strip()))
    assert sum(results) == 50  # budget 5.0 / 0.1
    assert Runtime(cat).remaining("nyc_taxi", "analytics-team") == 0


def test_killed_worker_leaves_ledger_consistent(env):
    cat, key = env
    code = f"""
from fractions import Fraction
from opencleanroom.runtime import Runtime
rt = Runtime({str(cat)!r})
_, ledger = rt._policy_and_ledger("nyc_taxi")
ledger.for_request("dead", "SELECT COUNT(*) FROM trips").reserve("analytics-team", Fraction(3, 10))
import os; os._exit(9)  # killed between reserve and commit
"""
    r = subprocess.run([sys.executable, "-c", code], check=False, cwd=Path(__file__).resolve().parents[1])
    assert r.returncode == 9
    rt = Runtime(cat)
    assert rt.remaining("nyc_taxi", "analytics-team") == Fraction(47, 10)  # the reservation counts as spent
    rt.query(key, "SELECT COUNT(*) FROM trips", 0.1)  # the ledger still works
    _, ledger = rt._policy_and_ledger("nyc_taxi")
    assert [row.request_id for row in ledger.reserved_rows()] == ["dead"]


def test_audit_reconciles_with_ledger(env, monkeypatch):
    """Exit criterion: after ok, rejected, failed and free requests the two tables agree; a hand-edited ledger row is caught."""
    import sqlite3

    from cleanroom_sql import QueryFailed, UnsupportedQuery
    from opencleanroom.backends import DuckDBBackend
    cat, key = env
    rt = Runtime(cat)
    for _ in range(3):
        rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)
    rt.public_query(key, "SELECT zone FROM taxi_zones LIMIT 5")
    with pytest.raises(UnsupportedQuery):
        rt.query(key, "SELECT MAX(fare_amount) FROM trips", epsilon=0.1)
    with monkeypatch.context() as m:
        m.setattr(DuckDBBackend, "session_con", lambda self: duckdb.connect())
        with pytest.raises(QueryFailed):
            rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)  # charged up to the per-query maximum
    [rec] = rt.reconcile("nyc_taxi")
    assert rec.ok and rec.ledger_committed == rec.audit_charged == Fraction(4, 10)  # three queries plus the penalty of 0.1
    assert rec.reserved_rows == 0 and rec.in_progress_rows == 0
    with sqlite3.connect(cat) as con:
        con.execute("UPDATE ledger SET epsilon = '1/5' WHERE id = (SELECT MIN(id) FROM ledger)")
    [rec] = rt.reconcile("nyc_taxi")
    assert not rec.ok and rec.ledger_committed - rec.audit_charged == Fraction(1, 10)
