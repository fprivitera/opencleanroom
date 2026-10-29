# SPDX-License-Identifier: Apache-2.0
"""The three example policies load and run one query end to end on DuckDB."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from cleanroom_sql import Session, load_policy
from cleanroom_sql.taxi import SCHEMA

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
SQL_TYPES = {"int": "INTEGER", "string": "VARCHAR", "bool": "BOOLEAN", "date": "DATE"}


def _connect_from_policy(policy) -> duckdb.DuckDBPyConnection:
    """Empty tables with the policy's columns and types: tests insert what they need."""
    con = duckdb.connect()
    for t in policy.dataset.schema.tables.values():
        cols = ", ".join(f"{c.name} {SQL_TYPES[c.type]}" for c in t.columns)
        con.execute(f"CREATE TABLE {t.name}({cols})")
    return con


def test_taxi_example_equals_the_library_schema():
    assert load_policy(EXAMPLES / "taxi.yaml").dataset.schema == SCHEMA


def test_taxi_example_runs_a_group_by_with_a_policy_keyset():
    policy = load_policy(EXAMPLES / "taxi.yaml")
    con = _connect_from_policy(policy)
    con.execute("INSERT INTO taxi_zones VALUES (1, 'Manhattan', 'z1'), (2, 'Queens', 'z2')")
    con.execute("INSERT INTO drivers VALUES ('HL1', 'fleet_A')")
    con.execute("INSERT INTO trips VALUES (1, 'HL1', 1, 500, 10)")
    s = Session.from_policy(policy, "analytics-team", con)
    res = s.query("SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough", epsilon=0.1)
    assert sorted(r[0] for r in res.rows) == ["Manhattan", "Queens"]


def test_single_table_example():
    policy = load_policy(EXAMPLES / "single_table.yaml")
    con = _connect_from_policy(policy)
    con.execute("INSERT INTO events VALUES (1, 'u1', 300), (2, 'u1', 900), (3, 'u2', 50)")
    s = Session.from_policy(policy, "reporting", con)
    res = s.query("SELECT COUNT(*), SUM(amount) FROM events", epsilon=0.1)
    assert len(res.rows) == 1 and res.sensitivities == (policy.dataset.k, policy.dataset.k * 1000)


def test_two_private_tables_example_joins_inside_one_privacy_unit():
    policy = load_policy(EXAMPLES / "two_private_tables.yaml")
    assert policy.dataset.schema.table("tips").privacy_id == "driver"
    con = _connect_from_policy(policy)
    con.execute("INSERT INTO drivers VALUES ('HL1', 'fleet_A')")
    con.execute("INSERT INTO trips VALUES (1, 'HL1', 500)")
    con.execute("INSERT INTO tips VALUES (1, 'HL1', 100)")
    s = Session.from_policy(policy, "finance", con)
    res = s.query("SELECT COUNT(*), SUM(p.amount) FROM tips p JOIN drivers d ON p.driver = d.hack_license", epsilon=0.1)
    assert res.sensitivities[0] == policy.dataset.k  # row ownership: tips ⋈ drivers costs k, not k + 1


@pytest.mark.parametrize("name", ["taxi.yaml", "single_table.yaml", "two_private_tables.yaml"])
def test_every_example_passes_policy_check(name):
    import subprocess
    import sys
    r = subprocess.run([sys.executable, "-m", "cleanroom_sql", "policy", "check", str(EXAMPLES / name)], check=False, capture_output=True, text=True, cwd=EXAMPLES.parent)
    assert r.returncode == 0, r.stderr
