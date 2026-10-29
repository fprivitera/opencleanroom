# SPDX-License-Identifier: Apache-2.0
"""Golden rewritten SQL per query shape and backend dialect, plus rewrite latency.

One snapshot per (shape, dialect) under tests/golden/. Any change in the
generated SQL fails the test; CLEANROOM_SQL_UPDATE_GOLDEN=1 rewrites the snapshots
so the diff can be reviewed in git.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
import sqlglot
from cleanroom_sql import Column, Rewriter, Schema, Table
from cleanroom_sql.taxi import SCHEMA
from test_sensitivity_property import SHAPES

GOLDEN = Path(__file__).parent / "golden"
DIALECTS = ["duckdb", "spark", "postgres"]
K = 50


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_rewritten_sql_matches_golden(shape, dialect):
    sql = Rewriter(SCHEMA, k=K, dialect=dialect).rewrite(SHAPES[shape][0]).sql
    path = GOLDEN / f"{shape}.{dialect}.sql"
    if os.environ.get("CLEANROOM_SQL_UPDATE_GOLDEN"):
        path.write_text(sql + "\n")
    assert path.exists(), f"missing snapshot {path.name}: run CLEANROOM_SQL_UPDATE_GOLDEN=1 pytest"
    assert sql == path.read_text().rstrip("\n"), (
        f"rewritten SQL changed for {shape} ({dialect}); review with CLEANROOM_SQL_UPDATE_GOLDEN=1 pytest"
    )


def test_no_orphan_snapshots():
    expected = {f"{s}.{d}.sql" for s in SHAPES for d in DIALECTS}
    assert {p.name for p in GOLDEN.glob("*.sql")} == expected


@pytest.mark.parametrize("dialect", ["spark", "postgres"])
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_golden_sql_parses_in_its_dialect(shape, dialect):
    sqlglot.parse_one((GOLDEN / f"{shape}.{dialect}.sql").read_text(), read=dialect)


def test_rewrite_latency_with_ten_public_joins():
    lookups = {
        f"l{i}": Table(name=f"l{i}", primary_key="id", columns=(Column("id"), Column(f"label{i}")))
        for i in range(10)
    }
    private = Table(
        name="events", primary_key="event_id", privacy_id="user_id",
        columns=(Column("event_id"), Column("user_id"), Column("amount", lower=0, upper=1000))
        + tuple(Column(f"k{i}") for i in range(10)),
        foreign_keys={f"k{i}": (f"l{i}", "id") for i in range(10)},
    )
    schema = Schema(tables={"events": private, **lookups})
    sql = "SELECT l0.label0, COUNT(*), SUM(e.amount) FROM events e " + " ".join(
        f"JOIN l{i} ON e.k{i} = l{i}.id" for i in range(10)
    ) + " GROUP BY l0.label0"
    rw = Rewriter(schema, k=K)
    times = []
    for _ in range(200):
        t0 = time.perf_counter()
        rw.rewrite(sql)
        times.append(time.perf_counter() - t0)
    p95 = sorted(times)[189]
    assert p95 < 0.050, f"p95 rewrite latency {p95 * 1000:.1f} ms"
