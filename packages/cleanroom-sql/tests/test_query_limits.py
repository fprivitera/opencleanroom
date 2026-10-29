# SPDX-License-Identifier: Apache-2.0
"""Planning time is not covered by a backend timeout: oversized queries are refused before any budget moves."""
from __future__ import annotations

from fractions import Fraction
from pathlib import Path

import pytest
from cleanroom_sql import Ledger, QueryFailed, Rewriter, Session, UnsupportedQuery, loads_policy
from cleanroom_sql.rewriter import MAX_QUERY_NODES, MAX_SQL_CHARS
from cleanroom_sql.taxi import SCHEMA

TAXI = loads_policy((Path(__file__).resolve().parents[1] / "examples" / "taxi.yaml").read_text())


def test_a_long_query_is_refused():
    sql = "SELECT COUNT(*) FROM trips WHERE hack_license = '" + "x" * MAX_SQL_CHARS + "'"
    with pytest.raises(UnsupportedQuery, match="longer"):
        Rewriter(SCHEMA, k=5).rewrite(sql)


def test_a_query_with_too_many_terms_is_refused():
    terms = " OR ".join(f"(trip_distance = {i} AND fare_amount > {i})" for i in range(MAX_QUERY_NODES // 4))
    assert len(terms) < MAX_SQL_CHARS
    with pytest.raises(UnsupportedQuery, match="terms"):
        Rewriter(SCHEMA, k=5).rewrite(f"SELECT COUNT(*) FROM trips WHERE {terms}")
    with pytest.raises(UnsupportedQuery, match="terms"):
        Rewriter(SCHEMA, k=5).check_public(f"SELECT zone FROM taxi_zones WHERE {terms.replace('trip_distance', 'location_id').replace('fare_amount', 'location_id')}")


def test_ordinary_queries_are_far_below_the_limits():
    sql = ("SELECT z.borough, COUNT(*), SUM(t.fare_amount) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
           "JOIN taxi_zones z ON t.pu_location_id = z.location_id WHERE d.fleet = 'fleet_A' AND t.trip_distance BETWEEN 1 AND 9 GROUP BY z.borough")
    Rewriter(SCHEMA, k=5).rewrite(sql)


class _FailingCon:
    def execute(self, sql):
        raise RuntimeError("Interrupted: /secret/path")  # what a timed-out cursor raises, with text that must not travel


def test_a_failed_keyset_read_is_query_failed_and_charges_nothing():
    ledger = Ledger(TAXI)
    session = Session.from_policy(TAXI, "analytics-team", _FailingCon(), ledger=ledger)
    with pytest.raises(QueryFailed) as e:
        session.query("SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough", epsilon=0.1)
    assert "secret" not in str(e.value)
    assert ledger.spent("analytics-team") == Fraction(0)  # only a public table was read, before any reservation
