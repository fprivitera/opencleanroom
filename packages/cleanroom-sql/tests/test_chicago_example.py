# SPDX-License-Identifier: Apache-2.0
"""The Chicago Taxi Trips example policy loads and runs, on synthetic rows shaped like the real data."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from cleanroom_sql import PolicyViolation, Session, UnsupportedQuery, load_policy

POLICY = load_policy(Path(__file__).resolve().parents[1] / "examples" / "chicago_taxi.yaml")


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE community_areas(area_number INTEGER, name VARCHAR)")
    con.execute("INSERT INTO community_areas VALUES (8, 'NEAR NORTH SIDE'), (32, 'LOOP'), (76, 'OHARE')")
    con.execute("""CREATE TABLE trips(trip_id VARCHAR, taxi_id VARCHAR, trip_date DATE, pickup_community_area INTEGER,
                   dropoff_community_area INTEGER, trip_seconds INTEGER, trip_miles INTEGER, fare INTEGER, tips INTEGER,
                   payment_type VARCHAR, company VARCHAR)""")
    con.execute("""INSERT INTO trips VALUES
        ('t1', 'a', DATE '2025-03-01', 32, 8, 600, 25, 1500, 300, 'Credit Card', 'Flash Cab'),
        ('t2', 'a', DATE '2025-03-01', 76, 32, 1800, 170, 4500, 0, 'Cash', 'Flash Cab'),
        ('t3', 'b', DATE '2025-03-02', 8, 8, 9000, 900, 99999, 5000, 'Mobile', 'Sun Taxi')""")
    return con


def test_group_by_community_area_uses_the_public_keyset():
    s = Session.from_policy(POLICY, "city-analytics", _con())
    res = s.query("SELECT z.name, COUNT(*) FROM trips t JOIN community_areas z ON t.pickup_community_area = z.area_number GROUP BY z.name",
                  epsilon=0.3)
    assert sorted(r[0] for r in res.rows) == ["LOOP", "NEAR NORTH SIDE", "OHARE"]
    assert res.sensitivities == (POLICY.dataset.k,)


def test_payment_types_come_from_the_admin_list_and_sums_are_clamped():
    s = Session.from_policy(POLICY, "city-analytics", _con())
    res = s.query("SELECT payment_type, COUNT(*), SUM(fare) FROM trips GROUP BY payment_type", epsilon=0.3)
    assert len(res.rows) == len(POLICY.dataset.keysets[("trips", "payment_type")])
    assert res.sensitivities == (POLICY.dataset.k, POLICY.dataset.k * 8000)  # a fare counts at most $80


def test_the_taxi_id_and_ungoverned_groups_are_refused():
    s = Session.from_policy(POLICY, "city-analytics", _con())
    with pytest.raises(UnsupportedQuery):
        s.query("SELECT taxi_id FROM trips", epsilon=0.1)
    with pytest.raises(PolicyViolation):  # no keyset for company: its values are not public
        s.query("SELECT company, COUNT(*) FROM trips GROUP BY company", epsilon=0.1)
