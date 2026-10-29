# SPDX-License-Identifier: Apache-2.0
"""Property test: the computed sensitivity bounds the true L1 change on neighbours.

For a random dataset D and its neighbour D' (every row of one privacy unit
removed from every private table), the L1 distance between the true,
keyset-aligned aggregate vectors must not exceed the sensitivity the
rewriter computed. No noise is involved: the test reads the executor's
true aggregate vectors through the same step `Session.query` uses.

Profiles: the default run is small so the commit gate stays fast;
CLEANROOM_SQL_HYPOTHESIS_PROFILE=full runs at least 1,000 examples per query shape.
"""
from __future__ import annotations

import os

import duckdb
import pytest
from cleanroom_sql import Rewriter, Session
from cleanroom_sql.taxi import SCHEMA, empty_connect
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

K = 3  # small, so truncation is active on most datasets
ZONES = [(1, "Manhattan"), (2, "Brooklyn"), (3, "Queens"), (4, "EWR")]
BOROUGHS = sorted({b for _, b in ZONES})
FLEETS = ["medallion", "fleet_A", "fleet_B"]

PROFILE = settings(
    max_examples=1000 if os.environ.get("CLEANROOM_SQL_HYPOTHESIS_PROFILE") == "full" else 25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

# (sql, keyset) for every supported query shape
SHAPES = {
    "single_table": ("SELECT COUNT(*), SUM(fare_amount), SUM(trip_distance) FROM trips", None),
    "single_table_avg": ("SELECT AVG(fare_amount), COUNT(*) FROM trips WHERE trip_distance > 5", None),
    "private_join_public_avg_group_by": (
        "SELECT z.borough, AVG(t.fare_amount) FROM trips t "
        "JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough",
        BOROUGHS + ["Atlantis"],
    ),
    "single_table_where": ("SELECT COUNT(*), SUM(fare_amount) FROM trips WHERE trip_distance > 5", None),
    "single_table_where_privacy_id_or": (
        "SELECT COUNT(*), SUM(fare_amount) FROM trips WHERE hack_license = 'HL1' OR trip_distance > 5", None,
    ),
    "keyed_table_group_by": ("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", FLEETS),
    "private_join_public_group_by": (
        "SELECT z.borough, COUNT(*), SUM(t.fare_amount) FROM trips t "
        "JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough",
        BOROUGHS + ["Atlantis"],
    ),
    "private_join_public_where_joined": (
        "SELECT COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id "
        "WHERE z.borough = 'Manhattan'",
        None,
    ),
    "private_join_public_where_mixed": (
        "SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id "
        "WHERE t.trip_distance > 5 AND z.borough = 'Manhattan' GROUP BY z.borough",
        BOROUGHS,
    ),
    "public_join_private": (
        "SELECT COUNT(*) FROM taxi_zones z JOIN trips t ON t.pu_location_id = z.location_id", None,
    ),
    "private_join_keyed": (
        "SELECT COUNT(*), SUM(t.fare_amount) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license",
        None,
    ),
    "keyed_join_private_group_by": (
        "SELECT d.fleet, COUNT(*) FROM drivers d JOIN trips t ON d.hack_license = t.hack_license GROUP BY d.fleet",
        FLEETS,
    ),
    "three_tables_where_group_by": (
        "SELECT z.borough, COUNT(*), SUM(t.fare_amount) FROM trips t "
        "JOIN drivers d ON t.hack_license = d.hack_license "
        "JOIN taxi_zones z ON t.pu_location_id = z.location_id "
        "WHERE d.fleet = 'fleet_A' GROUP BY z.borough",
        BOROUGHS,
    ),
}


@st.composite
def datasets(draw):
    n_drivers = draw(st.integers(1, 5))
    drivers = [(f"HL{i}", draw(st.sampled_from(FLEETS))) for i in range(n_drivers)]
    value = st.one_of(st.none(), st.integers(-100, 30_000))  # NULLs, negatives, beyond the clamp
    trips, trip_id = [], 0
    for hl, _ in drivers:
        for _ in range(draw(st.integers(0, 3 * K))):
            trip_id += 1
            trips.append((trip_id, hl, draw(st.integers(1, len(ZONES))), draw(value), draw(value)))
    removed = draw(st.sampled_from([hl for hl, _ in drivers]))
    return drivers, trips, removed


def _connect(drivers, trips) -> duckdb.DuckDBPyConnection:
    con = empty_connect()
    con.executemany("INSERT INTO taxi_zones VALUES (?, ?, ?)", [(i, b, f"zone_{i}") for i, b in ZONES])
    for row in drivers:
        con.execute("INSERT INTO drivers VALUES (?, ?)", row)
    for row in trips:
        con.execute("INSERT INTO trips VALUES (?, ?, ?, ?, ?)", row)
    return con


def _vectors_on(con, rq, keyset):
    return Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)._true_vectors(rq, keyset)[1]


@pytest.mark.parametrize("shape", list(SHAPES))
@PROFILE
@given(data=datasets())
def test_sensitivity_bounds_l1_change_on_neighbour(shape, data):
    drivers, trips, removed = data
    sql, keyset = SHAPES[shape]
    rq = Rewriter(SCHEMA, k=K).rewrite(sql)

    neighbour_drivers = [d for d in drivers if d[0] != removed]
    neighbour_trips = [t for t in trips if t[1] != removed]
    vectors = _vectors_on(_connect(drivers, trips), rq, keyset)
    neighbour = _vectors_on(_connect(neighbour_drivers, neighbour_trips), rq, keyset)

    for agg, v, w in zip(rq.measurements, vectors, neighbour):  # an AVG is a SUM and a COUNT
        l1 = sum(abs(a - b) for a, b in zip(v, w))
        assert l1 <= agg.sensitivity, (
            f"{shape}: {agg.kind}({agg.column}) moved by {l1} > Δ={agg.sensitivity}\n"
            f"removed privacy unit: {removed}\ndrivers: {drivers}\ntrips: {trips}\n"
            f"D : {v}\nD': {w}\nquery: {sql}\nrewritten: {rq.sql}"
        )


def test_private_keyed_join_bound_is_tight():
    """One driver with more than K trips: removing it moves COUNT(*) on trips ⋈ drivers by exactly K.

    Deterministic rather than Hypothesis-found, so a future loosening of the
    row-ownership rule cannot hide behind an unlucky draw.
    """
    sql = "SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license"
    rq = Rewriter(SCHEMA, k=K).rewrite(sql)
    drivers = [("HL1", "fleet_A"), ("HL2", "fleet_B")]
    trips = [(i, "HL1", 1, 100, 10) for i in range(1, 2 * K + 1)] + [(99, "HL2", 1, 100, 10)]
    full = _vectors_on(_connect(drivers, trips), rq, None)
    without = _vectors_on(_connect(drivers[1:], [t for t in trips if t[1] != "HL1"]), rq, None)
    assert rq.measurements[0].sensitivity == K
    assert abs(full[0][0] - without[0][0]) == K
