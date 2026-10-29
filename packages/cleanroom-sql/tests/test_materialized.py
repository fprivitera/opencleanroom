# SPDX-License-Identifier: Apache-2.0
"""A materialized truncated table stands in for the truncation window only when nothing is pushed into it.

The table holds exactly the rows the window keeps (the first k rows per privacy unit by the table's order), so
for a query whose WHERE has no conjunct on that table the two give the same rows. A pushed conjunct changes which
k rows the window keeps; then the window stays.
"""
from __future__ import annotations

import duckdb
import pytest
from cleanroom_sql import Rewriter, Session
from cleanroom_sql.sqlast import truncated
from cleanroom_sql.taxi import SCHEMA, populate

K = 5
PHYSICAL = {"trips": "trips__ocr_k5_s1"}


def _con():
    con = duckdb.connect()
    populate(con, n_drivers=60)
    table = SCHEMA.table("trips")
    con.execute(f"CREATE TABLE trips__ocr_k5_s1 AS {truncated(table, 'trips', K).this.sql(dialect='duckdb')}")
    return con


def test_rewriter_reads_the_materialized_table_when_nothing_is_pushed():
    rq = Rewriter(SCHEMA, k=K, materialized=PHYSICAL).rewrite("SELECT COUNT(*), SUM(fare_amount) FROM trips")
    assert "trips__ocr_k5_s1" in rq.sql and "ROW_NUMBER" not in rq.sql.upper()


def test_rewriter_keeps_the_window_when_a_conjunct_is_pushed():
    rq = Rewriter(SCHEMA, k=K, materialized=PHYSICAL).rewrite("SELECT COUNT(*) FROM trips WHERE trip_distance > 5")
    assert "trips__ocr_k5_s1" not in rq.sql and "ROW_NUMBER" in rq.sql.upper()


def test_alias_and_join_keep_working_on_the_materialized_table():
    rq = Rewriter(SCHEMA, k=K, materialized=PHYSICAL).rewrite(
        "SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough")
    assert "trips__ocr_k5_s1 AS t" in rq.sql


@pytest.mark.parametrize("sql,keyset", [
    ("SELECT COUNT(*), SUM(fare_amount), SUM(trip_distance) FROM trips", None),
    ("SELECT z.borough, COUNT(*), SUM(t.fare_amount) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough",
     ["Manhattan", "Brooklyn", "Queens", "Bronx"]),
    ("SELECT d.fleet, COUNT(*) FROM drivers d JOIN trips t ON d.hack_license = t.hack_license GROUP BY d.fleet", ["medallion", "fleet_A", "fleet_B"]),
    ("SELECT COUNT(*) FROM trips WHERE trip_distance > 5", None),
])
def test_same_true_aggregates_with_and_without_the_materialized_table(sql, keyset):
    con = _con()
    window = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    table = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K, materialized=PHYSICAL)
    rq_w = Rewriter(SCHEMA, k=K).rewrite(sql)
    rq_m = Rewriter(SCHEMA, k=K, materialized=PHYSICAL).rewrite(sql)
    assert window._true_vectors(rq_w, keyset) == table._true_vectors(rq_m, keyset)
