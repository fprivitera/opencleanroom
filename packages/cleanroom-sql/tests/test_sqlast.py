# SPDX-License-Identifier: Apache-2.0
"""The SQLGlot node builders: a SQLGlot upgrade that renames an argument breaks here first.

Expected strings are copied from tests/golden, not computed.
"""
import duckdb
import pytest
from cleanroom_sql import Column, Rewriter, Schema, Table
from cleanroom_sql.sqlast import clamp, from_table, parse, truncated
from cleanroom_sql.taxi import SCHEMA


def test_truncated_subquery_duckdb():
    sub = truncated(SCHEMA.table("trips"), alias="trips", k=50)
    assert sub.sql(dialect="duckdb") == (
        "(SELECT trip_id, hack_license, pu_location_id, fare_amount, trip_distance "
        "FROM (SELECT trip_id, hack_license, pu_location_id, fare_amount, trip_distance, ROW_NUMBER() OVER (PARTITION BY hack_license ORDER BY trip_id) AS _rn FROM trips) AS _t "
        "WHERE _rn <= 50) AS trips"
    )


@pytest.mark.parametrize("name", ["_rn", "_RN"])
def test_row_number_alias_never_shadows_a_declared_column(name):
    """A column named like the row number must not turn the k-row filter into a filter on the data."""
    schema = Schema({"t": Table("t", "id", (Column("id"), Column("pid"), Column(name)), privacy_id="pid")})
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t(id INT, pid INT, {name} INT)")
    con.executemany("INSERT INTO t VALUES (?, ?, 0)", [(i, 1) for i in range(100)])
    sql = Rewriter(schema, k=3).rewrite("SELECT COUNT(*) FROM t").sql
    assert con.execute(sql).fetchall() == [(3,)]


def test_clamp_spark():
    col = parse("SELECT fare_amount FROM trips").expressions[0]
    assert clamp(col, 0, 20_000).sql(dialect="spark") == (
        "CASE WHEN fare_amount IS NULL THEN NULL ELSE LEAST(GREATEST(fare_amount, 0), 20000) END"
    )


def test_from_table():
    assert from_table(parse("SELECT COUNT(*) FROM trips t")).alias_or_name == "t"


def test_clamp_propagates_null_in_every_dialect():
    """NULL clamps to NULL, so SUM and COUNT(col) skip the same rows on every backend."""
    col = parse("SELECT fare_amount FROM trips").expressions[0]
    expected = "CASE WHEN fare_amount IS NULL THEN NULL ELSE LEAST(GREATEST(fare_amount, 0), 20000) END"
    assert {clamp(col, 0, 20_000).sql(dialect=d) for d in ("duckdb", "spark", "postgres")} == {expected}


def test_truncated_orders_by_declared_column():
    from cleanroom_sql import Column, Table
    t = Table("t", "pk", (Column("pk"), Column("pid"), Column("seq")), privacy_id="pid", order_by="seq")
    assert "ORDER BY seq" in truncated(t, alias="t", k=3).sql(dialect="duckdb")
