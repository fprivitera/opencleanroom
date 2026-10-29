# SPDX-License-Identifier: Apache-2.0
"""Every backend query has a timeout; a timed-out query is a charged query_failed."""
from __future__ import annotations

import os
import sqlite3
import time
from fractions import Fraction
from pathlib import Path

import duckdb
import pytest
import yaml
from cleanroom_sql import QueryFailed
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from opencleanroom.runtime import Runtime

URL = os.environ.get("OCR_SPARK_URL")
TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()
MANY = 5_000_000
SLOW_TRIPS = f"""SELECT i::INT AS trip_id, 'HL' || (i % 1000) AS hack_license, (i % 263 + 1)::INT AS pu_location_id,
                  (i % 20000)::INT AS fare_amount, (i % 500)::INT AS trip_distance FROM range({MANY}) t(i)"""


def test_catalog_stores_a_query_timeout_and_init_upgrades_old_catalogs(tmp_path):
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "a", "duckdb", "/x.duckdb")
    catalog.add_backend(cat, "b", "duckdb", "/y.duckdb", query_timeout=2.5)
    assert [(b.name, b.query_timeout) for b in catalog.list_backends(cat)] == [("a", 30.0), ("b", 2.5)]
    for bad in (0, -1):
        with pytest.raises(ValueError):
            catalog.add_backend(cat, "c", "duckdb", "/z.duckdb", query_timeout=bad)
    old = tmp_path / "old.sqlite"  # a catalog written before the column existed
    with sqlite3.connect(old) as con:
        con.execute("CREATE TABLE backends (name TEXT PRIMARY KEY, kind TEXT NOT NULL, location TEXT NOT NULL, created_at TEXT NOT NULL)")
        con.execute("INSERT INTO backends VALUES ('legacy', 'duckdb', '/l.duckdb', '2026-01-01')")
    catalog.init(old)
    assert catalog.get_backend(old, "legacy").query_timeout == 30.0


def _runtime_on(cat, backend_kind, location, timeout):
    catalog.init(cat)
    catalog.add_backend(cat, "slow", backend_kind, location, query_timeout=timeout)
    doc = yaml.safe_load(TAXI_YAML)
    doc["dataset"]["backend"] = "slow"
    doc["dataset"]["dialect"] = backend_kind
    doc["dataset"]["truncation"]["k"] = MANY  # truncation keeps every row: the window runs over all of them
    catalog.apply_policy(cat, yaml.safe_dump(doc, sort_keys=False))
    return Runtime(cat), catalog.create_consumer(cat, "nyc_taxi", "analytics-team")


def _assert_timed_out_and_charged(rt, key, timeout, margin):
    before = rt.remaining_for_key(key)
    started = time.monotonic()
    with pytest.raises(QueryFailed):
        rt.query(key, "SELECT COUNT(*), SUM(fare_amount) FROM trips", epsilon=0.1)
    assert time.monotonic() - started < timeout + margin
    assert before - rt.remaining_for_key(key) == Fraction(1, 2)  # charged up to max_epsilon_per_query
    assert rt.audit("nyc_taxi")[-1].outcome == "query_failed"


def test_duckdb_query_stops_at_the_timeout(tmp_path):
    db = tmp_path / "slow.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=20)
    con.execute("DROP TABLE trips")
    con.execute(f"CREATE TABLE trips AS {SLOW_TRIPS}")
    con.close()
    rt, key = _runtime_on(tmp_path / "catalog.sqlite", "duckdb", str(db), 0.02)
    _assert_timed_out_and_charged(rt, key, 0.02, 2.0)


@pytest.mark.skipif(not URL, reason="needs a Spark Connect server: scripts/spark-connect.sh up")
def test_spark_query_stops_at_the_timeout_and_the_session_survives(tmp_path):
    from opencleanroom.backends import SparkBackend
    admin = SparkBackend(URL).spark  # trusted path builds the slow data
    admin.sql("CREATE DATABASE IF NOT EXISTS ocr_slow").collect()
    duck = duckdb.connect()
    populate(duck, n_drivers=20)
    for table, types in (("taxi_zones", "location_id INT, borough STRING, zone STRING"), ("drivers", "hack_license STRING, fleet STRING")):
        admin.createDataFrame([tuple(r) for r in duck.execute(f"SELECT * FROM {table}").fetchall()], schema=types) \
            .write.mode("overwrite").saveAsTable(f"ocr_slow.{table}")
    admin.sql("DROP TABLE IF EXISTS ocr_slow.trips").collect()
    admin.sql(f"""CREATE TABLE ocr_slow.trips USING parquet AS
                  SELECT CAST(id AS INT) AS trip_id, concat('HL', id % 1000) AS hack_license, CAST(id % 263 + 1 AS INT) AS pu_location_id,
                         CAST(id % 20000 AS INT) AS fare_amount, CAST(id % 500 AS INT) AS trip_distance FROM range({MANY})""").collect()
    rt, key = _runtime_on(tmp_path / "catalog.sqlite", "spark", f"{URL}/;database=ocr_slow", 0.5)
    _assert_timed_out_and_charged(rt, key, 0.5, 15.0)
    backend = rt._backend("slow")
    assert backend.session_con().execute("SELECT COUNT(*) FROM taxi_zones").fetchall() == [(263,)]  # the session still serves


@pytest.mark.parametrize("bad", [float("inf"), float("nan"), 1e10, 1e-6, 0, -1])
def test_query_timeouts_outside_the_working_range_are_refused(tmp_path, bad):
    from opencleanroom.backends import DuckDBBackend
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    with pytest.raises(ValueError):
        catalog.add_backend(cat, "x", "duckdb", "/x.duckdb", query_timeout=bad)
    db = tmp_path / "t.duckdb"
    duckdb.connect(str(db)).close()
    with pytest.raises(ValueError):  # a hand-edited catalog row cannot switch the timer off either
        DuckDBBackend(str(db), query_timeout=bad)


def test_the_watchdog_is_gone_when_the_query_returns(tmp_path):
    import threading

    from opencleanroom.backends import DuckDBBackend
    db = tmp_path / "t.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE t AS SELECT range AS x FROM range(1000)")
    con.close()
    session = DuckDBBackend(str(db), query_timeout=5).session_con()
    before = threading.active_count()
    for _ in range(200):
        assert session.execute("SELECT COUNT(*) FROM t").fetchall() == [(1000,)]
    assert threading.active_count() == before  # each watchdog was joined: none can interrupt a later query
