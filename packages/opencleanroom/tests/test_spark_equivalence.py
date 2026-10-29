# SPDX-License-Identifier: Apache-2.0
"""DuckDB and Spark return identical true aggregates on the whole query corpus.

The corpus is the library's query shapes (`SHAPES`, the same queries the goldens and the golden policies
snapshot), each with its keyset. Every shape runs on two datasets: the synthetic taxi data, and a small one
with NULLs, negatives, values beyond the clamp, NULL group keys and join keys that match nothing. DuckDB is
the oracle. Equality is exact on the keyset-aligned integer vectors `Session.query` would add noise to.
"""
from __future__ import annotations

import importlib.util
import os
import random
from pathlib import Path

import duckdb
import pytest
from cleanroom_sql import Rewriter, Session, loads_policy
from cleanroom_sql.taxi import SCHEMA, empty_connect, populate

URL = os.environ.get("OCR_SPARK_URL")
pytestmark = pytest.mark.skipif(not URL, reason="needs a Spark Connect server: scripts/spark-connect.sh up")

_LIB_TESTS = Path(__file__).resolve().parents[2] / "cleanroom-sql" / "tests"
_spec = importlib.util.spec_from_file_location("cleanroom_sql_shapes", _LIB_TESTS / "test_sensitivity_property.py")
_shapes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_shapes)
SHAPES, ZONES = _shapes.SHAPES, _shapes.ZONES

SPARK_TYPES = {
    "taxi_zones": "location_id INT, borough STRING, zone STRING",
    "drivers": "hack_license STRING, fleet STRING",
    "trips": "trip_id INT, hack_license STRING, pu_location_id INT, fare_amount INT, trip_distance INT",
}


def _nulls_dataset() -> duckdb.DuckDBPyConnection:
    rng = random.Random(7)
    con = empty_connect()
    con.executemany("INSERT INTO taxi_zones VALUES (?, ?, ?)", [(i, b, f"zone_{i}") for i, b in ZONES])
    fleets = ["medallion", "fleet_A", "fleet_B", None]  # a NULL group key
    drivers = [(f"HL{i}", fleets[i % 4]) for i in range(12)]
    con.executemany("INSERT INTO drivers VALUES (?, ?)", drivers)
    values = [None, None, -50, 0, 7, 499, 20_000, 25_000]  # NULLs, a negative, the clamp edges, beyond the clamp
    trips, trip_id = [], 0
    for hl, _ in drivers:
        for _ in range(rng.randint(0, 9)):
            trip_id += 1
            zone = rng.choice([1, 2, 3, 4, 5, None])  # 5 and NULL match no zone
            trips.append((trip_id, hl, zone, rng.choice(values), rng.choice(values)))
    con.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?)", trips)
    return con


def _taxi_dataset() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    populate(con, n_drivers=150)
    return con


DATASETS = {"taxi": (_taxi_dataset, 50), "nulls": (_nulls_dataset, 3)}


@pytest.fixture(scope="module")
def engines():
    from opencleanroom.backends import SparkBackend
    admin = SparkBackend(URL).spark  # trusted fixture path: data goes in here, never through a session connection
    out = {}
    for name, (build, k) in DATASETS.items():
        duck = build()
        database = f"ocr_eq_{name}"
        admin.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
        for table, types in SPARK_TYPES.items():
            rows = [tuple(r) for r in duck.execute(f"SELECT * FROM {table}").fetchall()]
            admin.createDataFrame(rows, schema=types).write.mode("overwrite").saveAsTable(f"{database}.{table}")
        out[name] = (duck, SparkBackend(f"{URL}/;database={database}"), k)
    yield out
    admin.stop()
    for _, spark, _ in out.values():
        spark.spark.stop()


def _vectors(con, dialect, k, sql, keyset):
    rq = Rewriter(SCHEMA, k=k, dialect=dialect).rewrite(sql)
    return Session(con=con, schema=SCHEMA, epsilon=1.0, k=k, dialect=dialect)._true_vectors(rq, keyset)


@pytest.mark.parametrize("dataset", list(DATASETS))
def test_duckdb_and_spark_agree_on_the_corpus(engines, dataset):
    duck, spark, k = engines[dataset]
    disagreements = {}
    for shape, (sql, keyset) in SHAPES.items():
        oracle = _vectors(duck, "duckdb", k, sql, keyset)
        got = _vectors(spark.session_con(), "spark", k, sql, keyset)
        if got != oracle:
            disagreements[shape] = (oracle, got)
    assert disagreements == {}


WIDE_POLICY = """
version: 1
dataset:
  name: wide
  privacy_unit: {column: unit}
  truncation: {k: 2}
  tables:
    big:
      primary_key: id
      columns:
        id: {type: int}
        unit: {type: string}
        v: {type: int, clamp: [0, 1099511627775]}
  budget: {epsilon: "1.0"}
consumers:
  c: {tables: [big], aggregates: [count, sum], max_epsilon_per_query: 0.5}
"""


def test_sums_beyond_2_pow_53_are_exact_on_both_engines(engines):
    from opencleanroom.backends import SparkBackend
    policy = loads_policy(WIDE_POLICY)
    rows = [(i, f"u{i % 10_000}", 2**40 - 1) for i in range(20_000)]  # true sum about 2^54.2
    duck = duckdb.connect()
    duck.execute("CREATE TABLE big(id INTEGER, unit VARCHAR, v BIGINT)")
    duck.executemany("INSERT INTO big VALUES (?, ?, ?)", rows)
    admin = SparkBackend(URL).spark
    admin.sql("CREATE DATABASE IF NOT EXISTS ocr_eq_wide").collect()
    admin.createDataFrame(rows, schema="id INT, unit STRING, v BIGINT").write.mode("overwrite").saveAsTable("ocr_eq_wide.big")
    spark = SparkBackend(f"{URL}/;database=ocr_eq_wide")
    sql = "SELECT COUNT(*), SUM(v) FROM big"
    oracle = Session.from_policy(policy, "c", duck)
    on_spark = Session.from_policy(policy, "c", spark.session_con())
    expected = (None, [[20_000], [20_000 * (2**40 - 1)]])
    for session, dialect in ((oracle, "duckdb"), (on_spark, "spark")):
        rq = Rewriter(session.schema, session.k, dialect).rewrite(sql)
        keyset, vectors = session._true_vectors(rq, None)
        assert vectors == expected[1], dialect
