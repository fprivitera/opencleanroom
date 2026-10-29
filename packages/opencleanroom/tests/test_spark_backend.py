# SPDX-License-Identifier: Apache-2.0
"""Spark Connect backend: the select-only session connection, and queries on a real server.

The server tests run when OCR_SPARK_URL points at a Spark Connect server
(`scripts/spark-connect.sh up`); without it they are skipped, never faked.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import duckdb
import pytest
import yaml
from cleanroom_sql import PolicyError, Rewriter, Session, UnsupportedQuery, loads_policy
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from opencleanroom.backends import SparkSelectOnly
from opencleanroom.runtime import Runtime

URL = os.environ.get("OCR_SPARK_URL")
needs_spark = pytest.mark.skipif(not URL, reason="needs a Spark Connect server: scripts/spark-connect.sh up")
TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()

ALLOWED = [
    "SELECT COUNT(*) FROM trips",
    "SELECT COUNT(*) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY hack_license ORDER BY trip_id) AS _rn FROM trips) AS t WHERE _rn <= 50",
]
REFUSED = [
    "SET spark.sql.ansi.enabled=false", "ADD JAR /tmp/x.jar", "CREATE TEMPORARY FUNCTION f AS 'x.Y'", "INSERT INTO trips SELECT * FROM trips",
    "REFRESH TABLE trips", "CACHE TABLE trips", "SHOW TABLES", "DESCRIBE trips", "SELECT 1; SELECT 2", "",
    "SELECT * FROM parquet.`/etc`", "SELECT * FROM other_db.trips", "SELECT * FROM range(10)", "SELECT * FROM read_files('/etc/hosts')",
    "SELECT reflect('java.lang.System', 'getenv')", "SELECT java_method('java.lang.Math', 'abs', 1)", "SELECT try_reflect('a', 'b')",
    "DROP TABLE trips", "SELECT FROM WHERE",
    "WITH a AS (SELECT trip_id FROM trips) SELECT COUNT(*) FROM a",  # allow-list: the rewriter emits neither
    "SELECT borough FROM taxi_zones UNION ALL SELECT borough FROM taxi_zones",
]


class _RecordingSpark:
    """Stands in for a SparkSession: records what would reach the server."""

    def __init__(self):
        self.ran: list[str] = []

    def sql(self, sql):
        self.ran.append(sql)
        return self

    def collect(self):
        return [(1, "a")]

    def addTag(self, tag):  # the SparkSession method names
        pass

    def removeTag(self, tag):
        pass

    def interruptTag(self, tag):
        return []


def test_spark_session_connection_runs_only_a_single_select_over_bare_tables():
    spark = _RecordingSpark()
    con = SparkSelectOnly(spark, 30)
    for sql in ALLOWED:
        assert con.execute(sql).fetchall() == [(1, "a")]
    for sql in REFUSED:
        with pytest.raises(UnsupportedQuery):
            con.execute(sql)
    assert spark.ran == ALLOWED  # a refused statement never reaches the server


def test_catalog_registers_a_spark_backend(tmp_path):
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-dev", "spark", "sc://127.0.0.1:15002/;database=nyc")
    assert [(b.kind, b.location) for b in catalog.list_backends(cat)] == [("spark", "sc://127.0.0.1:15002/;database=nyc")]
    for bad in ("http://x:15002", "/data/file.duckdb", "sc://"):
        with pytest.raises(ValueError):
            catalog.add_backend(cat, "bad", "spark", bad)


# -- on a real server ------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark_env(tmp_path_factory):
    from opencleanroom.backends import SparkBackend
    duck = duckdb.connect()
    populate(duck, n_drivers=100)
    backend = SparkBackend(URL)
    for name in ("taxi_zones", "drivers", "trips"):  # trusted fixture path, never the session connection
        backend.spark.createDataFrame(duck.sql(f"SELECT * FROM {name}").df()).write.mode("overwrite").saveAsTable(name)  # pandas: both client lines take it
    cat = tmp_path_factory.mktemp("spark") / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-dev", "spark", URL + "/;database=default")
    doc = yaml.safe_load(TAXI_YAML)
    doc["dataset"]["backend"], doc["dataset"]["dialect"] = "spark-dev", "spark"
    text = yaml.safe_dump(doc, sort_keys=False)
    catalog.apply_policy(cat, text)
    yield cat, duck, backend, loads_policy(text)
    backend.spark.stop()  # an open gRPC channel breaks later subprocess tests


def _true(con, policy, sql):
    session = Session.from_policy(policy, "analytics-team", con)
    rq = Rewriter(session.schema, session.k, session.dialect).rewrite(sql)
    return session._true_vectors(rq, None)[1]


@needs_spark
def test_count_through_runtime_on_spark_matches_duckdb(spark_env):
    cat, duck, backend, policy = spark_env
    key = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    res = Runtime(cat).query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert len(res.rows) == 1 and "ROW_NUMBER" in res.rewritten_sql.upper()
    duck_policy = loads_policy(TAXI_YAML)
    assert _true(backend.session_con(), policy, "SELECT COUNT(*) FROM trips") == _true(duck, duck_policy, "SELECT COUNT(*) FROM trips")


@needs_spark
def test_spark_session_connection_refuses_on_a_real_server(spark_env):
    _, _, backend, _ = spark_env
    for sql in ("SET spark.sql.ansi.enabled=false", "SHOW TABLES", "SELECT * FROM parquet.`/etc`"):
        with pytest.raises(UnsupportedQuery):
            backend.session_con().execute(sql)
    assert backend.session_con().execute("SELECT COUNT(*) FROM taxi_zones").fetchall() == [(263,)]


def test_policy_dialect_must_match_the_backend(tmp_path):
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-dev", "spark", "sc://127.0.0.1:15002")
    doc = yaml.safe_load(TAXI_YAML)
    doc["dataset"]["backend"] = "spark-dev"  # dialect left at its duckdb default
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, yaml.safe_dump(doc, sort_keys=False))
    assert [p for p, _ in e.value.errors] == ["dataset.dialect"]


def test_runtime_refuses_a_dialect_mismatch_from_a_hand_edited_catalog(tmp_path):
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=20)
    con.close()
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "duckdb-dev", "duckdb", str(db))
    catalog.apply_policy(cat, TAXI_YAML)
    key = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    with sqlite3.connect(cat) as c:
        c.execute("UPDATE backends SET kind = 'spark', location = 'sc://127.0.0.1:1' WHERE name = 'duckdb-dev'")

    class _FakeSpark:
        dialect = "spark"
    rt = Runtime(cat)
    rt._backends["duckdb-dev"] = _FakeSpark()
    with pytest.raises(RuntimeError):
        rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)


def test_backend_list_hides_a_spark_token(tmp_path):
    assert catalog.redact_location("sc://h:15002/;token=s3cret;use_ssl=true") == "sc://h:15002/;token=***;use_ssl=true"
    assert catalog.redact_location("/data/taxi.duckdb") == "/data/taxi.duckdb"


@needs_spark
def test_a_stopped_spark_session_is_replaced(spark_env):
    _, _, backend, _ = spark_env
    from opencleanroom.backends import SparkBackend
    fresh = SparkBackend(URL)
    fresh.spark.stop()  # what a server restart or an idle timeout leaves behind
    assert fresh.session_con().execute("SELECT COUNT(*) FROM taxi_zones").fetchall() == [(263,)]
    assert not fresh.spark.is_stopped
