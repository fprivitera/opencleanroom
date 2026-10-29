# SPDX-License-Identifier: Apache-2.0
"""Materialized tables on Spark (declared types and keys checked first); the runtime uses one only when it matches the policy."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import duckdb
import pytest
import yaml
from cleanroom_sql import Session, loads_policy
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from opencleanroom.materialize import materialize, physical_name
from opencleanroom.runtime import Runtime

URL = os.environ.get("OCR_SPARK_URL")
needs_spark = pytest.mark.skipif(not URL, reason="needs a Spark Connect server: scripts/spark-connect.sh up")
TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()
TYPES = {"taxi_zones": "location_id INT, borough STRING, zone STRING", "drivers": "hack_license STRING, fleet STRING",
         "trips": "trip_id INT, hack_license STRING, pu_location_id INT, fare_amount INT, trip_distance INT"}


def _policy(**changes) -> str:
    doc = yaml.safe_load(TAXI_YAML)
    doc["dataset"]["backend"], doc["dataset"]["dialect"] = "spark-mat", "spark"
    for dotted, value in changes.items():
        node = doc
        *parents, last = dotted.split(".")
        for p in parents:
            node = node[p]
        node[last] = value
    return yaml.safe_dump(doc, sort_keys=False)


def test_physical_names_are_bare_identifiers():
    name = physical_name("trips", 50, "2026-10", "2026-10/snap 1")
    assert name.startswith("trips__ocr_k50_2026_10_snap_1_") and name.replace("_", "a").isalnum()


def test_runtime_picks_only_records_matching_epoch_k_order_and_backend(tmp_path):
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-mat", "spark", "sc://127.0.0.1:1")
    catalog.apply_policy(cat, _policy())
    epoch = catalog.get_policy(cat, "nyc_taxi").epoch
    from opencleanroom.materialize import definition_sha256
    policy = loads_policy(catalog.get_policy(cat, "nyc_taxi").text)
    name = physical_name("trips", 50, epoch, "s1")
    catalog.record_materialization(cat, "nyc_taxi", "trips", "spark-mat", epoch, "s1", 50, "trip_id", "hack_license", name,
                                   definition_sha256(policy.dataset.schema.tables["trips"], 50))
    rt = Runtime(cat)
    assert rt._materialized("nyc_taxi", policy, epoch) == {"trips": name}
    assert rt._materialized("nyc_taxi", policy, "2099-01") == {}
    assert rt._materialized("nyc_taxi", loads_policy(_policy(**{"dataset.truncation.k": 40})), epoch) == {}
    with pytest.raises(ValueError):  # one materialization per table, epoch and k
        catalog.record_materialization(cat, "nyc_taxi", "trips", "spark-mat", epoch, "s2", 50, "trip_id", "hack_license",
                                       physical_name("trips", 50, epoch, "s2"), "")


def test_materialize_needs_a_spark_backend(tmp_path):
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=20)
    con.close()
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "duckdb-dev", "duckdb", str(db))
    catalog.apply_policy(cat, TAXI_YAML)
    with pytest.raises(ValueError, match="Spark"):
        materialize(cat, "nyc_taxi", "s1")


# -- on a real server ------------------------------------------------------------------

def _load(admin, database, duck, duplicate_zone=False):
    admin.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
    for table in admin.sql(f"SHOW TABLES IN {database}").collect():
        admin.sql(f"DROP TABLE IF EXISTS {database}.{table.tableName}").collect()
    for table, types in TYPES.items():
        rows = [tuple(r) for r in duck.execute(f"SELECT * FROM {table}").fetchall()]
        if duplicate_zone and table == "taxi_zones":
            rows.append(rows[0])
        admin.createDataFrame(rows, schema=types).write.mode("overwrite").saveAsTable(f"{database}.{table}")


@pytest.fixture
def spark_cat(tmp_path):
    from opencleanroom.backends import SparkBackend
    admin = SparkBackend(URL)
    duck = duckdb.connect()
    populate(duck, n_drivers=120)
    _load(admin.spark, "ocr_mat", duck)
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-mat", "spark", f"{URL}/;database=ocr_mat")
    catalog.apply_policy(cat, _policy())
    yield cat, admin
    admin.spark.stop()


_spec = importlib.util.spec_from_file_location("cleanroom_sql_shapes", Path(__file__).resolve().parents[2] / "cleanroom-sql" / "tests" / "test_sensitivity_property.py")
_shapes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_shapes)


@needs_spark
def test_materialized_truncation_matches_the_window(spark_cat):
    cat, admin = spark_cat
    created = materialize(cat, "nyc_taxi", "s1")
    epoch = catalog.get_policy(cat, "nyc_taxi").epoch
    assert created == [physical_name("trips", 50, epoch, "s1")]
    rt = Runtime(cat)
    policy = loads_policy(catalog.get_policy(cat, "nyc_taxi").text)
    used = rt._materialized("nyc_taxi", policy, epoch)
    assert used == {"trips": created[0]}
    con = rt._backend("spark-mat").session_con()
    differences = {}
    for shape, (sql, keyset) in _shapes.SHAPES.items():
        window = Session.from_policy(policy, "analytics-team", con)
        table = Session.from_policy(policy, "analytics-team", con, materialized=used)
        from cleanroom_sql import Rewriter
        a = window._true_vectors(Rewriter(window.schema, window.k, "spark").rewrite(sql), keyset)
        b = table._true_vectors(Rewriter(table.schema, table.k, "spark", used).rewrite(sql), keyset)
        if a != b:
            differences[shape] = (a, b)
    assert differences == {}
    key = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    res = rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert created[0] in res.rewritten_sql and "ROW_NUMBER" not in res.rewritten_sql.upper()
    with pytest.raises(ValueError):  # the same epoch and k again
        materialize(cat, "nyc_taxi", "s1")


@needs_spark
def test_stale_or_foreign_tables_are_never_read(spark_cat):
    cat, admin = spark_cat
    materialize(cat, "nyc_taxi", "s1")
    admin.spark.sql("CREATE TABLE ocr_mat.trips__ocr_k40_s1 USING parquet AS SELECT * FROM ocr_mat.trips").collect()  # hand-made, no record
    key = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    catalog.apply_policy(cat, _policy(**{"dataset.truncation.k": 40}))  # another k in the same epoch
    res = Runtime(cat).query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert "ROW_NUMBER" in res.rewritten_sql.upper() and "__ocr_" not in res.rewritten_sql
    catalog.new_epoch(cat, "nyc_taxi", "2026-11", "s2", "renewal")
    catalog.apply_policy(cat, _policy(**{"dataset.budget.epoch": "2026-11"}))  # k back to 50, but a new epoch
    res = Runtime(cat).query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert "__ocr_" not in res.rewritten_sql
    with pytest.raises(ValueError, match="snapshot"):  # the epoch records snapshot s2
        materialize(cat, "nyc_taxi", "s1")


@needs_spark
def test_materialize_refuses_a_non_unique_primary_key(tmp_path):
    from opencleanroom.backends import SparkBackend
    admin = SparkBackend(URL)
    duck = duckdb.connect()
    populate(duck, n_drivers=20)
    _load(admin.spark, "ocr_mat_dup", duck, duplicate_zone=True)
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-mat", "spark", f"{URL}/;database=ocr_mat_dup")
    catalog.apply_policy(cat, _policy())
    with pytest.raises(ValueError, match="taxi_zones"):
        materialize(cat, "nyc_taxi", "s1")
    assert catalog.materializations(cat, "nyc_taxi") == []
    assert not [t for t in admin.spark.sql("SHOW TABLES IN ocr_mat_dup").collect() if "__ocr_" in t.tableName]
    admin.spark.stop()


# -- review findings: types, names, record trust -------------------------------------------

def test_a_key_stored_as_text_is_refused_before_counting(tmp_path):
    from opencleanroom.backends import DuckDBBackend
    from opencleanroom.materialize import check_primary_keys
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=20)
    con.execute("CREATE OR REPLACE TABLE taxi_zones AS SELECT CAST(location_id AS VARCHAR) AS location_id, borough, zone FROM taxi_zones")
    con.execute("INSERT INTO taxi_zones VALUES ('07', 'Queens', 'zone_07')")  # distinct as text, equal to 7 after a join's cast
    con.close()
    with pytest.raises(ValueError, match=r"taxi_zones\.location_id is varchar, declared int"):
        check_primary_keys(DuckDBBackend(str(db)).admin_sql, loads_policy(TAXI_YAML))


def test_physical_names_never_collide_on_lookalike_snapshots_or_epochs():
    names = {physical_name("trips", 50, e, s) for e in ("2026-10", "2026_10") for s in ("s-1", "s_1")}
    assert len(names) == 4 and all(n.replace("_", "a").isalnum() for n in names)


def test_runtime_ignores_edited_and_outdated_records(tmp_path):
    from opencleanroom.materialize import definition_sha256
    policy = loads_policy(_policy())
    trips = policy.dataset.schema.tables["trips"]
    good_sha = definition_sha256(trips, 50)
    widened = loads_policy(_policy(**{"dataset.tables.trips.columns.tip_amount": {"type": "int", "clamp": [0, 100]}}))
    cases = {
        "edited name": ("trips", good_sha, {}),
        "older column list": (physical_name("trips", 50, "2026-10", "s1"), definition_sha256(widened.dataset.schema.tables["trips"], 50), {}),
        "as created": (physical_name("trips", 50, "2026-10", "s1"), good_sha, {"trips": physical_name("trips", 50, "2026-10", "s1")}),
    }
    for label, (name, sha, expected) in cases.items():
        cat = tmp_path / f"{label.replace(' ', '_')}.sqlite"
        catalog.init(cat)
        catalog.add_backend(cat, "spark-mat", "spark", "sc://127.0.0.1:1")
        catalog.apply_policy(cat, _policy())
        catalog.record_materialization(cat, "nyc_taxi", "trips", "spark-mat", "2026-10", "s1", 50, "trip_id", "hack_license", name, sha)
        assert Runtime(cat)._materialized("nyc_taxi", policy, "2026-10") == expected, label


@needs_spark
def test_materialize_refuses_a_key_stored_as_text_on_spark(tmp_path):
    from opencleanroom.backends import SparkBackend
    admin = SparkBackend(URL)
    duck = duckdb.connect()
    populate(duck, n_drivers=20)
    duck.execute("CREATE OR REPLACE TABLE taxi_zones AS SELECT CAST(location_id AS VARCHAR) AS location_id, borough, zone FROM taxi_zones")
    duck.execute("INSERT INTO taxi_zones VALUES ('07', 'Queens', 'zone_07')")
    saved = dict(TYPES)
    TYPES["taxi_zones"] = "location_id STRING, borough STRING, zone STRING"
    try:
        _load(admin.spark, "ocr_mat_types", duck)
    finally:
        TYPES.update(saved)
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-mat", "spark", f"{URL}/;database=ocr_mat_types")
    catalog.apply_policy(cat, _policy())
    with pytest.raises(ValueError, match=r"taxi_zones\.location_id is string, declared int"):
        materialize(cat, "nyc_taxi", "s1")
    assert catalog.materializations(cat, "nyc_taxi") == []
    admin.spark.stop()
