# SPDX-License-Identifier: Apache-2.0
"""The backend guards are allow-lists, and everything the rewriter emits passes them.

The goldens are every rewritten query shape in each dialect; if the rewriter starts emitting a new node kind,
this test fails before a consumer query does.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from cleanroom_sql import Rewriter, Schema, UnsupportedQuery, loads_policy
from cleanroom_sql.sqlast import distinct_values
from opencleanroom.backends import check_duckdb_select, check_spark_select

GOLDEN = Path(__file__).resolve().parents[2] / "cleanroom-sql" / "tests" / "golden"
TAXI = loads_policy((GOLDEN.parents[1] / "examples" / "taxi.yaml").read_text())
PUBLIC = Schema(tables={n: t for n, t in TAXI.dataset.schema.tables.items() if t.is_public})
PUBLIC_QUERIES = ["SELECT DISTINCT borough FROM taxi_zones", "SELECT zone FROM taxi_zones WHERE location_id BETWEEN 1 AND 9 LIMIT 5",
                  "SELECT a.zone, b.zone FROM taxi_zones a JOIN taxi_zones b ON a.borough = b.borough"]


def _rewriter_output(dialect: str) -> list[str]:
    out = [p.read_text() for p in sorted(GOLDEN.glob(f"*.{dialect}.sql"))]
    out += [Rewriter(PUBLIC, k=50, dialect=dialect).check_public(q) for q in PUBLIC_QUERIES]
    out.append(distinct_values("taxi_zones", "borough").sql(dialect=dialect))
    assert len(out) > 10
    return out


def test_everything_the_rewriter_emits_passes_the_duckdb_guard():
    con = duckdb.connect()
    for sql in _rewriter_output("duckdb"):
        check_duckdb_select(sql, con)


def test_everything_the_rewriter_emits_passes_the_spark_guard():
    for sql in _rewriter_output("spark"):
        check_spark_select(sql)


SPARK_RCE_AND_LEAKS = [
    "SELECT TRANSFORM(a) USING 'sh -c \"id\"' AS (b) FROM t",
    "SELECT TRANSFORM(a) USING 'cat' AS (b) FROM (SELECT 1 AS a) s",
    "SELECT input_file_name() FROM trips",
    "SELECT current_user()",
    "SELECT COUNT(*) FROM trips WHERE hack_license = current_user()",
    "SELECT uuid() FROM trips",
    "SELECT COUNT(*) FROM trips t JOIN (SELECT reflect('java.lang.System', 'getenv', 'HOME') AS x) r ON t.hack_license = r.x",
    "WITH a AS (SELECT * FROM trips) SELECT COUNT(*) FROM a",
    "SELECT borough FROM taxi_zones UNION ALL SELECT borough FROM taxi_zones",
    "SELECT * FROM trips TABLESAMPLE (10 PERCENT)",
    "SELECT * FROM trips LATERAL VIEW explode(array(1)) e AS x",
    "SELECT /*+ BROADCAST(trips) */ COUNT(*) FROM trips",
    "SELECT CAST(fare_amount AS STRING) FROM trips",
    "SELECT COUNT(*) FROM trips WHERE hack_license = user",
    "SELECT session_user",
    "SELECT current_date",
]
DUCKDB_LEAKS = [
    "SELECT current_setting('enable_external_access')", "SELECT getenv('HOME')", "SELECT version()",
    "SELECT COUNT(*) FROM trips WHERE hack_license = current_user", "SELECT * FROM main.trips", "SELECT * FROM 'x.csv'",
    "WITH a AS (SELECT * FROM trips) SELECT COUNT(*) FROM a", "SELECT borough FROM taxi_zones UNION ALL SELECT borough FROM taxi_zones",
    "SELECT md5(hack_license) FROM trips", "SELECT CAST(fare_amount AS VARCHAR) FROM trips",
    "SELECT current_catalog", "SELECT COUNT(*) FROM trips WHERE hack_license = user", "SELECT current_schema",
]


@pytest.mark.parametrize("sql", SPARK_RCE_AND_LEAKS)
def test_spark_guard_refuses_script_transforms_and_server_state(sql):
    with pytest.raises(UnsupportedQuery):
        check_spark_select(sql)


@pytest.mark.parametrize("sql", DUCKDB_LEAKS)
def test_duckdb_guard_refuses_functions_and_shapes_the_rewriter_never_emits(sql):
    with pytest.raises(UnsupportedQuery):
        check_duckdb_select(sql, duckdb.connect())
