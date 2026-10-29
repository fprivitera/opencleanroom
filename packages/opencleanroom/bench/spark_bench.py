# SPDX-License-Identifier: Apache-2.0
"""Benchmark: truncation overhead and latency on Spark, with the window and with materialized tables.

    python bench/spark_bench.py --url sc://127.0.0.1:15002 [--trips 5000000] [--drivers 50000] [--reps 5]

Builds a heavy-tailed synthetic taxi dataset inside Spark (a few drivers own most trips, so truncation at k = 50
bites), materializes it through `opencleanroom.materialize`, then times every library query shape three ways:
the consumer's SQL as a plain aggregate, the rewritten SQL with the truncation window, and the rewritten SQL on the
materialized table. Each run is the backend round trip (execute and collect); one warm-up run per query is dropped.
It also times `Runtime.query` end to end (rewrite, ledger, backend, noise). Writes Markdown to stdout or `--out`.
"""
from __future__ import annotations

import argparse
import importlib.util
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

import yaml
from cleanroom_sql import Rewriter, loads_policy
from opencleanroom import catalog
from opencleanroom.backends import SparkBackend
from opencleanroom.materialize import materialize
from opencleanroom.runtime import Runtime

HERE = Path(__file__).resolve().parent
TAXI_YAML = HERE.parents[1] / "cleanroom-sql" / "examples" / "taxi.yaml"
_spec = importlib.util.spec_from_file_location("shapes", HERE.parents[1] / "cleanroom-sql" / "tests" / "test_sensitivity_property.py")
_shapes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_shapes)
DB = "ocr_bench"


def build_data(spark, trips: int, drivers: int) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}").collect()
    for t in spark.sql(f"SHOW TABLES IN {DB}").collect():
        spark.sql(f"DROP TABLE {DB}.{t.tableName}").collect()
    boroughs = "array('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island', 'EWR')"
    spark.sql(f"""CREATE TABLE {DB}.taxi_zones USING parquet AS SELECT CAST(id + 1 AS INT) AS location_id,
                  element_at({boroughs}, CAST(id % 6 + 1 AS INT)) AS borough, concat('zone_', id + 1) AS zone FROM range(263)""").collect()
    spark.sql(f"""CREATE TABLE {DB}.drivers USING parquet AS SELECT concat('HL', id) AS hack_license,
                  element_at(array('medallion', 'fleet_A', 'fleet_B'), CAST(id % 3 + 1 AS INT)) AS fleet FROM range({drivers})""").collect()
    # u^3 over [0, 1) puts most trips on the first drivers: a heavy tail like the real fleet
    spark.sql(f"""CREATE TABLE {DB}.trips USING parquet AS SELECT CAST(id AS INT) AS trip_id,
                  concat('HL', CAST(floor({drivers} * pow(pmod(hash(id), 1000003) / 1000003.0, 3)) AS BIGINT)) AS hack_license,
                  CAST(pmod(hash(id, 3), 263) + 1 AS INT) AS pu_location_id, CAST(pmod(hash(id, 1), 25000) AS INT) AS fare_amount,
                  CAST(pmod(hash(id, 2), 600) AS INT) AS trip_distance FROM range({trips})""").collect()


def timed(run, sql: str, reps: int) -> list[float]:
    run(sql)  # warm-up, dropped
    out = []
    for _ in range(reps):
        t0 = time.perf_counter()
        run(sql)
        out.append(time.perf_counter() - t0)
    return out


def p(values: list[float], q: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, round(q * (len(s) - 1)))]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--url", required=True)
    ap.add_argument("--trips", type=int, default=5_000_000)
    ap.add_argument("--drivers", type=int, default=50_000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--note", action="append", default=[], help="a line for the Setup section: data you could not use, the machine")
    a = ap.parse_args()

    admin = SparkBackend(a.url)
    started = time.perf_counter()
    build_data(admin.spark, a.trips, a.drivers)
    built = time.perf_counter() - started
    top = admin.admin_sql(f"SELECT MAX(c), PERCENTILE(c, 0.5) FROM (SELECT COUNT(*) AS c FROM {DB}.trips GROUP BY hack_license)")[0]

    cat = Path(tempfile.mkdtemp()) / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "spark-bench", "spark", f"{a.url}/;database={DB}", query_timeout=600)
    doc = yaml.safe_load(TAXI_YAML.read_text())
    doc["dataset"]["backend"], doc["dataset"]["dialect"] = "spark-bench", "spark"
    doc["dataset"]["budget"]["epsilon"] = doc["consumers"]["analytics-team"]["budget"]["epsilon"] = "1000"
    catalog.apply_policy(cat, yaml.safe_dump(doc, sort_keys=False))
    policy = loads_policy(catalog.get_policy(cat, "nyc_taxi").text)
    started = time.perf_counter()
    materialize(cat, "nyc_taxi", "bench")
    materialized_in = time.perf_counter() - started
    rt = Runtime(cat)
    used = rt._materialized("nyc_taxi", policy, catalog.get_policy(cat, "nyc_taxi").epoch)
    session = rt._backend("spark-bench")
    counted = {"trips": "trips", **{f"{t} materialized": m for t, m in used.items()}}
    sizes = {name: session.admin_sql(f"SELECT COUNT(*) FROM {table}")[0][0] for name, table in counted.items()}

    def run(sql):
        return session.session_con().execute(sql).fetchall()

    k, rows = policy.dataset.k, []
    for shape, (sql, _) in _shapes.SHAPES.items():
        window = Rewriter(policy.dataset.schema, k, "spark").rewrite(sql).sql
        stored = Rewriter(policy.dataset.schema, k, "spark", used).rewrite(sql).sql
        plain = timed(session.admin_sql, sql, a.reps)  # the consumer's SQL as written, no truncation, in the dataset's database
        w, m = timed(run, window, a.reps), timed(run, stored, a.reps)
        truncates = "ROW_NUMBER" in window.upper()  # shapes without a truncated table have nothing to materialize
        rows.append((shape, plain, w, m, truncates, any(name in stored for name in used.values())))

    key = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    e2e = timed(lambda s: rt.query(key, s, epsilon=0.1), "SELECT COUNT(*), SUM(fare_amount) FROM trips", max(a.reps, 10))

    commit = subprocess.run(["git", "describe", "--always", "--dirty"], check=False, capture_output=True, text=True, cwd=HERE).stdout.strip()
    lines = [
        "# Benchmark: truncation on Spark",
        "",
        f"Generated by `packages/opencleanroom/bench/spark_bench.py` at commit `{commit}`.",
        "",
        "## Setup",
        "",
        f"- Data: synthetic, heavy-tailed taxi dataset built inside Spark: {a.trips:,} trips, {a.drivers:,} drivers, 263 zones. "
        f"The busiest driver has {int(top[0]):,} trips, the median driver {int(top[1]):,}; truncation keeps k = {k}. "
        + ", ".join(f"{n}: {c:,} rows" for n, c in sizes.items()) + ".",
        f"- Engine: Spark {admin.spark.version} behind Spark Connect; client on {platform.machine()} ({platform.system()} {platform.release()}).",
        *[f"- {note}" for note in a.note],
        f"- Each figure is {a.reps} timed runs after one warm-up, wall clock of the backend round trip (execute and collect); "
        f"with {a.reps} runs, p95 is the slowest run. Data built in {built:.0f} s; materialization took {materialized_in:.0f} s.",
        "- A ratio below 1x does not mean truncation is free: the materialized table holds at most k rows per privacy unit, "
        "so it is smaller than the table the plain aggregate scans. The plain aggregate is the consumer's SQL as written, "
        "with no truncation, clamp or noise.",
        "",
        "## Per query (seconds; overhead is the p50 ratio against the plain aggregate)",
        "",
        "| Query shape | Plain p50 | Window p50 / p95 | Overhead | Materialized p50 / p95 | Overhead | Materialized table read |",
        "|---|---|---|---|---|---|---|",
    ]
    for shape, plain, w, m, truncates, stored in rows:
        base = statistics.median(plain)
        lines.append(f"| {shape} | {base:.2f} | {statistics.median(w):.2f} / {p(w, .95):.2f} | {statistics.median(w) / base:.1f}x | "
                     f"{statistics.median(m):.2f} / {p(m, .95):.2f} | {statistics.median(m) / base:.1f}x | "
                     f"{'yes' if stored else ('no: WHERE on trips keeps the window' if truncates else 'no truncated table')} |")
    ratios_w = [statistics.median(w) / statistics.median(pl) for _, pl, w, _, t, _ in rows if t]
    ratios_m = [statistics.median(m) / statistics.median(pl) for _, pl, _, m, t, s in rows if t and s]
    ratios_f = [statistics.median(m) / statistics.median(pl) for _, pl, _, m, t, s in rows if t and not s]
    met = max(ratios_m) <= 3
    lines += [
        "",
        "## Summary",
        "",
        f"- Window, on the {len(ratios_w)} shapes with a truncated table: median overhead {statistics.median(ratios_w):.1f}x, worst {max(ratios_w):.1f}x.",
        f"- Materialized, on the {len(ratios_m)} shapes that read the materialized table: median {statistics.median(ratios_m):.1f}x, "
        f"worst {max(ratios_m):.1f}x. Target of at most 3x: {'met' if met else 'missed'}.",
        f"- The {len(ratios_f)} shapes with a filter on `trips` keep the window: median {statistics.median(ratios_f):.1f}x, "
        f"worst {max(ratios_f):.1f}x. Target {'met' if ratios_f and max(ratios_f) <= 3 else 'missed'} for them. Filtering the materialized "
        "table after truncation would serve them too, at the cost of answers that differ from the window's; that choice is open.",
        f"- `Runtime.query` end to end (rewrite, ledger, backend, noise), `SELECT COUNT(*), SUM(fare_amount) FROM trips`, "
        f"{max(a.reps, 10)} runs: p50 {statistics.median(e2e):.2f} s, p95 {p(e2e, .95):.2f} s.",
        "",
    ]
    text = "\n".join(lines)
    if a.out:
        a.out.write_text(text)
    print(text)


if __name__ == "__main__":
    main()
