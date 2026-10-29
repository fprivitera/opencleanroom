# SPDX-License-Identifier: Apache-2.0
"""Adversarial probes against the platform: catalog, runtime, keys, backend. Exits non-zero on FAIL or ??.

Statuses: PASS (blocked when it should be, or works when it should), FAIL,
?? (unexpected exception type), KNOWN <limit> (one of the known limits in
SECURITY.md; a KNOWN probe whose behaviour changes prints FAIL so the list
gets updated). Runs on a temp catalog and a temp DuckDB file.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import threading
from fractions import Fraction
from pathlib import Path

import duckdb
import yaml
from cleanroom_sql import BudgetExhaustedException, PolicyError, PolicyViolation, QueryFailed, UnsupportedQuery
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from opencleanroom.backends import DuckDBBackend
from opencleanroom.runtime import AuthError, Runtime

HERE = Path(__file__).resolve().parent
TAXI_YAML = (HERE.parents[0] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()
SINGLE_YAML = (HERE.parents[0] / "cleanroom-sql" / "examples" / "single_table.yaml").read_text()

BLOCKED = (UnsupportedQuery, PolicyViolation, PolicyError, AuthError, BudgetExhaustedException, QueryFailed,
           ValueError, RuntimeError, duckdb.Error, sqlite3.Error)
RESULTS: list[str] = []


def probe(name, fn, expect_block=True, known=None):
    try:
        out = fn()
        if known:
            status = f"KNOWN {known}" if not expect_block else f"KNOWN {known} (accepted)"
        else:
            status = "FAIL (accepted)" if expect_block else (f"PASS -> {out}" if out else f"FAIL -> {out}")
    except BLOCKED as e:
        if known and expect_block:
            status = f"FAIL (the KNOWN hole closed: {type(e).__name__}; update SECURITY.md: {known})"
        else:
            status = f"PASS (blocked: {type(e).__name__}: {str(e)[:60]})" if expect_block else f"FAIL (blocked: {e})"
    except Exception as e:
        status = f"?? {type(e).__name__}: {str(e)[:80]}"
    RESULTS.append(status)
    print(f"{name:58} {status}")


def _policy(text: str, **changes) -> str:
    doc = yaml.safe_load(text)
    for dotted, value in changes.items():
        node = doc
        *parents, last = dotted.split(".")
        for p in parents:
            node = node[p]
        if value is None:
            node.pop(last)
        else:
            node[last] = value
    return yaml.safe_dump(doc, sort_keys=False)


# -- fixture -----------------------------------------------------------------------
tmp = Path(tempfile.mkdtemp(prefix="ocr-red-"))
taxi_db = tmp / "taxi.duckdb"
con = duckdb.connect(str(taxi_db))
populate(con, n_drivers=100)
con.close()
events_db = tmp / "events.duckdb"
con = duckdb.connect(str(events_db))
con.execute("CREATE TABLE events(event_id INT, user_id VARCHAR, amount INT)")
con.execute("INSERT INTO events VALUES (1, 'u1', 10), (2, 'u2', 20)")
con.close()
cat = tmp / "catalog.sqlite"
catalog.init(cat)
catalog.add_backend(cat, "duckdb-dev", "duckdb", str(taxi_db))
catalog.add_backend(cat, "events-dev", "duckdb", str(events_db))
catalog.apply_policy(cat, TAXI_YAML)
catalog.apply_policy(cat, _policy(SINGLE_YAML, **{"dataset.backend": "events-dev", "dataset.budget.epoch": "2026-10"}))
KEY = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
COUNTS = catalog.create_consumer(cat, "nyc_taxi", "counts-only")
OTHER = catalog.create_consumer(cat, "app_events", "reporting")
rt = Runtime(cat)
Q = "SELECT COUNT(*) FROM trips"

# -- helpers used by the probes ------------------------------------------------------
def _edited_hash():
    k = catalog.create_consumer(cat, "nyc_taxi", "counts-only") if catalog.resolve_key(cat, COUNTS) is None else COUNTS
    c = sqlite3.connect(cat)
    c.execute("UPDATE consumers SET key_sha256 = substr(key_sha256, 2) || substr(key_sha256, 1, 1) WHERE name = 'counts-only' AND revoked_at IS NULL")
    c.commit()
    c.close()
    try:
        return rt.query(k, Q, 0.1)
    finally:
        catalog.revoke_consumer(cat, "nyc_taxi", "counts-only")
        globals()["COUNTS"] = catalog.create_consumer(cat, "nyc_taxi", "counts-only")


def _revoked():
    k = catalog.create_consumer(cat, "nyc_taxi", "counts-only") if catalog.resolve_key(cat, COUNTS) is None else COUNTS
    catalog.revoke_consumer(cat, "nyc_taxi", "counts-only")
    try:
        return rt.query(k, Q, 0.1)
    finally:
        globals()["COUNTS"] = catalog.create_consumer(cat, "nyc_taxi", "counts-only")


def _delete_row():
    k = catalog.create_consumer(cat, "nyc_taxi", "counts-only") if catalog.resolve_key(cat, COUNTS) is None else COUNTS
    c = sqlite3.connect(cat)
    c.execute("DELETE FROM consumers WHERE name = 'counts-only'")
    c.commit()
    c.close()
    try:
        return rt.query(k, Q, 0.1)
    finally:
        globals()["COUNTS"] = catalog.create_consumer(cat, "nyc_taxi", "counts-only")


def _duplicate_hash():
    c = sqlite3.connect(cat)
    try:
        c.execute("INSERT INTO consumers (dataset, name, key_sha256, created_at) SELECT dataset, 'twin', key_sha256, created_at FROM consumers LIMIT 1")
        c.commit()
    finally:
        c.close()


def _edited_text():
    c = sqlite3.connect(cat)
    c.execute("UPDATE policies SET text = replace(text, 'k: 50', 'k: 5000') WHERE dataset = 'nyc_taxi'")
    c.commit()
    c.close()
    try:
        return Runtime(cat).query(KEY, Q, 0.1)
    finally:
        c = sqlite3.connect(cat)
        c.execute("UPDATE policies SET text = replace(text, 'k: 5000', 'k: 50') WHERE dataset = 'nyc_taxi'")
        c.commit()
        c.close()


def _edited_budget_column():
    c = sqlite3.connect(cat)
    c.execute("UPDATE policies SET budget_epsilon = '1000' WHERE dataset = 'nyc_taxi'")
    c.commit()
    c.close()
    try:
        catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.budget.epsilon": "900"}))  # above the file's 10, below the edited 1000
        return "a raise inside the epoch was accepted after the column edit"
    finally:
        c = sqlite3.connect(cat)
        c.execute("UPDATE policies SET budget_epsilon = '10' WHERE dataset = 'nyc_taxi'")
        c.execute("DELETE FROM policy_history WHERE dataset = 'nyc_taxi'")
        c.commit()
        c.close()
        catalog.apply_policy(cat, TAXI_YAML)


def _view_bound():
    """The operator replaces trips with a view that duplicates every row: truncation still caps k rows per driver."""
    db = tmp / "view.duckdb"
    c = duckdb.connect(str(db))
    populate(c, n_drivers=20)
    c.execute("ALTER TABLE trips RENAME TO trips_raw")
    c.execute("CREATE VIEW trips AS SELECT * FROM trips_raw UNION ALL SELECT * FROM trips_raw")
    c.close()
    catalog.add_backend(cat, "view-dev", "duckdb", str(db))
    catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.name": "taxi_view", "dataset.backend": "view-dev"}))
    k = catalog.create_consumer(cat, "taxi_view", "analytics-team")
    res = Runtime(cat).query(k, "SELECT COUNT(*) FROM trips", 0.1)
    return res.sensitivities == (50,)


def _old_epoch_raise():
    catalog.new_epoch(cat, "nyc_taxi", "2026-11", "snap-red", "red team: renewal probe")
    catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.budget.epoch": "2026-11", "dataset.budget.epsilon": "1.0",
                                                   "consumers.analytics-team.budget.epsilon": 1, "consumers.analytics-team.max_epsilon_per_query": 0.5,
                                                   "consumers.counts-only.max_epsilon_per_query": 0.2}))
    try:
        return catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.budget.epsilon": "100.0"}))
    finally:  # the original budgets come back under a recorded, newer epoch: the old label can never return
        catalog.new_epoch(cat, "nyc_taxi", "2026-12", "snap-red", "red team: restore the taxi budgets")
        catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.budget.epoch": "2026-12"}))


def _shared_budget():
    text = _policy(TAXI_YAML, **{"dataset.name": "shared", "dataset.budget.epsilon": "0.5",
                                 "consumers.analytics-team.budget.epsilon": 0.5, "consumers.analytics-team.max_epsilon_per_query": 0.3,
                                 "consumers.counts-only.max_epsilon_per_query": 0.3})
    catalog.apply_policy(cat, text)
    a = catalog.create_consumer(cat, "shared", "analytics-team")
    b = catalog.create_consumer(cat, "shared", "counts-only")
    r = Runtime(cat)
    r.query(a, Q, 0.3)
    try:
        r.query(b, Q, 0.3)
        return False
    except BudgetExhaustedException:
        return r.budgets("shared")["spent"] == Fraction(3, 10)


def _threads_reserve():
    text = _policy(TAXI_YAML, **{"dataset.name": "threads", "dataset.budget.epsilon": "1.0",
                                 "consumers.analytics-team.budget.epsilon": 1, "consumers.analytics-team.max_epsilon_per_query": 0.1,
                                 "consumers.counts-only.max_epsilon_per_query": 0.1})
    catalog.apply_policy(cat, text)
    k = catalog.create_consumer(cat, "threads", "analytics-team")
    r = Runtime(cat)
    ok, barrier = [], threading.Barrier(20)

    def worker():
        barrier.wait()
        try:
            r.query(k, Q, 0.1)
            ok.append(1)
        except BudgetExhaustedException:
            pass
    ts = [threading.Thread(target=worker) for _ in range(20)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return len(ok) == 10 and r.budgets("threads")["spent"] == Fraction(1)


def _two_writers():
    holder = sqlite3.connect(cat, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        try:
            catalog.add_backend(cat, "late", "duckdb", str(taxi_db), timeout=0.2)
            return False
        except sqlite3.OperationalError:
            pass
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    return sqlite3.connect(cat).execute("PRAGMA integrity_check").fetchone()[0] == "ok"


# -- probes ------------------------------------------------------------------------
print("--- keys ---")
probe("empty key", lambda: rt.query("", Q, 0.1))
probe("key with trailing newline", lambda: rt.query(KEY + "\n", Q, 0.1))
probe("key with the prefix stripped", lambda: rt.query(KEY[4:], Q, 0.1))
probe("another dataset's key", lambda: rt.query(OTHER, Q, 0.1))
probe("hash edited in the catalog", lambda: _edited_hash())
probe("revoked key", lambda: _revoked())
probe("valid key works", lambda: len(rt.query(KEY, Q, 0.1).rows), expect_block=False)

print("--- catalog tampering ---")

probe("deleted consumer row", _delete_row)
probe("duplicated key hash", _duplicate_hash)
probe("edited policy text (hash tripwire)", _edited_text)
probe("edited budget_epsilon column raises the ceiling", _edited_budget_column, expect_block=False, known="catalog file trusted")

print("--- DuckDB read-only backend ---")
ESCAPES = {
    "COPY TO": "COPY (SELECT 1) TO '/tmp/ocr-red-escape.csv'",
    "INSTALL": "INSTALL nonexistent_extension_xyz",
    "LOAD": "LOAD nonexistent_extension_xyz",
    "ATTACH": f"ATTACH '{tmp}/other.duckdb' AS other",
    "PRAGMA": "PRAGMA database_list",
    "CREATE TABLE": "CREATE TABLE leak AS SELECT * FROM trips",
    "CALL": "CALL pragma_database_list()",
    "metadata table function": "SELECT * FROM duckdb_databases()",
    "file table function": "SELECT * FROM read_csv('/etc/hosts')",
    "SET": "SET threads = 1",
    "two statements": "SELECT 1; SELECT 2",
    "database name through a pseudo-column": "SELECT current_catalog",
    "scalar function outside the rewriter's set": "SELECT md5(hack_license) FROM trips",
    "CTE": "WITH a AS (SELECT * FROM trips) SELECT COUNT(*) FROM a",
}
for name, sql in ESCAPES.items():
    probe(f"{name} through the rewriter", lambda sql=sql: rt.query(KEY, sql, 0.1))
backend = DuckDBBackend(str(taxi_db))
for name, sql in ESCAPES.items():
    probe(f"{name} on the session connection", lambda sql=sql: backend.session_con().execute(sql).fetchall())
if os.environ.get("OCR_SPARK_URL"):
    print("--- Spark Connect backend (OCR_SPARK_URL) ---")
    from opencleanroom.backends import SparkBackend

    spark_backend = SparkBackend(os.environ["OCR_SPARK_URL"])
    SPARK_ESCAPES = {**ESCAPES, "ADD JAR": "ADD JAR /tmp/x.jar", "temporary function": "CREATE TEMPORARY FUNCTION f AS 'x.Y'",
                     "file as a table": "SELECT * FROM parquet.`/etc`", "reflect": "SELECT reflect('java.lang.System', 'getenv')",
                     "another database": "SELECT * FROM other_db.trips", "table function": "SELECT * FROM range(10)",
                     "script transform": "SELECT TRANSFORM(a) USING 'sh -c id' AS (b) FROM (SELECT 1 AS a) s",
                     "file paths": "SELECT input_file_name() FROM trips", "server user": "SELECT COUNT(*) FROM trips WHERE hack_license = user"}
    for name, sql in SPARK_ESCAPES.items():
        probe(f"{name} on the Spark session connection", lambda sql=sql: spark_backend.session_con().execute(sql).fetchall())
probe("external access disabled",
      lambda: backend.con.execute("SELECT current_setting('enable_external_access')").fetchone()[0] in (False, "false"), expect_block=False)
probe("a view in place of a table is still truncated", lambda: _view_bound(), expect_block=False)

print("--- ADR 0004 and budget sharing ---")
probe("raise budget inside the epoch", lambda: catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.budget.epsilon": "12.0"})))
probe("apply an unrecorded epoch label", lambda: catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.budget.epoch": "2026-12"})))
probe("raise budget via an old epoch", lambda: _old_epoch_raise())
probe("re-apply the git file of the old epoch after a renewal", lambda: catalog.apply_policy(cat, TAXI_YAML))
probe("record an epoch that sorts before the current one", lambda: catalog.new_epoch(cat, "nyc_taxi", "2025-01", "s", "r"))
probe("whitespace epoch", lambda: catalog.apply_policy(cat, _policy(TAXI_YAML, **{"dataset.budget.epoch": " 2026-10 ", "dataset.budget.epsilon": "12.0"})))
probe("apply dropping a consumer with an active key", lambda: catalog.apply_policy(cat, _policy(TAXI_YAML, **{"consumers.counts-only": None})))
probe("two consumers cannot exceed the dataset budget together", _shared_budget, expect_block=False)
probe("cross-process spend: a fresh Runtime sees the same ledger",
      lambda: Runtime(cat).remaining("nyc_taxi", "analytics-team") == rt.remaining("nyc_taxi", "analytics-team"),
      expect_block=False)

print("--- concurrency ---")

probe("20 threads reserve exactly the budget", _threads_reserve, expect_block=False)
probe("second writer gets the lock, file intact", _two_writers, expect_block=False)

print("--- metadata ---")
probe("DESCRIBE a table outside the policy", lambda: rt.query(COUNTS, "DESCRIBE drivers"))
probe("SHOW TABLES lists only the consumer's tables", lambda: [r[0] for r in rt.query(COUNTS, "SHOW TABLES")] == ["taxi_zones", "trips"], expect_block=False)
probe("DESCRIBE followed by a statement", lambda: rt.query(COUNTS, "DESCRIBE trips; SELECT 1"))


print("--- http api ---")
from opencleanroom.api import create_app  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

http = TestClient(create_app(cat, rate="1000/60"), raise_server_exceptions=False)
_h = {"Authorization": f"Bearer {KEY}"}


def _post(sql, epsilon=0.1, headers=None):
    return http.post("/v1/query", json={"sql": sql, "epsilon": epsilon}, headers={**_h, **(headers or {})})


def _no_backend_text(resp, code):
    body = resp.json()
    low = resp.text.lower()
    return body.get("error") == code and set(body) == {"error", "message"} and not any(w in low for w in ("duckdb", "traceback", "sqlite", "/", "catalog"))


probe("HTTP unsupported_query carries no backend text", lambda: _no_backend_text(_post("SELECT * FROM trips"), "unsupported_query"), expect_block=False)
probe("HTTP policy_violation carries no backend text",
      lambda: _no_backend_text(_post("SELECT COUNT(*) FROM trips", epsilon=9.0), "policy_violation"), expect_block=False)
probe("HTTP invalid_key carries no backend text",
      lambda: _no_backend_text(http.get("/v1/budget", headers={"Authorization": "Bearer x"}), "invalid_key"), expect_block=False)
probe("HTTP bad JSON body is bad_request, not internal",
      lambda: _no_backend_text(http.post("/v1/query", content=b"{", headers={**_h, "content-type": "application/json"}), "bad_request"), expect_block=False)
probe("HTTP NaN epsilon is bad_request and spends nothing",
      lambda: _no_backend_text(http.post("/v1/query", content=b'{"sql": "SELECT COUNT(*) FROM trips", "epsilon": NaN}',
                                         headers={**_h, "content-type": "application/json"}), "bad_request"), expect_block=False)


def _http_replay():
    before = float(http.get("/v1/budget", headers=_h).json()["remaining"])
    a = _post("SELECT COUNT(*) FROM trips", headers={"Idempotency-Key": "red-replay"})
    b = _post("SELECT COUNT(*) FROM trips", headers={"Idempotency-Key": "red-replay"})
    after = float(http.get("/v1/budget", headers=_h).json()["remaining"])
    return a.status_code == b.status_code == 200 and a.json() == b.json() and abs(before - after - 0.1) < 1e-9


probe("HTTP replay with the same Idempotency-Key spends once", _http_replay, expect_block=False)
probe("HTTP same Idempotency-Key under another consumer runs its own query (one more audit row)",
      lambda: http.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1},
                        headers={"Authorization": f"Bearer {COUNTS}", "Idempotency-Key": "red-replay"}).status_code == 200
      and sum(1 for a in rt.audit("nyc_taxi") if a.request_id == "red-replay") == 2, expect_block=False)
probe("HTTP schema lists only the key's tables",
      lambda: "drivers" not in http.get("/v1/schema", headers={"Authorization": f"Bearer {COUNTS}"}).json()["tables"], expect_block=False)
_small = TestClient(create_app(cat, rate="3/60"), raise_server_exceptions=False)
probe("HTTP unknown keys are rate limited together",
      lambda: [_small.get("/v1/budget", headers={"Authorization": f"Bearer k{i}"}).status_code for i in range(4)] == [401, 401, 401, 429],
      expect_block=False)

bad = [r for r in RESULTS if r.startswith("FAIL") or r.startswith("??")]
known = [r for r in RESULTS if r.startswith("KNOWN")]
print(f"\n{len(RESULTS)} probes, {len(bad)} failing, {len(known)} known")
if os.environ.get("OCR_RED_KEEP") is None:
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
sys.exit(1 if bad else 0)
