# SPDX-License-Identifier: Apache-2.0
"""A key becomes a Session: stored policy, read-only DuckDB backend, the shared SQLite ledger of the catalog file."""
from __future__ import annotations

from fractions import Fraction
from pathlib import Path

import duckdb
import pytest
import yaml
from cleanroom_sql import BudgetExhaustedException, QueryFailed, UnsupportedQuery
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from opencleanroom.backends import DuckDBBackend, open_backend
from opencleanroom.runtime import AuthError, InProgress, Runtime

TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()


def _policy_text(**changes) -> str:
    doc = yaml.safe_load(TAXI_YAML)
    for dotted, value in changes.items():
        node = doc
        *parents, last = dotted.split(".")
        for p in parents:
            node = node[p]
        node[last] = value
    return yaml.safe_dump(doc, sort_keys=False)


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=100)
    con.close()
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "duckdb-dev", "duckdb", str(db))
    catalog.apply_policy(cat, _policy_text())
    return cat


def test_duckdb_backend_is_read_only(tmp_path):
    db = tmp_path / "t.duckdb"
    duckdb.connect(str(db)).execute("CREATE TABLE t(x INT)").close()
    b = DuckDBBackend(str(db))
    assert b.dialect == "duckdb" and b.con.execute("SELECT COUNT(*) FROM t").fetchall() == [(0,)]
    with pytest.raises(duckdb.Error):
        b.con.execute("INSERT INTO t VALUES (1)")


def test_open_backend_rejects_unknown_kind(tmp_path):
    with pytest.raises(ValueError):
        open_backend("nosuchkind", "x")


def test_open_session_runs_a_query(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    res = rt.open_session(key).query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert len(res.rows) == 1 and res.sensitivities == (50,)


def test_unknown_and_revoked_keys_get_the_same_generic_error(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    catalog.revoke_consumer(env, "nyc_taxi", "analytics-team")
    msgs = set()
    for k in (key, "ocr_nothing"):
        with pytest.raises(AuthError) as e:
            rt.open_session(k)
        msgs.add(str(e.value))
    assert msgs == {"invalid key"}


def test_sessions_in_one_runtime_share_the_ledger(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    rt.open_session(key).query("SELECT COUNT(*) FROM trips", epsilon=0.5)
    assert rt.open_session(key).spent == 0.5
    assert rt.remaining("nyc_taxi", "analytics-team") == 4.5
    assert rt.budgets("nyc_taxi") == {"budget": 10, "spent": 0.5, "remaining": 9.5}


def test_reapply_in_the_same_epoch_keeps_spend_and_new_epoch_starts_fresh(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    rt.open_session(key).query("SELECT COUNT(*) FROM trips", epsilon=0.5)
    catalog.apply_policy(env, _policy_text(**{"dataset.truncation.k": 40}))  # version 2, same epoch
    assert rt.open_session(key).spent == 0.5
    catalog.new_epoch(env, "nyc_taxi", "2026-11", "snap-2026-11", "monthly renewal")
    catalog.apply_policy(env, _policy_text(**{"dataset.budget.epoch": "2026-11"}))  # new epoch
    assert rt.open_session(key).spent == 0


def test_budget_blocks_across_sessions(env):
    key = catalog.create_consumer(env, "nyc_taxi", "counts-only")
    rt = Runtime(env)
    catalog.apply_policy(env, _policy_text(**{"dataset.budget.epsilon": "0.3", "consumers.analytics-team.budget.epsilon": 0.3,
                                              "consumers.analytics-team.max_epsilon_per_query": 0.2}))
    rt.open_session(key).query("SELECT COUNT(*) FROM trips", epsilon=0.2)
    with pytest.raises(BudgetExhaustedException):
        rt.open_session(key).query("SELECT COUNT(*) FROM trips", epsilon=0.2)


def test_lowered_budget_reapplied_in_the_same_epoch_blocks_after_spend(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    rt.open_session(key).query("SELECT COUNT(*) FROM trips", epsilon=0.5)
    catalog.apply_policy(env, _policy_text(**{"dataset.budget.epsilon": "0.3", "consumers.analytics-team.budget.epsilon": 0.3,
                                              "consumers.analytics-team.max_epsilon_per_query": 0.1}))
    assert rt.remaining("nyc_taxi", "analytics-team") < 0
    with pytest.raises(BudgetExhaustedException):
        rt.open_session(key).query("SELECT COUNT(*) FROM trips", epsilon=0.1)


def test_budgets_for_an_unknown_dataset_is_a_runtime_error(env):
    with pytest.raises(RuntimeError):
        Runtime(env).budgets("ghost")


def test_sessions_get_their_own_cursor(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    a, b = rt.open_session(key), rt.open_session(key)
    assert a.con is not b.con


# -- hash tripwire and revocation at every query ------------------------------------

def test_tampered_policy_text_is_refused(env):
    import sqlite3
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    con = sqlite3.connect(env)
    con.execute("UPDATE policies SET text = replace(text, 'k: 50', 'k: 5000') WHERE dataset = 'nyc_taxi'")
    con.commit()
    con.close()
    with pytest.raises(RuntimeError, match="nyc_taxi"):
        Runtime(env).open_session(key)


def test_runtime_query_rechecks_the_key_every_time(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    res = rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert len(res.rows) == 1
    catalog.revoke_consumer(env, "nyc_taxi", "analytics-team")
    with pytest.raises(AuthError):
        rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)


def test_runtime_query_handles_metadata_without_epsilon(env):
    key = catalog.create_consumer(env, "nyc_taxi", "counts-only")
    rt = Runtime(env)
    rows = rt.query(key, "SHOW TABLES")
    assert [r[0] for r in rows] == ["taxi_zones", "trips"]
    assert rt.query(key, "DESCRIBE trips")[0][0] == "trip_id"
    with pytest.raises(ValueError, match="epsilon"):
        rt.query(key, "SELECT COUNT(*) FROM trips")


def test_duckdb_backend_has_no_external_access(tmp_path):
    db = tmp_path / "t.duckdb"
    duckdb.connect(str(db)).execute("CREATE TABLE t(x INT)").close()
    con = DuckDBBackend(str(db)).con
    assert con.execute("SELECT current_setting('enable_external_access')").fetchone()[0] in (False, "false")
    assert con.execute("SELECT current_setting('autoinstall_known_extensions')").fetchone()[0] in (False, "false")
    with pytest.raises(duckdb.Error):
        con.execute("COPY (SELECT 1) TO '/tmp/ocr-should-not-exist.csv'")


# -- persistent ledger, idempotent replay, public_query -----------------------------------

def test_two_runtimes_share_the_budget_through_the_file(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    Runtime(env).query(key, "SELECT COUNT(*) FROM trips", 0.5)
    assert Runtime(env).remaining("nyc_taxi", "analytics-team") == 4.5


def test_replay_with_the_same_request_id_spends_nothing(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    first = rt.query(key, "SELECT COUNT(*) FROM trips", 0.1, request_id="r1")
    second = rt.query(key, "SELECT COUNT(*) FROM trips", 0.1, request_id="r1")
    assert first.rows == second.rows and first.request_id == "r1"
    assert rt.remaining("nyc_taxi", "analytics-team") == Fraction(49, 10)


def test_in_flight_request_id_is_a_conflict(env):
    from opencleanroom.runtime import InProgress
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    _, ledger = rt._policy_and_ledger("nyc_taxi")
    ledger.for_request("stuck", "q").reserve("analytics-team", __import__("fractions").Fraction(1, 10))
    with pytest.raises(InProgress):
        rt.query(key, "SELECT COUNT(*) FROM trips", 0.1, request_id="stuck")


def test_every_query_gets_an_audit_row(env):
    from opencleanroom import ledger_store
    key = catalog.create_consumer(env, "nyc_taxi", "counts-only")
    rt = Runtime(env)
    rt.query(key, "SELECT COUNT(*) FROM trips", 0.1)
    with pytest.raises(UnsupportedQuery):
        rt.query(key, "SELECT MAX(fare_amount) FROM trips", 0.1)
    rows = rt.audit("nyc_taxi")
    assert [r.outcome for r in rows] == ["ok", "unsupported_query"]
    assert rows[0].epsilon_charged == __import__("fractions").Fraction(1, 10) and rows[1].epsilon_charged == 0
    assert isinstance(rows[0], ledger_store.AuditRow)


def test_public_query_reads_public_rows_only(env):
    key = catalog.create_consumer(env, "nyc_taxi", "counts-only")  # counts-only may read taxi_zones: public, and in its grant
    rt = Runtime(env)
    rows = rt.public_query(key, "SELECT DISTINCT borough FROM taxi_zones")
    assert set(r[0] for r in rows) >= {"Manhattan", "Brooklyn"}
    assert rt.remaining("nyc_taxi", "counts-only") == 10
    for sql in ("SELECT hack_license FROM trips", "SELECT * FROM taxi_zones z JOIN trips t ON t.pu_location_id = z.location_id",
                "SELECT borough FROM taxi_zones WHERE sqrt(location_id) > 1", "SELECT COUNT(*) FROM taxi_zones a, taxi_zones b, taxi_zones c"):
        with pytest.raises(UnsupportedQuery):
            rt.public_query(key, sql)


def test_public_query_is_capped(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rows = Runtime(env).public_query(key, "SELECT a.zone, b.zone FROM taxi_zones a JOIN taxi_zones b ON a.borough = b.borough")
    assert len(rows) <= 10_000


def test_replay_of_a_failed_query_raises_without_running_again(env, monkeypatch):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    monkeypatch.setattr(DuckDBBackend, "session_con", lambda self: duckdb.connect())  # an empty database: every query fails
    with pytest.raises(QueryFailed):
        rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1, request_id="broken")
    charged = rt.remaining("nyc_taxi", "analytics-team")
    with pytest.raises(QueryFailed):
        rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1, request_id="broken")
    assert rt.remaining("nyc_taxi", "analytics-team") == charged
    assert sum(1 for a in rt.audit("nyc_taxi") if a.request_id == "broken") == 1


def test_racing_the_same_new_request_id_is_a_conflict_charged_once(env):
    import threading
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    outcomes, barrier = [], threading.Barrier(8)

    def worker():
        barrier.wait()
        try:
            rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1, request_id="race")
            outcomes.append("ok")
        except InProgress:
            outcomes.append("in_progress")
    ts = [threading.Thread(target=worker) for _ in range(8)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert sorted(outcomes) == ["in_progress"] * 7 + ["ok"]
    assert rt.remaining("nyc_taxi", "analytics-team") == Fraction(49, 10)
    assert sum(1 for a in rt.audit("nyc_taxi") if a.request_id == "race") == 1


def test_public_query_audit_row_charges_nothing_and_ids_are_generated(env):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    rt = Runtime(env)
    rt.public_query(key, "SELECT zone FROM taxi_zones LIMIT 3")
    res = rt.query(key, "SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert res.request_id and len(res.request_id) == 36
    rows = rt.audit("nyc_taxi")
    public = [a for a in rows if a.rewritten_sql and "taxi_zones" in a.rewritten_sql]
    assert len(public) == 1 and public[0].outcome == "ok" and public[0].epsilon_charged == 0 and public[0].epsilon_requested == 0


def test_check_public_caps_the_limit():
    from cleanroom_sql import Rewriter, Schema, loads_policy
    policy = loads_policy(_policy_text())
    public = {n: t for n, t in policy.dataset.schema.tables.items() if t.is_public}
    rw = Rewriter(Schema(tables=public), k=policy.dataset.k, dialect=policy.dataset.dialect)
    assert rw.check_public("SELECT zone FROM taxi_zones").endswith("LIMIT 10000")
    assert rw.check_public("SELECT zone FROM taxi_zones LIMIT 999999").endswith("LIMIT 10000")
    assert rw.check_public("SELECT zone FROM taxi_zones LIMIT 5").endswith("LIMIT 5")
    with pytest.raises(UnsupportedQuery):
        rw.check_public("SELECT zone FROM taxi_zones LIMIT 'x'")


def test_public_query_honours_the_consumer_grant(env):
    catalog.apply_policy(env, _policy_text(**{"consumers.counts-only.tables": ["trips"]}))
    key = catalog.create_consumer(env, "nyc_taxi", "counts-only")
    with pytest.raises(UnsupportedQuery):  # taxi_zones is public, but not granted to this consumer: an unknown table to it
        Runtime(env).public_query(key, "SELECT borough FROM taxi_zones")


# -- the session connection runs one SELECT and nothing else -------------------------------

def test_session_connection_runs_only_a_single_select(tmp_path):
    db = tmp_path / "t.duckdb"
    duckdb.connect(str(db)).execute("CREATE TABLE t(x INT)").close()
    con = DuckDBBackend(str(db)).session_con()
    for sql in ("SELECT COUNT(*) FROM t", "SELECT a.x FROM t a JOIN t b ON a.x = b.x",
                "SELECT COUNT(*) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY x ORDER BY x) AS _rn FROM t) WHERE _rn <= 5"):
        con.execute(sql).fetchall()
    for sql in ("PRAGMA database_list", "CALL pragma_database_list()", "SELECT * FROM duckdb_databases()",
                "SELECT * FROM read_csv('/etc/hosts')", "DESCRIBE t", "SUMMARIZE t", "EXPLAIN SELECT 1",
                "SET threads = 1", "COPY (SELECT 1) TO '/tmp/ocr-x.csv'", "ATTACH '/tmp/ocr-x.duckdb'", "INSTALL httpfs",
                "LOAD httpfs", "EXPORT DATABASE '/tmp/ocr-x'", "CREATE TABLE u(x INT)", "SELECT 1; SELECT 2", "",
                "WITH a AS (SELECT x FROM t) SELECT COUNT(*) FROM a", "SELECT x FROM t UNION SELECT x FROM t"):  # allow-list: the rewriter emits neither
        with pytest.raises(UnsupportedQuery):
            con.execute(sql)


def test_backend_configuration_is_locked(tmp_path):
    db = tmp_path / "t.duckdb"
    duckdb.connect(str(db)).execute("CREATE TABLE t(x INT)").close()
    con = DuckDBBackend(str(db)).con
    for sql in ("SET threads = 1", "SET memory_limit = '1TB'", "SET enable_external_access = true"):
        with pytest.raises(duckdb.Error):
            con.execute(sql)
