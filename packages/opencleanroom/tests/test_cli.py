# SPDX-License-Identifier: Apache-2.0
"""The `opencleanroom` command, driven as a subprocess on a temp catalog and a temp DuckDB file."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
import yaml
from cleanroom_sql.taxi import populate

HERE = Path(__file__).resolve().parent
TAXI_YAML = HERE.parents[1] / "cleanroom-sql" / "examples" / "taxi.yaml"


def _run(cat: Path, *args: str, stdin: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-m", "opencleanroom", "--catalog", str(cat), *args], check=False,
                          capture_output=True, text=True, input=stdin, cwd=HERE.parent)


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=100)
    con.close()
    cat = tmp_path / "catalog.sqlite"
    assert _run(cat, "init").returncode == 0
    assert _run(cat, "backend", "add", "duckdb-dev", "--duckdb", str(db)).returncode == 0
    return cat


def _policy_file(tmp_path, **changes) -> Path:
    doc = yaml.safe_load(TAXI_YAML.read_text())
    for dotted, value in changes.items():
        node = doc
        *parents, last = dotted.split(".")
        for p in parents:
            node = node[p]
        node[last] = value
    f = tmp_path / "policy.yaml"
    f.write_text(yaml.safe_dump(doc, sort_keys=False))
    return f


def test_init_and_backend_list(env):
    r = _run(env, "backend", "list", "--json")
    assert r.returncode == 0 and json.loads(r.stdout)[0]["name"] == "duckdb-dev"


def test_policy_apply_and_show(env):
    r = _run(env, "policy", "apply", str(TAXI_YAML))
    assert r.returncode == 0 and r.stdout.strip() == "applied nyc_taxi version 1 epoch 2026-10"
    r = _run(env, "policy", "show", "nyc_taxi")
    assert r.returncode == 0 and "version 1" in r.stdout and "name: nyc_taxi" in r.stdout


def test_policy_apply_lists_every_error(env, tmp_path):
    # loader faults come first (as cleanroom-sql policy check); catalog faults are listed together once the file loads
    f = _policy_file(tmp_path, **{"consumers.analytics-team.aggregates": ["count", "max"]})
    r = _run(env, "policy", "apply", str(f))
    assert r.returncode == 1 and "consumers.analytics-team.aggregates: " in r.stderr
    doc = yaml.safe_load(TAXI_YAML.read_text())
    doc["dataset"]["backend"] = "nope"
    doc["dataset"]["budget"].pop("epoch")
    f.write_text(yaml.safe_dump(doc, sort_keys=False))
    r = _run(env, "policy", "apply", str(f))
    assert r.returncode == 1
    assert "dataset.backend: " in r.stderr and "dataset.budget.epoch: " in r.stderr


def test_consumer_create_prints_the_key_once_and_list_hides_it(env):
    _run(env, "policy", "apply", str(TAXI_YAML))
    r = _run(env, "consumer", "create", "nyc_taxi", "analytics-team")
    assert r.returncode == 0
    key = r.stdout.strip()
    assert key.startswith("ocr_") and "\n" not in key
    r = _run(env, "consumer", "list", "nyc_taxi", "--json")
    rows = json.loads(r.stdout)
    assert rows[0]["name"] == "analytics-team" and rows[0]["revoked_at"] is None and key not in r.stdout


def test_query_end_to_end_and_json(env):
    _run(env, "policy", "apply", str(TAXI_YAML))
    key = _run(env, "consumer", "create", "nyc_taxi", "analytics-team").stdout.strip()
    r = _run(env, "query", "--key", key, "SELECT COUNT(*) FROM trips", "--epsilon", "0.1")
    assert r.returncode == 0, r.stderr
    assert "ROW_NUMBER" in r.stdout and "remaining" in r.stdout and "shared through the catalog ledger" in r.stderr
    r = _run(env, "query", "--key", key, "SELECT COUNT(*) FROM trips", "--epsilon", "0.1", "--json")
    out = json.loads(r.stdout)
    assert len(out["rows"]) == 1 and out["sensitivities"] == [50] and out["epsilon_per_measurement"] == 0.1


def test_query_with_a_bad_key_is_generic(env):
    _run(env, "policy", "apply", str(TAXI_YAML))
    key = _run(env, "consumer", "create", "nyc_taxi", "analytics-team").stdout.strip()
    _run(env, "consumer", "revoke", "nyc_taxi", "analytics-team")
    for k in (key, "ocr_nothing"):
        r = _run(env, "query", "--key", k, "SELECT COUNT(*) FROM trips", "--epsilon", "0.1")
        assert r.returncode == 1 and r.stderr.strip() == "invalid key"


def test_query_policy_violation_is_exit_one_with_message(env):
    _run(env, "policy", "apply", str(TAXI_YAML))
    key = _run(env, "consumer", "create", "nyc_taxi", "counts-only").stdout.strip()
    r = _run(env, "query", "--key", key, "SELECT SUM(fare_amount) FROM trips", "--epsilon", "0.1")
    assert r.returncode == 1 and "sum" in r.stderr


def test_missing_catalog_exits_two(tmp_path):
    r = _run(tmp_path / "nope.sqlite", "backend", "list")
    assert r.returncode == 2


def test_console_script_is_declared():
    assert 'opencleanroom = "opencleanroom.__main__:main"' in (HERE.parent / "pyproject.toml").read_text()


def test_consumer_revoke_exit_codes(env):
    _run(env, "policy", "apply", str(TAXI_YAML))
    _run(env, "consumer", "create", "nyc_taxi", "analytics-team")
    assert _run(env, "consumer", "revoke", "nyc_taxi", "analytics-team").returncode == 0
    r = _run(env, "consumer", "revoke", "nyc_taxi", "analytics-team")
    assert r.returncode == 1 and "no active key" in r.stderr


def test_backend_add_requires_an_existing_file(env, tmp_path):
    r = _run(env, "backend", "add", "ghost", "--duckdb", str(tmp_path / "missing.duckdb"))
    assert r.returncode == 2 and "not found" in r.stderr


def test_not_a_catalog_exits_two(tmp_path):
    bad = tmp_path / "bad.sqlite"
    bad.write_text("not a database")
    assert _run(bad, "backend", "list").returncode == 2


def test_describe_and_show_tables_cost_nothing(env):
    _run(env, "policy", "apply", str(TAXI_YAML))
    key = _run(env, "consumer", "create", "nyc_taxi", "counts-only").stdout.strip()
    r = _run(env, "query", "--key", key, "DESCRIBE trips", "--json")
    assert r.returncode == 0 and json.loads(r.stdout)[3] == ["fare_amount", "int", "[0, 20000]"]
    r = _run(env, "query", "--key", key, "SHOW TABLES")
    assert r.returncode == 0 and r.stdout.splitlines()[0].startswith("taxi_zones\tpublic")
    r = _run(env, "query", "--key", key, "DESCRIBE drivers")
    assert r.returncode == 1 and "drivers" in r.stderr
    r = _run(env, "query", "--key", key, "SELECT COUNT(*) FROM trips")
    assert r.returncode == 1 and "--epsilon" in r.stderr


def test_key_from_environment(env):
    import os
    _run(env, "policy", "apply", str(TAXI_YAML))
    key = _run(env, "consumer", "create", "nyc_taxi", "counts-only").stdout.strip()
    r = subprocess.run([sys.executable, "-m", "opencleanroom", "--catalog", str(env), "query", "SHOW TABLES"],
                       check=False, capture_output=True, text=True, cwd=HERE.parent, env={**os.environ, "OPENCLEANROOM_KEY": key})
    assert r.returncode == 0 and r.stdout.startswith("taxi_zones")
    r = _run(env, "query", "SHOW TABLES")
    assert r.returncode == 1 and "OPENCLEANROOM_KEY" in r.stderr


def test_backend_error_is_generic_on_the_cli(env, tmp_path):
    """A query the backend cannot run reports no backend text (a declared int column that holds text in the file)."""
    bad = tmp_path / "bad.duckdb"
    con = duckdb.connect(str(bad))
    populate(con, n_drivers=10)
    con.execute("ALTER TABLE trips ALTER fare_amount TYPE VARCHAR")
    con.execute("UPDATE trips SET fare_amount = 'secret-' || fare_amount")
    con.close()
    _run(env, "backend", "add", "bad-dev", "--duckdb", str(bad))
    _run(env, "policy", "apply", str(_policy_file(tmp_path, **{"dataset.name": "bad_taxi", "dataset.backend": "bad-dev"})))
    key = _run(env, "consumer", "create", "bad_taxi", "analytics-team").stdout.strip()
    r = _run(env, "query", "--key", key, "SELECT SUM(fare_amount) FROM trips", "--epsilon", "0.1")
    assert r.returncode == 1 and r.stderr.strip() == "the backend could not run the query"


# -- admin commands: epochs, audit, ledger -------------------------------------------

def test_new_epoch_then_apply(env, tmp_path):
    assert _run(env, "policy", "apply", str(TAXI_YAML)).returncode == 0
    renewed = _policy_file(tmp_path, **{"dataset.budget.epoch": "2026-11"})
    r = _run(env, "policy", "apply", str(renewed))
    assert r.returncode == 1 and "dataset.budget.epoch: " in r.stderr and "new-epoch" in r.stderr
    r = _run(env, "budget", "new-epoch", "nyc_taxi", "--epoch", "2026-11", "--snapshot", "snap-7", "--reason", "monthly renewal")
    assert r.returncode == 0 and r.stdout.strip() == "epoch 2026-11 recorded for nyc_taxi; apply the policy file with budget.epoch: 2026-11"
    r = _run(env, "policy", "apply", str(renewed))
    assert r.returncode == 0 and r.stdout.strip() == "applied nyc_taxi version 2 epoch 2026-11"
    r = _run(env, "budget", "new-epoch", "nyc_taxi", "--epoch", "2026-11", "--snapshot", "snap-7", "--reason", "again")
    assert r.returncode == 1 and "not after the current epoch" in r.stderr


def test_audit_list_reconcile_and_ledger_release(env):
    assert _run(env, "policy", "apply", str(TAXI_YAML)).returncode == 0
    key = _run(env, "consumer", "create", "nyc_taxi", "analytics-team").stdout.strip()
    assert _run(env, "query", "--key", key, "SELECT COUNT(*) FROM trips", "--epsilon", "0.1").returncode == 0
    assert _run(env, "query", "--key", key, "SELECT MAX(fare_amount) FROM trips", "--epsilon", "0.1").returncode == 1
    r = _run(env, "audit", "list", "nyc_taxi", "--json")
    rows = json.loads(r.stdout)
    assert [x["outcome"] for x in rows] == ["ok", "unsupported_query"] and rows[0]["epsilon_charged"] == 0.1 and rows[1]["epsilon_charged"] == 0
    r = _run(env, "audit", "reconcile", "nyc_taxi")
    assert r.returncode == 0 and "ledger committed 1/10\taudit charged 1/10\tok" in r.stdout
    # a worker that dies between reserve and commit leaves a reserved row
    code = f"""
from fractions import Fraction
from cleanroom_sql import loads_policy
from opencleanroom import catalog
from opencleanroom.ledger_store import SqliteLedger
p = loads_policy(catalog.get_policy({str(env)!r}, "nyc_taxi").text)
SqliteLedger({str(env)!r}, "nyc_taxi", p.dataset.epoch, p).for_request("crash-1", "SELECT COUNT(*) FROM trips", "analytics-team").reserve("analytics-team", Fraction(3, 10))
"""
    subprocess.run([sys.executable, "-c", code], check=True, cwd=HERE.parent)
    r = _run(env, "ledger", "list", "nyc_taxi")
    assert r.returncode == 0 and "analytics-team\t3/10\tcrash-1" in r.stdout
    row_id = r.stdout.split("\t")[0]
    r = _run(env, "audit", "reconcile", "nyc_taxi")
    assert r.returncode == 0 and "reserved rows 1\tin_progress rows 1" in r.stdout
    assert _run(env, "ledger", "release", "999", "--reason", "x").returncode == 1
    r = _run(env, "ledger", "release", row_id, "--reason", "worker crashed during the demo")
    assert r.returncode == 0 and r.stdout.strip() == f"ledger row {row_id} released"
    assert _run(env, "ledger", "list", "nyc_taxi").stdout == ""
    assert "worker crashed during the demo" in _run(env, "audit", "list", "nyc_taxi", "--json").stdout


def test_backend_add_spark(env):
    r = _run(env, "backend", "add", "spark-dev", "--spark", "sc://127.0.0.1:15002", "--database", "nyc")
    assert r.returncode == 0 and r.stdout.strip() == "registered backend spark-dev"
    rows = json.loads(_run(env, "backend", "list", "--json").stdout)
    assert {"name": "spark-dev", "kind": "spark", "location": "sc://127.0.0.1:15002/;database=nyc"}.items() <= next(
        b for b in rows if b["name"] == "spark-dev").items()
    r = _run(env, "backend", "add", "bad", "--spark", "http://x:1")
    assert r.returncode == 1 and "Spark Connect URL" in r.stderr
    assert _run(env, "backend", "add", "both", "--spark", "sc://x:1", "--duckdb", "/x").returncode == 2  # argparse: one or the other


def test_backend_add_with_a_query_timeout(env):
    r = _run(env, "backend", "add", "t", "--spark", "sc://127.0.0.1:15002", "--timeout", "12.5")
    assert r.returncode == 0
    assert next(b for b in json.loads(_run(env, "backend", "list", "--json").stdout) if b["name"] == "t")["query_timeout"] == 12.5


def test_backend_add_database_needs_spark_and_list_hides_the_token(env, tmp_path):
    db = tmp_path / "other.duckdb"
    duckdb.connect(str(db)).close()
    r = _run(env, "backend", "add", "x", "--duckdb", str(db), "--database", "nyc")
    assert r.returncode == 1 and "Spark backend only" in r.stderr
    assert _run(env, "backend", "add", "tok", "--spark", "sc://h:15002/;token=s3cret").returncode == 0
    out = _run(env, "backend", "list").stdout + _run(env, "backend", "list", "--json").stdout
    assert "s3cret" not in out and "token=***" in out


def test_backend_check_keys_on_duckdb(env, tmp_path):
    assert _run(env, "policy", "apply", str(TAXI_YAML)).returncode == 0
    r = _run(env, "backend", "check-keys", "nyc_taxi")
    assert r.returncode == 0 and r.stdout.strip() == "declared primary keys of nyc_taxi are unique"
    db = tmp_path / "dup.duckdb"  # same data, one zone twice
    con = duckdb.connect(str(db))
    populate(con, n_drivers=20)
    con.execute("INSERT INTO taxi_zones SELECT * FROM taxi_zones LIMIT 1")
    con.close()
    other = tmp_path / "other.sqlite"
    assert _run(other, "init").returncode == 0 and _run(other, "backend", "add", "duckdb-dev", "--duckdb", str(db)).returncode == 0
    assert _run(other, "policy", "apply", str(TAXI_YAML)).returncode == 0
    r = _run(other, "backend", "check-keys", "nyc_taxi")
    assert r.returncode == 1 and "taxi_zones" in r.stderr and "primary keys" in r.stderr
    r = _run(other, "backend", "materialize", "nyc_taxi", "--snapshot", "s1")
    assert r.returncode == 1 and "Spark" in r.stderr


def test_policy_check_and_diff(env, tmp_path):
    assert _run(env, "policy", "check", str(TAXI_YAML)).stdout.startswith("OK: nyc_taxi")
    assert "new dataset" in _run(env, "policy", "diff", str(TAXI_YAML)).stdout
    _run(env, "policy", "apply", str(TAXI_YAML))
    assert "no changes" in _run(env, "policy", "diff", str(TAXI_YAML)).stdout
    changed = _policy_file(tmp_path, **{"dataset.truncation": {"k": 40}})
    r = _run(env, "policy", "diff", str(changed))
    assert r.returncode == 0 and "~ dataset.truncation.k: 50 -> 40" in r.stdout
    assert "version 1" in _run(env, "policy", "show", "nyc_taxi").stdout  # a diff applies nothing


def test_query_sql_from_file_or_stdin(env, tmp_path):
    _run(env, "policy", "apply", str(TAXI_YAML))
    key = _run(env, "consumer", "create", "nyc_taxi", "counts-only").stdout.strip()
    sql = tmp_path / "q.sql"
    sql.write_text("SHOW\nTABLES\n")
    assert _run(env, "query", "--key", key, "--file", str(sql)).stdout.startswith("taxi_zones")
    assert _run(env, "query", "--key", key, "--file", "-", stdin="SHOW TABLES").stdout.startswith("taxi_zones")
    r = _run(env, "query", "--key", key, "--file", str(sql), "SHOW TABLES")
    assert r.returncode == 1 and "not both" in r.stderr
    sql.write_text("  \n")
    r = _run(env, "query", "--key", key, "--file", str(sql))
    assert r.returncode == 1 and "contains no SQL" in r.stderr


def test_json_output_serialises_dates(env, tmp_path):
    import datetime

    from opencleanroom.__main__ import _jsonable
    assert json.dumps(_jsonable([(datetime.date(2026, 1, 1), 3)])) == '[["2026-01-01", 3]]'
