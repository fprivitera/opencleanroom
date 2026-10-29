# SPDX-License-Identifier: Apache-2.0
"""HTTP API: Bearer keys, one error code per failure class, idempotent replay, free metadata, rate limit."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
import yaml
from cleanroom_sql import loads_policy
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from opencleanroom.api import create_app
from opencleanroom.ledger_store import SqliteLedger
from opencleanroom.ratelimit import RateLimiter
from starlette.testclient import TestClient

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
    catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epsilon": "1.0", "consumers.analytics-team.budget.epsilon": "1.0"}))
    return cat


@pytest.fixture
def client(env):
    return TestClient(create_app(env, rate="5/60"), raise_server_exceptions=False)


def _auth(key):
    return {"Authorization": f"Bearer {key}"}


def _audit_rows(env):
    policy = loads_policy(catalog.get_policy(env, "nyc_taxi").text)
    return SqliteLedger(env, "nyc_taxi", policy.dataset.epoch, policy).audit_rows()


def test_healthz_needs_no_key(client):
    assert client.get("/healthz").status_code == 200


def test_query_end_to_end(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=_auth(key))
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["rows"]) == 1 and body["epsilon_spent"] == 0.1 and body["request_id"]
    assert "NOISE" not in body["rewritten_sql"].upper() and body["remaining"] == pytest.approx(0.9)
    assert body["sensitivities"] == [50]  # k from the taxi policy


def test_schema_and_budget_cost_nothing(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    s = client.get("/v1/schema", headers=_auth(key))
    assert s.status_code == 200 and "trips" in s.json()["tables"] and "drivers" in s.json()["tables"]
    assert any(c["column"] == "fare_amount" for c in s.json()["tables"]["trips"]["columns"])
    b = client.get("/v1/budget", headers=_auth(key))
    assert b.status_code == 200 and b.json()["remaining"] == 1.0
    assert _audit_rows(env) == []


def test_metadata_query_costs_nothing(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    r = client.post("/v1/query", json={"sql": "SHOW TABLES"}, headers=_auth(key))
    assert r.status_code == 200 and [row[0] for row in r.json()["rows"]] == ["drivers", "taxi_zones", "trips"]
    assert client.get("/v1/budget", headers=_auth(key)).json()["remaining"] == 1.0


@pytest.mark.parametrize("header", [{}, {"Authorization": "Bearer nope"}, {"Authorization": "Basic abc"}])
def test_invalid_key(client, header):
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=header)
    assert r.status_code == 401 and r.json() == {"error": "invalid_key", "message": "unknown or revoked key"}


def test_error_codes_carry_no_backend_text(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    cases = {
        "SELECT * FROM trips": (400, "unsupported_query"),
        "SELECT COUNT(*) FROM trips": (400, "policy_violation"),  # ε above max_epsilon_per_query
        "SELECT COUNT(*) FROM trips WHERE fare > 'x'": (400, "unsupported_query"),
    }
    for sql, (status, code) in cases.items():
        r = client.post("/v1/query", json={"sql": sql, "epsilon": 0.9}, headers=_auth(key))
        assert (r.status_code, r.json()["error"]) == (status, code), (sql, r.text)
        assert "duckdb" not in r.text.lower() and "traceback" not in r.text.lower()
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips"}, headers=_auth(key))
    assert (r.status_code, r.json()["error"]) == (400, "bad_request")
    r = client.post("/v1/query", content=b"not json", headers={**_auth(key), "content-type": "application/json"})
    assert (r.status_code, r.json()["error"]) == (400, "bad_request")


def test_budget_exhausted_is_429(env):
    client = TestClient(create_app(env, rate="100/60"), raise_server_exceptions=False)
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    for _ in range(10):
        assert client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=_auth(key)).status_code == 200
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=_auth(key))
    assert (r.status_code, r.json()["error"]) == (429, "budget_exhausted")


def test_idempotent_replay_spends_once(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    body = {"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}
    h = {**_auth(key), "Idempotency-Key": "abc-1"}
    first = client.post("/v1/query", json=body, headers=h)
    second = client.post("/v1/query", json=body, headers=h)
    assert first.status_code == second.status_code == 200 and first.json() == second.json()
    assert first.json()["request_id"] == "abc-1" and first.json()["remaining"] == pytest.approx(0.9)
    assert len(_audit_rows(env)) == 1
    third = client.post("/v1/query", json={**body, "idempotency_key": "abc-1"}, headers=_auth(key))
    assert third.json() == first.json()


def test_in_progress_is_409(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    policy = loads_policy(catalog.get_policy(env, "nyc_taxi").text)
    ledger = SqliteLedger(env, "nyc_taxi", policy.dataset.epoch, policy)
    from fractions import Fraction
    ledger.for_request("stuck", "q", "analytics-team").reserve("analytics-team", Fraction(1, 10))  # crashed worker
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers={**_auth(key), "Idempotency-Key": "stuck"})
    assert (r.status_code, r.json()["error"]) == (409, "in_progress")


def test_vanished_backend_is_internal_without_detail(env, client, tmp_path):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    (tmp_path / "taxi.duckdb").unlink()  # the backend cannot be opened: not a query failure
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=_auth(key))
    assert (r.status_code, r.json()) == (500, {"error": "internal", "message": "internal error"})
    assert "taxi.duckdb" not in r.text


def test_internal_error_hides_detail(env, client, monkeypatch):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    from opencleanroom import runtime as rt_mod

    def boom(*a, **k):
        raise RuntimeError("secret path /srv/x")
    monkeypatch.setattr(rt_mod.Runtime, "query", boom)
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=_auth(key))
    assert r.status_code == 500 and r.json() == {"error": "internal", "message": "internal error"}


def test_unknown_keys_share_one_bucket(client):
    codes = [client.get("/v1/budget", headers={"Authorization": f"Bearer bad{i}"}).status_code for i in range(7)]
    assert codes == [401] * 5 + [429] * 2
    r = client.get("/v1/budget", headers={"Authorization": "Bearer bad99"})
    assert r.json() == {"error": "rate_limited", "message": "too many requests"}


def test_known_key_has_its_own_bucket(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    for _ in range(5):
        client.get("/v1/budget", headers={"Authorization": "Bearer bad"})
    assert client.get("/v1/budget", headers=_auth(key)).status_code == 200
    codes = [client.get("/v1/budget", headers=_auth(key)).status_code for _ in range(5)]
    assert codes == [200] * 4 + [429]
    assert client.get("/healthz").status_code == 200


def test_rate_limiter_refills_and_evicts():
    now = [0.0]
    rl = RateLimiter("2/10", max_keys=2, clock=lambda: now[0])
    assert rl.allow("a") and rl.allow("a") and not rl.allow("a")
    now[0] += 5.0  # one token back
    assert rl.allow("a") and not rl.allow("a")
    assert rl.allow("b") and rl.allow("c")  # "a" evicted (LRU of two)
    assert rl.allow("a")
    with pytest.raises(ValueError):
        RateLimiter("0/10")
    with pytest.raises(ValueError):
        RateLimiter("ten/10")


def test_non_finite_epsilon_is_bad_request(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    r = client.post("/v1/query", content=b'{"sql": "SELECT COUNT(*) FROM trips", "epsilon": NaN}', headers={**_auth(key), "content-type": "application/json"})
    assert (r.status_code, r.json()["error"]) == (400, "bad_request")
    assert client.get("/v1/budget", headers=_auth(key)).json()["remaining"] == 1.0


def test_query_failed_from_the_backend_is_502(env, client, monkeypatch):
    from cleanroom_sql import QueryFailed
    from opencleanroom import runtime as rt_mod
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")

    def fail(self, sql, epsilon):
        raise QueryFailed("duckdb said: /secret/path")
    monkeypatch.setattr(rt_mod.Session, "query", fail)
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=_auth(key))
    assert (r.status_code, r.json()) == (502, {"error": "query_failed", "message": "the backend could not run the query"})


def test_date_rows_are_text_in_json_and_in_the_stored_response(env, client, monkeypatch):
    import datetime

    from cleanroom_sql import Result
    from opencleanroom import runtime as rt_mod
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    fake = Result(rows=[(datetime.date(2026, 10, 23), 7)], rewritten_sql="SELECT 1", epsilon_spent=0.1, epsilon_per_measurement=0.1,
                  sensitivities=(50,), ci95_halfwidth=(1.0,), request_id="d1")
    monkeypatch.setattr(rt_mod.Session, "query", lambda self, sql, epsilon: fake)
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers={**_auth(key), "Idempotency-Key": "d1"})
    assert r.status_code == 200 and r.json()["rows"] == [["2026-10-23", 7]]
    [row] = _audit_rows(env)  # the stored response is JSON text, so a replay returns the same rows
    assert row.outcome == "ok" and json.loads(row.response)["rows"] == [["2026-10-23", 7]]


def test_body_too_large_is_413(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    big = b'{"sql": "' + b"x" * 1_100_000 + b'", "epsilon": 0.1}'
    r = client.post("/v1/query", content=big, headers={**_auth(key), "content-type": "application/json"})
    assert (r.status_code, r.json()["error"]) == (413, "bad_request")


def test_huge_integer_epsilon_is_bad_request(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    r = client.post("/v1/query", content=b'{"sql": "SELECT COUNT(*) FROM trips", "epsilon": ' + b"9" * 400 + b"}",
                    headers={**_auth(key), "content-type": "application/json"})
    assert (r.status_code, r.json()["error"]) == (400, "bad_request")


def test_two_different_idempotency_keys_are_bad_request(env, client):
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    r = client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1, "idempotency_key": "b"},
                    headers={**_auth(key), "Idempotency-Key": "a"})
    assert (r.status_code, r.json()["error"]) == (400, "bad_request") and _audit_rows(env) == []


def test_remaining_does_not_show_other_consumers_spend(env, client):
    a = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    b = catalog.create_consumer(env, "nyc_taxi", "counts-only")
    before = client.get("/v1/budget", headers=_auth(a)).json()["remaining"]
    assert client.post("/v1/query", json={"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}, headers=_auth(b)).status_code == 200
    assert client.get("/v1/budget", headers=_auth(a)).json()["remaining"] == before
