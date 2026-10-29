# SPDX-License-Identifier: Apache-2.0
"""The Streamlit console, driven headless with AppTest: admin applies a policy and creates a key; consumer runs a query."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from cleanroom_sql.taxi import populate
from opencleanroom import catalog
from streamlit.testing.v1 import AppTest

HERE = Path(__file__).resolve().parent
CONSOLE = HERE.parent / "opencleanroom" / "console.py"
TAXI_YAML = HERE.parents[1] / "cleanroom-sql" / "examples" / "taxi.yaml"


@pytest.fixture
def env(tmp_path, monkeypatch):
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=100)
    con.close()
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "duckdb-dev", "duckdb", str(db))
    monkeypatch.setenv("OPENCLEANROOM_CATALOG", str(cat))
    return cat


def _app() -> AppTest:
    at = AppTest.from_file(str(CONSOLE), default_timeout=30)
    at.run()
    assert not at.exception, at.exception
    return at


def test_admin_applies_a_policy_and_creates_a_key(env):
    at = _app()
    assert at.sidebar.radio[0].value == "Admin"
    at.file_uploader[0].upload("taxi.yaml", TAXI_YAML.read_bytes()).run()
    assert any("applied nyc_taxi version 1" in s.value for s in at.success)
    assert "nyc_taxi" in str(at.dataframe[1].value)  # policies table
    at.selectbox[0].set_value("nyc_taxi")
    at.text_input[0].set_value("analytics-team")
    at.button[0].click().run()
    key = [c.value for c in at.code if c.value.startswith("ocr_")]
    assert len(key) == 1 and catalog.resolve_key(env, key[0]) == ("nyc_taxi", "analytics-team")
    assert any("catalog ledger" in m.value for m in at.caption)


def test_admin_shows_loader_errors_with_paths(env):
    at = _app()
    bad = TAXI_YAML.read_text().replace("aggregates: [count, sum, avg]", "aggregates: [count, max]")
    at.file_uploader[0].upload("bad.yaml", bad.encode()).run()
    assert any("consumers.analytics-team.aggregates" in e.value for e in at.error)


def test_consumer_runs_a_query_and_remaining_drops(env):
    catalog.apply_policy(env, TAXI_YAML.read_text())
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    at = _app()
    at.sidebar.radio[0].set_value("Consumer").run()
    at.text_input[0].set_value(key)
    at.text_area[0].set_value("SELECT COUNT(*) FROM trips")
    at.number_input[0].set_value(0.1)
    at.button[0].click().run()
    assert not at.exception
    assert "ROW_NUMBER" in at.code[0].value
    assert len(at.dataframe[0].value) == 1
    remaining = [m.value for m in at.metric if m.label == "remaining ε"]
    assert remaining == ["4.9"]


def test_consumer_bad_key_is_generic(env):
    at = _app()
    at.sidebar.radio[0].set_value("Consumer").run()
    at.text_input[0].set_value("ocr_nothing")
    at.text_area[0].set_value("SELECT COUNT(*) FROM trips")
    at.button[0].click().run()
    assert [e.value for e in at.error] == ["We couldn't connect with this key."]
    assert any(c.value == "code: AuthError" for c in at.caption)


def test_admin_clicks_after_an_upload_do_not_reapply(env):
    at = _app()
    at.file_uploader[0].upload("taxi.yaml", TAXI_YAML.read_bytes()).run()
    at.selectbox[0].set_value("nyc_taxi")
    at.text_input[0].set_value("analytics-team")
    at.button[0].click().run()
    assert catalog.get_policy(env, "nyc_taxi").version == 1
    assert any("version 1" in s.value for s in at.success)


def test_consumer_describe_is_free(env):
    catalog.apply_policy(env, TAXI_YAML.read_text())
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    at = _app()
    at.sidebar.radio[0].set_value("Consumer").run()
    at.text_input[0].set_value(key)
    at.text_area[0].set_value("DESCRIBE trips")
    at.button[0].click().run()
    assert not at.exception
    assert "fare_amount" in str(at.dataframe[0].value)
    assert [m.value for m in at.metric if m.label == "remaining ε"] == ["5"]


def test_consumer_revoked_between_two_runs_is_blocked(env):
    catalog.apply_policy(env, TAXI_YAML.read_text())
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    at = _app()
    at.sidebar.radio[0].set_value("Consumer").run()
    at.text_input[0].set_value(key)
    at.text_area[0].set_value("SELECT COUNT(*) FROM trips")
    at.button[0].click().run()
    assert not at.error
    catalog.revoke_consumer(env, "nyc_taxi", "analytics-team")
    at.button[0].click().run()
    assert [e.value for e in at.error] == ["We couldn't connect with this key."]
    assert any(c.value == "code: AuthError" for c in at.caption)


def test_example_buttons_fill_the_query_and_show_a_banner(env):
    catalog.apply_policy(env, TAXI_YAML.read_text())
    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    at = _app()
    at.sidebar.radio[0].set_value("Consumer").run()
    at.text_input[0].set_value(key)
    passing = next(b for b in at.button if b.label == "Trips per borough")
    passing.click().run()
    assert "GROUP BY z.borough" in at.text_area[0].value
    at.button[0].click().run()  # Run
    assert any(s.value.startswith("accepted") for s in at.success)
    blocked = next(b for b in at.button if b.label == "Hidden cross join")
    blocked.click().run()
    at.button[0].click().run()
    assert [e.value for e in at.error] == ["This SQL feature isn't supported."]
    assert any(c.value == "code: UnsupportedQuery" for c in at.caption)
