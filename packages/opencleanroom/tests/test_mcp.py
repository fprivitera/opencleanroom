# SPDX-License-Identifier: Apache-2.0
"""MCP tools through the in-memory FastMCP client, bound to a key from the environment (stdio path)."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb
import pytest
from cleanroom_sql.taxi import populate
from fastmcp import Client
from fastmcp.exceptions import ToolError
from opencleanroom import catalog
from opencleanroom.mcp import create_server

TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "taxi.duckdb"
    con = duckdb.connect(str(db))
    populate(con, n_drivers=100)
    con.close()
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "duckdb-dev", "duckdb", str(db))
    catalog.apply_policy(cat, TAXI_YAML)
    return cat


def call(server, name, **args):
    async def run():
        async with Client(server) as c:
            r = await c.call_tool(name, args)
            return r.data if r.data is not None else json.loads(r.content[0].text)
    return asyncio.run(run())


def test_agent_workflow_under_a_key(env, monkeypatch):
    monkeypatch.setenv("OPENCLEANROOM_KEY", catalog.create_consumer(env, "nyc_taxi", "analytics-team"))
    server = create_server(env, key_from_env=True)
    schema = call(server, "describe_schema")
    assert "trips" in schema["tables"] and "taxi_zones" in schema["tables"]
    keyset = [r[0] for r in call(server, "public_query", sql="SELECT DISTINCT borough FROM taxi_zones")["rows"]]
    assert "Manhattan" in keyset
    res = call(server, "dp_query", sql="SELECT COUNT(*) FROM trips", epsilon=0.2, request_id="mcp-1")
    assert res["epsilon_spent"] == 0.2 and res["request_id"] == "mcp-1" and res["remaining"] == pytest.approx(4.8)
    again = call(server, "dp_query", sql="SELECT COUNT(*) FROM trips", epsilon=0.2, request_id="mcp-1")
    assert again == res
    assert call(server, "budget")["remaining"] == pytest.approx(4.8)


def test_private_table_is_not_readable_in_clear(env, monkeypatch):
    monkeypatch.setenv("OPENCLEANROOM_KEY", catalog.create_consumer(env, "nyc_taxi", "analytics-team"))
    server = create_server(env, key_from_env=True)
    with pytest.raises(ToolError, match="^unsupported_query$"):
        call(server, "public_query", sql="SELECT * FROM trips LIMIT 1")


def test_keysets_come_from_the_policy_not_the_agent(env, monkeypatch):
    monkeypatch.setenv("OPENCLEANROOM_KEY", catalog.create_consumer(env, "nyc_taxi", "analytics-team"))
    with pytest.raises(ToolError):
        call(create_server(env, key_from_env=True), "dp_query", sql="SELECT COUNT(*) FROM trips", epsilon=0.1, keyset=["Manhattan"])


def test_errors_are_generic_codes(env, monkeypatch):
    monkeypatch.setenv("OPENCLEANROOM_KEY", catalog.create_consumer(env, "nyc_taxi", "counts-only"))
    server = create_server(env, key_from_env=True)
    with pytest.raises(ToolError, match="^policy_violation$"):
        call(server, "dp_query", sql="SELECT SUM(fare_amount) FROM trips", epsilon=0.1)
    with pytest.raises(ToolError, match="^policy_violation$"):
        call(server, "dp_query", sql="SELECT COUNT(*) FROM trips", epsilon=1e9)
    monkeypatch.setenv("OPENCLEANROOM_KEY", "nope")
    with pytest.raises(ToolError, match="^invalid_key$"):
        call(server, "budget")
    monkeypatch.delenv("OPENCLEANROOM_KEY")
    with pytest.raises(ToolError, match="^invalid_key$"):
        call(server, "describe_schema")


def test_http_transport_reads_the_bearer_header(env):
    import socket
    import threading
    import time

    import uvicorn
    from fastmcp.client.transports import StreamableHttpTransport

    key = catalog.create_consumer(env, "nyc_taxi", "analytics-team")
    app = create_server(env).http_app(path="/mcp")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.05)

        async def run(headers):
            async with Client(StreamableHttpTransport(f"http://127.0.0.1:{port}/mcp", headers=headers)) as c:
                return await c.call_tool("budget", {})
        assert asyncio.run(run({"Authorization": f"Bearer {key}"})).data["remaining"] == 5.0
        with pytest.raises(ToolError, match="^invalid_key$"):
            asyncio.run(run({"Authorization": "Bearer nope"}))
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def test_http_transport_never_falls_back_to_the_environment(env, monkeypatch):
    """An anonymous or mis-schemed HTTP client must not spend the key of the shell that started the server."""
    monkeypatch.setenv("OPENCLEANROOM_KEY", catalog.create_consumer(env, "nyc_taxi", "analytics-team"))
    server = create_server(env)  # the http transport: no key_from_env
    with pytest.raises(ToolError, match="^invalid_key$"):
        call(server, "budget")  # the in-memory client carries no HTTP request, like a header-less call


def test_argument_limits_and_rate_limit(env, monkeypatch):
    monkeypatch.setenv("OPENCLEANROOM_KEY", catalog.create_consumer(env, "nyc_taxi", "analytics-team"))
    server = create_server(env, key_from_env=True, rate="3/60")
    with pytest.raises(ToolError):
        call(server, "dp_query", sql="SELECT COUNT(*) FROM trips", epsilon="0.1")  # strings are not epsilons
    with pytest.raises(ToolError):
        call(server, "dp_query", sql="SELECT COUNT(*) FROM trips", epsilon=0.1, request_id="x" * 201)
    assert call(server, "budget")["remaining"] == 5.0  # argument errors cost no token: three calls left
    call(server, "budget")
    call(server, "budget")
    with pytest.raises(ToolError, match="^rate_limited$"):
        call(server, "budget")
