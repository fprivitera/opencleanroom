# SPDX-License-Identifier: Apache-2.0
"""MCP server on the Runtime: four tools bound to the key's policy, the same rules as HTTP.

Key: `Authorization: Bearer <key>` on the HTTP transport; on stdio, and only
there, `OPENCLEANROOM_KEY` of the server process (one process, one agent).
Tool errors carry the generic code only: a backend error can carry data.
"""
from __future__ import annotations

import hashlib
import os
from typing import Annotated, Any

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_http_headers
from pydantic import Field

from .api import ERRORS, _jsonable
from .ratelimit import GLOBAL, RateLimiter
from .runtime import AuthError, Result, Runtime


def _code(exc: BaseException) -> str:
    for cls, (_, code, _) in ERRORS.items():
        if isinstance(exc, cls):
            return code
    return "internal"


def create_server(catalog_path: str | os.PathLike, key_from_env: bool = False, rate: str = "60/60") -> FastMCP:
    """`key_from_env` is for the stdio transport only: an HTTP client never inherits the server's environment."""
    runtime = Runtime(catalog_path)
    limiter = RateLimiter(rate)
    mcp = FastMCP("opencleanroom", mask_error_details=True)

    def key() -> str:
        if key_from_env:
            k = os.environ.get("OPENCLEANROOM_KEY", "")
        else:
            auth = get_http_headers(include={"authorization"}).get("authorization", "")
            k = auth[7:] if auth.startswith("Bearer ") else ""
        if not k:
            raise AuthError("no key")
        return k

    def guarded(fn):
        try:
            k = key()
            if not limiter.allow(hashlib.sha256(k.encode()).hexdigest() if runtime.resolves(k) else GLOBAL):
                raise ToolError("rate_limited")
            return fn(k)
        except ToolError:
            raise
        except Exception as e:
            raise ToolError(_code(e)) from None

    @mcp.tool
    def describe_schema() -> dict[str, Any]:
        """Tables and columns your policy lets you read, with clamping bounds; free. Public tables can be read in full with public_query."""
        def run(k):
            session = runtime.open_session(k)
            return {"tables": {name: {"kind": kind, "columns": [{"column": c, "type": t, "clamp": cl} for c, t, cl in session.describe(name)]}
                               for name, kind in session.tables()}}
        return guarded(run)

    @mcp.tool
    def public_query(sql: str) -> dict[str, Any]:
        """Rows of public tables only (keysets, lookups): no noise, no budget, capped. Example: SELECT DISTINCT borough FROM taxi_zones."""
        return guarded(lambda k: {"rows": _jsonable(runtime.public_query(k, sql))})

    @mcp.tool
    def dp_query(sql: str, epsilon: Annotated[float, Field(strict=True)],
                 request_id: Annotated[str | None, Field(min_length=1, max_length=200)] = None) -> dict[str, Any]:
        """Run an aggregate under differential privacy and spend epsilon. GROUP BY keys come from the policy's keysets, never from you.

        Pass the same request_id to retry a dropped call without spending again.
        """
        def run(k):
            res = runtime.query(k, sql, epsilon=epsilon, request_id=request_id)
            if not isinstance(res, Result):
                return {"rows": _jsonable(res)}
            return {
                "rows": _jsonable(res.rows), "rewritten_sql": res.rewritten_sql, "epsilon_spent": res.epsilon_spent,
                "epsilon_per_measurement": res.epsilon_per_measurement, "sensitivities": list(res.sensitivities),
                "ci95_halfwidth": list(res.ci95_halfwidth), "request_id": res.request_id,
                "remaining": float(runtime.remaining_for_key(k)),
            }
        return guarded(run)

    @mcp.tool
    def budget() -> dict[str, Any]:
        """Your remaining epsilon; free."""
        return guarded(lambda k: {"remaining": float(runtime.remaining_for_key(k))})

    return mcp
