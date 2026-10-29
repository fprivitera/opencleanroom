# SPDX-License-Identifier: Apache-2.0
"""HTTP JSON API over the Runtime: Bearer keys, one error code per failure class, idempotent replay, rate limit.

Errors carry a code and a generic message, never backend text: a backend error can carry data.
"""
from __future__ import annotations

import hashlib
import json
import math
import os

from cleanroom_sql import BudgetExhaustedException, PolicyViolation, QueryFailed, Result, UnsupportedQuery
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from . import catalog
from .jsonable import jsonable as _jsonable
from .ratelimit import GLOBAL, RateLimiter
from .runtime import AuthError, InProgress, Runtime

MAX_BODY = 1_000_000  # bytes; one SQL statement and a number never need more

ERRORS: dict[type, tuple[int, str, str]] = {  # checked in order: ValueError stays last, it is the broadest class
    AuthError: (401, "invalid_key", "unknown or revoked key"),
    UnsupportedQuery: (400, "unsupported_query", "the query uses something the clean room does not support"),
    PolicyViolation: (400, "policy_violation", "the query is outside your policy"),
    BudgetExhaustedException: (429, "budget_exhausted", "not enough epsilon left"),
    InProgress: (409, "in_progress", "a request with this idempotency key is still running"),
    QueryFailed: (502, "query_failed", "the backend could not run the query"),
    ValueError: (400, "bad_request", "the request body is not valid"),
}


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse({"error": code, "message": message}, status_code=status)


def create_app(catalog_path: str | os.PathLike, rate: str = "60/60") -> Starlette:
    runtime = Runtime(catalog_path)
    limiter = RateLimiter(rate)

    def guarded(handler):
        """Rate limit, resolve the key, run `handler(key, headers, body)` in the threadpool; map every exception to one code."""

        async def endpoint(request: Request):
            try:
                header = request.headers.get("authorization", "")
                key = header[7:] if header.startswith("Bearer ") else ""
                known = bool(key) and await run_in_threadpool(catalog.resolve_key, catalog_path, key) is not None
                bucket = hashlib.sha256(key.encode()).hexdigest() if known else GLOBAL
                if not limiter.allow(bucket):
                    return _error(429, "rate_limited", "too many requests")
                if not known:
                    return _error(*ERRORS[AuthError])
                body = b""
                async for chunk in request.stream():
                    body += chunk
                    if len(body) > MAX_BODY:
                        return _error(413, "bad_request", "the request body is too large")
                return JSONResponse(await run_in_threadpool(handler, key, request.headers, body))
            except Exception as e:
                for cls, (status, code, message) in ERRORS.items():
                    if isinstance(e, cls):
                        return _error(status, code, message)
                return _error(500, "internal", "internal error")
        return endpoint

    def query(key: str, headers, raw: bytes) -> dict:
        try:
            body = json.loads(raw)
        except ValueError:
            raise ValueError("body is not JSON") from None
        if not isinstance(body, dict) or not isinstance(body.get("sql"), str):
            raise ValueError("body must be a JSON object with a sql string")
        epsilon = body.get("epsilon")
        if epsilon is not None:
            try:
                finite = not isinstance(epsilon, bool) and isinstance(epsilon, (int, float)) and math.isfinite(epsilon)
            except OverflowError:  # an integer too large for a float
                finite = False
            if not finite:
                raise ValueError("epsilon must be a finite number")
        from_header, from_body = headers.get("idempotency-key"), body.get("idempotency_key")
        if from_header is not None and from_body is not None and from_header != from_body:
            raise ValueError("Idempotency-Key header and idempotency_key body disagree")
        rid = from_header if from_header is not None else from_body
        if rid is not None and (not isinstance(rid, str) or not 1 <= len(rid) <= 200):
            raise ValueError("idempotency key must be a short string")
        res = runtime.query(key, body["sql"], epsilon=epsilon, request_id=rid)
        if not isinstance(res, Result):  # metadata: rows only, no ε
            return {"rows": _jsonable(res)}
        return {
            "rows": _jsonable(res.rows), "rewritten_sql": res.rewritten_sql, "epsilon_spent": res.epsilon_spent,
            "epsilon_per_measurement": res.epsilon_per_measurement, "sensitivities": list(res.sensitivities),
            "ci95_halfwidth": list(res.ci95_halfwidth), "request_id": res.request_id,
            "remaining": float(runtime.remaining_for_key(key)),
        }

    def schema(key: str, headers, raw: bytes) -> dict:
        session = runtime.open_session(key)
        return {"tables": {name: {"kind": kind, "columns": [{"column": c, "type": t, "clamp": cl} for c, t, cl in session.describe(name)]}
                           for name, kind in session.tables()}}

    def budget(key: str, headers, raw: bytes) -> dict:
        return {"remaining": float(runtime.remaining_for_key(key))}

    async def healthz(request: Request):
        return JSONResponse({"ok": True})

    return Starlette(routes=[
        Route("/v1/query", guarded(query), methods=["POST"]),
        Route("/v1/schema", guarded(schema), methods=["GET"]),
        Route("/v1/budget", guarded(budget), methods=["GET"]),
        Route("/healthz", healthz, methods=["GET"]),
    ])
