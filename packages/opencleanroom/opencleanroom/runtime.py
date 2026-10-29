# SPDX-License-Identifier: Apache-2.0
"""A key becomes a Session over the persistent ledger: stored policy, registered backend, one request id per query.

Every `Runtime` on one catalog file shares the budget through the SQLite
ledger (ADR 0006); a policy re-applied inside the same epoch keeps the
spent ε (ADR 0004). A replayed request id returns the stored response
and spends nothing; an in-flight id is a conflict.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import threading
import uuid
from fractions import Fraction
from typing import Any

from cleanroom_sql import (
    BudgetExhaustedException,
    Policy,
    PolicyViolation,
    QueryFailed,
    Result,
    Rewriter,
    Session,
    UnsupportedQuery,
    loads_policy,
)
from cleanroom_sql.session import is_metadata_query

from . import catalog
from .backends import Backend, open_backend
from .ledger_store import AuditRow, LedgerRow, Reconciliation, RequestInFlight, SqliteLedger


class AuthError(Exception):
    """One generic error for unknown and revoked keys alike: the text reveals nothing."""


class InProgress(Exception):
    """A request with this id is still reserved (in flight or left by a crash): retry later or ask an admin."""


ERROR_CODES = {
    UnsupportedQuery: "unsupported_query", PolicyViolation: "policy_violation", BudgetExhaustedException: "budget_exhausted",
    QueryFailed: "query_failed", ValueError: "bad_request",
}


def error_code(exc: BaseException) -> str:
    for cls, code in ERROR_CODES.items():  # PolicyViolation before ValueError: dict order matters
        if isinstance(exc, cls):
            return code
    return "internal"


def _to_json(res: Result) -> str:
    return json.dumps({
        "rows": [list(r) for r in res.rows], "rewritten_sql": res.rewritten_sql, "epsilon_spent": res.epsilon_spent,
        "epsilon_per_measurement": res.epsilon_per_measurement, "sensitivities": list(res.sensitivities),
        "ci95_halfwidth": list(res.ci95_halfwidth), "request_id": res.request_id,
    }, default=str)  # dates and decimals from the backend become their text; a replay returns the same text


def _from_json(text: str) -> Result:
    d = json.loads(text)
    return Result(rows=[tuple(r) for r in d["rows"]], rewritten_sql=d["rewritten_sql"], epsilon_spent=d["epsilon_spent"],
                  epsilon_per_measurement=d["epsilon_per_measurement"], sensitivities=tuple(d["sensitivities"]),
                  ci95_halfwidth=tuple(d["ci95_halfwidth"]), request_id=d["request_id"])


class Runtime:
    def __init__(self, catalog_path: str | os.PathLike):
        self.catalog = catalog_path
        self._policies: dict[tuple[str, int], Policy] = {}
        self._backends: dict[str, Backend] = {}
        self._lock = threading.Lock()

    # -- consumer paths ---------------------------------------------------------

    def query(self, key: str, sql: str, epsilon: float | None = None, request_id: str | None = None) -> Result | list[tuple[Any, ...]]:
        """Resolve the key, run one statement under its policy on the shared ledger, record it in the audit log.

        Metadata statements need no ε and are not recorded in the ledger. A
        `request_id` makes the call idempotent: a committed id returns the stored
        response, a reserved one raises `InProgress`.
        """
        dataset, consumer, policy, ledger, backend = self._bind(key)
        meta = is_metadata_query(sql)
        session = Session.from_policy(policy, consumer, backend.session_con(), ledger=ledger)  # metadata: no backend read
        if meta is not None:
            kind, table = meta
            return session.describe(table) if kind == "describe" else session.tables()
        rid = request_id or str(uuid.uuid4())
        hit = ledger.lookup(rid, consumer)
        if hit is not None:
            state, response = hit
            if state == "reserved" or response is None:
                raise InProgress(f"request {rid} is still in progress")
            if state == "failed":  # already charged; a replay does not run the backend again
                raise QueryFailed("the backend could not run the query")
            return _from_json(response)
        view = ledger.for_request(rid, sql, consumer)
        session = Session.from_policy(policy, consumer, backend.session_con(), ledger=view,
                                      materialized=self._materialized(dataset, policy, ledger.epoch))
        if epsilon is None:
            view.finish("bad_request", "bad_request", None)
            raise ValueError("epsilon is required for a SELECT")
        try:
            res = session.query(sql, epsilon=epsilon)
        except RequestInFlight:  # lost the race for this id against another worker: nothing charged here
            raise InProgress(f"request {rid} is still in progress") from None
        except Exception as e:
            view.finish(error_code(e), error_code(e), None)
            raise
        res = dataclasses.replace(res, request_id=rid)
        view.finish("ok", None, _to_json(res), rewritten_sql=res.rewritten_sql, sensitivities=json.dumps(list(res.sensitivities)))
        return res

    def public_query(self, key: str, sql: str) -> list[tuple[Any, ...]]:
        """Rows from the dataset's public tables, through the rewriter's whitelist, capped, no ε (keysets and lookups)."""
        dataset, consumer, policy, ledger, backend = self._bind(key)
        granted = policy.consumers[consumer].tables  # the grant list applies to public tables too: one rule for every path
        public = {n: t for n, t in policy.dataset.schema.tables.items() if t.is_public and n in granted}
        from cleanroom_sql import Schema  # local import keeps the module header small
        checked = Rewriter(Schema(tables=public), k=policy.dataset.k, dialect=policy.dataset.dialect).check_public(sql)
        rid = str(uuid.uuid4())
        view = ledger.for_request(rid, sql, consumer)
        try:
            rows = backend.session_con().execute(checked).fetchall()
        except Exception:
            view.finish("query_failed", "query_failed", None)
            raise QueryFailed("the backend could not run the query") from None
        view.finish("ok", None, json.dumps({"rows": [list(r) for r in rows]}, default=str), rewritten_sql=checked)
        return rows

    def resolves(self, key: str) -> bool:
        return catalog.resolve_key(self.catalog, key) is not None

    def remaining_for_key(self, key: str) -> Fraction:
        """What the key's consumer may still spend under its own cap; AuthError for an unknown or revoked key.

        The dataset's shared remainder is not shown here: it would tell one consumer when the others run queries.
        A reservation beyond it still fails with `budget_exhausted`.
        """
        resolved = catalog.resolve_key(self.catalog, key)
        if resolved is None:
            raise AuthError("invalid key")
        dataset, consumer = resolved
        _, ledger = self._policy_and_ledger(dataset)
        cap = ledger.caps.get(consumer)
        return self.remaining(dataset, consumer) if cap is None else cap - ledger.spent(consumer)

    def remaining(self, dataset: str, consumer: str) -> Fraction:
        _, ledger = self._policy_and_ledger(dataset)
        return ledger.remaining(consumer)

    def budgets(self, dataset: str) -> dict[str, Fraction]:
        _, ledger = self._policy_and_ledger(dataset)
        return {"budget": ledger.budget, "spent": ledger.spent_total, "remaining": ledger.budget - ledger.spent_total}

    def audit(self, dataset: str) -> list[AuditRow]:
        _, ledger = self._policy_and_ledger(dataset)
        return ledger.audit_rows()

    def reconcile(self, dataset: str) -> list[Reconciliation]:
        _, ledger = self._policy_and_ledger(dataset)
        return ledger.reconcile()

    def reserved(self, dataset: str) -> list[LedgerRow]:
        _, ledger = self._policy_and_ledger(dataset)
        return ledger.reserved_rows()

    def release_reservation(self, ledger_id: int, reason: str) -> None:
        """Admin: free the reservation a crashed worker left; the reason lands in the audit log."""
        dataset = catalog.ledger_dataset(self.catalog, ledger_id)
        if dataset is None:
            raise ValueError(f"ledger row {ledger_id} does not exist")
        _, ledger = self._policy_and_ledger(dataset)
        ledger.release_reservation(ledger_id, reason)

    def open_session(self, key: str) -> Session:
        """A session for a key, bound to the shared ledger. Revocation is checked here, at open time; prefer `query`."""
        dataset, consumer, policy, ledger, backend = self._bind(key)
        return Session.from_policy(policy, consumer, backend.session_con(), ledger=ledger,
                                   materialized=self._materialized(dataset, policy, ledger.epoch))

    # -- internals ----------------------------------------------------------

    def _materialized(self, dataset: str, policy: Policy, epoch: str) -> dict[str, str]:
        """Truncated table -> its materialized table, only for a record whose name and definition match the current policy.

        The definition hash covers k, order, privacy id and the column list; the name is recomputed, so an edited
        record cannot point the rewriter at another table. Spark only: DuckDB files are read-only.
        """
        from .materialize import definition_sha256, physical_name
        ds = policy.dataset
        if ds.dialect != "spark":
            return {}
        out = {}
        for m in catalog.materializations(self.catalog, dataset):
            table = ds.schema.tables.get(m.tbl)
            if (table is not None and table.is_truncated and m.backend == ds.backend and m.epoch == epoch and m.k == ds.k
                    and m.physical_name == physical_name(m.tbl, m.k, m.epoch, m.snapshot)
                    and m.definition_sha256 == definition_sha256(table, ds.k)):
                out[m.tbl] = m.physical_name
        return out

    def _bind(self, key: str):
        resolved = catalog.resolve_key(self.catalog, key)
        if resolved is None:
            raise AuthError("invalid key")
        dataset, consumer = resolved
        policy, ledger = self._policy_and_ledger(dataset)
        backend = self._backend(policy.dataset.backend)
        if backend.dialect != policy.dataset.dialect:  # apply refuses this; a hand-edited catalog must still fail closed
            raise RuntimeError(f"policy of {dataset} is written for {policy.dataset.dialect}, its backend runs {backend.dialect}")
        return dataset, consumer, policy, ledger, backend

    def _policy_and_ledger(self, dataset: str) -> tuple[Policy, SqliteLedger]:
        stored = catalog.get_policy(self.catalog, dataset)
        if stored is None:
            raise RuntimeError(f"no policy applied for dataset {dataset}")
        if hashlib.sha256(stored.text.encode()).hexdigest() != stored.sha256:  # integrity check: the catalog file was edited by hand
            raise RuntimeError(f"policy text of {dataset} does not match its hash; the catalog was modified outside opencleanroom")
        with self._lock:
            policy = self._policies.get((dataset, stored.version))
            if policy is None:
                policy = self._policies[(dataset, stored.version)] = loads_policy(stored.text)
        # The ledger is the file: a new view per call reads the stored policy's budget and caps and the current spend.
        return policy, SqliteLedger(self.catalog, dataset, stored.epoch, policy)

    def _backend(self, name: str) -> Backend:
        with self._lock:
            backend = self._backends.get(name)
        if backend is not None:
            return backend
        row = catalog.get_backend(self.catalog, name)
        if row is None:
            raise RuntimeError(f"backend {name} is no longer registered")
        opened = open_backend(row.kind, row.location, row.query_timeout)  # outside the lock: a slow server stalls only its own datasets
        with self._lock:
            return self._backends.setdefault(name, opened)
