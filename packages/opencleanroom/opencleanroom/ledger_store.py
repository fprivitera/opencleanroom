# SPDX-License-Identifier: Apache-2.0
"""The persistent ledger: ε reservations in the catalog file, exact fractions, one transaction per step.

Same surface as `cleanroom_sql.Ledger` (budget, caps, rules, update_policy, spent,
spent_total, remaining, reserve, commit, release, charge_up_to), so the
executor does not know which one it holds. Every writer runs `BEGIN
IMMEDIATE` (ADR 0006). A reservation left behind by a crash stays
`reserved` and counts as spent until an admin releases it. A per-request
view stamps a request id on the rows it writes and keeps the audit row in
the same transactions.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction

from cleanroom_sql import BudgetExhaustedException, Policy, loads_policy


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")



@dataclass(frozen=True)
class Reconciliation:
    """One epoch: committed ledger ε against the ε the audit log says was charged; rows left open on either side."""
    epoch: str
    consumer: str
    ledger_committed: Fraction
    audit_charged: Fraction
    reserved_rows: int
    in_progress_rows: int

    @property
    def ok(self) -> bool:
        return self.ledger_committed == self.audit_charged


class RequestInFlight(ValueError):
    """Another worker holds the reservation for this request id."""

@dataclass(frozen=True)
class LedgerRow:
    id: int
    dataset: str
    epoch: str
    consumer: str
    epsilon: Fraction
    state: str
    kind: str
    request_id: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class AuditRow:
    id: int
    request_id: str | None
    dataset: str
    epoch: str
    consumer: str
    ts: str
    sql: str | None
    rewritten_sql: str | None
    epsilon_requested: Fraction
    epsilon_charged: Fraction
    sensitivities: str | None
    outcome: str
    error_code: str | None
    response: str | None


class SqliteLedger:
    def __init__(self, path: str | os.PathLike, dataset: str, epoch: str, policy: Policy,
                 request_id: str | None = None, sql: str | None = None, consumer: str | None = None, timeout: float = 5.0):
        self.path = path
        self.dataset = dataset
        self.epoch = epoch
        self.request_id = request_id
        self.sql = sql
        self.consumer = consumer
        self.timeout = timeout
        self.audit_id: int | None = None
        self._reserved: dict[int, str] = {}  # reservations this view made, id -> ε as text
        self._state: dict = {}  # budget, caps, rules: one dict shared by reference with every view
        self.update_policy(policy)

    # -- policy -----------------------------------------------------------------

    def update_policy(self, policy: Policy) -> None:
        """Budget, caps and rules follow the stored policy; spend lives in the file and is never touched."""
        if policy.dataset.epoch is not None and policy.dataset.epoch != self.epoch:
            raise ValueError(f"policy epoch {policy.dataset.epoch} differs from this ledger's epoch {self.epoch}: open a new ledger")
        self._state.update(budget=policy.dataset.budget, caps={name: c.cap for name, c in policy.consumers.items()},
                           rules=dict(policy.consumers))

    def _sync(self, con=None) -> None:
        """Follow the policy stored in the catalog file, so a re-apply reaches every open view, in every process."""
        def load(con):
            try:
                return con.execute("SELECT version, text FROM policies WHERE dataset = ?", (self.dataset,)).fetchone()
            except sqlite3.OperationalError:  # a ledger file without a catalog: the policy it was opened with stands
                return None
        if con is None:
            with self._db() as own:
                row = load(own)
        else:
            row = load(con)
        if row is not None and row[0] != self._state.get("version"):
            self.update_policy(loads_policy(row[1]))  # raises if the epoch changed: that session must be reopened
            self._state["version"] = row[0]

    @property
    def budget(self) -> Fraction:
        self._sync()
        return self._state["budget"]

    @property
    def caps(self) -> dict[str, Fraction | None]:
        self._sync()
        return self._state["caps"]

    @property
    def rules(self) -> dict:
        self._sync()
        return self._state["rules"]

    def for_request(self, request_id: str, sql: str, consumer: str | None = None) -> SqliteLedger:
        """A view of this ledger for one request: its rows carry the id, and an audit row follows them."""
        view = SqliteLedger.__new__(SqliteLedger)
        view.__dict__.update(self.__dict__)  # _state is shared by reference: a sync reaches every view
        view.request_id, view.sql, view.consumer, view.audit_id, view._reserved = request_id, sql, consumer, None, {}
        return view

    # -- connections ------------------------------------------------------------

    @contextmanager
    def _db(self, write: bool = False):
        con = sqlite3.connect(self.path, isolation_level=None, timeout=self.timeout)
        try:
            if write:
                con.execute("BEGIN IMMEDIATE")
            yield con
            if write:
                con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _sum(self, con, where: str, params: tuple) -> Fraction:
        rows = con.execute(f"SELECT epsilon FROM ledger WHERE dataset = ? AND epoch = ? AND state IN ('reserved', 'committed') {where}",
                           (self.dataset, self.epoch, *params)).fetchall()
        return sum((Fraction(r[0]) for r in rows), Fraction(0))

    # -- reads -----------------------------------------------------------------

    @property
    def spent_total(self) -> Fraction:
        with self._db() as con:
            return self._sum(con, "", ())

    def spent(self, consumer: str) -> Fraction:
        with self._db() as con:
            return self._sum(con, "AND consumer = ?", (consumer,))

    def remaining(self, consumer: str) -> Fraction:
        with self._db() as con:
            self._sync(con)
            return self._remaining(con, consumer)

    def _remaining(self, con, consumer: str) -> Fraction:
        left = self._state["budget"] - self._sum(con, "", ())  # the caller has synced the state on this connection
        cap = self._state["caps"].get(consumer)
        if cap is not None:
            left = min(left, cap - self._sum(con, "AND consumer = ?", (consumer,)))
        return left

    # -- writes -----------------------------------------------------------------

    def reserve(self, consumer: str, epsilon: Fraction) -> int:
        if not isinstance(epsilon, Fraction) or epsilon <= 0:
            raise ValueError(f"epsilon must be a positive Fraction, got {epsilon!r}")
        with self._db(write=True) as con:
            existing = None
            if self.request_id is not None:
                existing = con.execute(
                    "SELECT id, state FROM ledger WHERE dataset = ? AND consumer = ? AND request_id = ?",
                    (self.dataset, consumer, self.request_id)).fetchone()
                if existing is not None and existing[1] != "released":
                    raise RequestInFlight(f"request {self.request_id} already has a reservation for {consumer}")
            self._sync(con)
            remaining = self._remaining(con, consumer)
            if epsilon > remaining:
                raise BudgetExhaustedException(f"requested {epsilon}, remaining {remaining} for {consumer}")
            now = _now()
            if existing is not None:  # a released request is retried: reuse its row
                con.execute("UPDATE ledger SET epsilon = ?, state = 'reserved', epoch = ?, updated_at = ? WHERE id = ?",
                            (str(epsilon), self.epoch, now, existing[0]))
                row_id = existing[0]
            else:
                row_id = con.execute(
                    "INSERT INTO ledger (dataset, epoch, consumer, epsilon, delta, state, kind, request_id, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, '0', 'reserved', 'query', ?, ?, ?)",
                    (self.dataset, self.epoch, consumer, str(epsilon), self.request_id, now, now)).lastrowid
            if self.request_id is not None:
                self.audit_id = con.execute(
                    "INSERT INTO audit (request_id, dataset, epoch, consumer, ts, sql, epsilon_requested, epsilon_charged, outcome) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, '0', 'in_progress')",
                    (self.request_id, self.dataset, self.epoch, consumer, now, self.sql, str(epsilon))).lastrowid
            self._reserved[row_id] = str(epsilon)  # commit and release act only on the reservation this view made, at this ε
            return row_id

    _OPEN = "id = ? AND dataset = ? AND epoch = ? AND state = 'reserved' AND epsilon = ?"

    def _own(self, reservation: int) -> tuple:
        """Parameters that match the row only if it is still the reservation this view made: an admin may have released it."""
        epsilon = self._reserved.get(reservation)
        if epsilon is None:
            raise ValueError(f"reservation {reservation} was not made by this ledger view")
        return (reservation, self.dataset, self.epoch, epsilon)

    def commit(self, reservation: int) -> None:
        with self._db(write=True) as con:
            row = con.execute(f"SELECT consumer, epsilon FROM ledger WHERE {self._OPEN}", self._own(reservation)).fetchone()
            if row is None:
                raise ValueError(f"reservation {reservation} is not open in {self.dataset}/{self.epoch}")
            con.execute("UPDATE ledger SET state = 'committed', updated_at = ? WHERE id = ?", (_now(), reservation))
            self._add_charged(con, Fraction(row[1]))

    def release(self, reservation: int) -> None:
        with self._db(write=True) as con:
            n = con.execute(f"UPDATE ledger SET state = 'released', updated_at = ? WHERE {self._OPEN}",
                            (_now(), *self._own(reservation))).rowcount
            if n == 0:
                raise ValueError(f"reservation {reservation} is not open in {self.dataset}/{self.epoch}")

    def charge_up_to(self, consumer: str, amount: Fraction) -> None:
        """Spend `amount` more if it fits, else everything left: a failed query costs the per-query maximum."""
        if not isinstance(amount, Fraction):
            raise ValueError(f"amount must be a Fraction, got {amount!r}")
        with self._db(write=True) as con:
            self._sync(con)
            extra = min(amount, self._remaining(con, consumer))
            if extra <= 0:
                return
            now = _now()
            con.execute(
                "INSERT INTO ledger (dataset, epoch, consumer, epsilon, delta, state, kind, request_id, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, '0', 'committed', 'penalty', NULL, ?, ?)",
                (self.dataset, self.epoch, consumer, str(extra), now, now))
            self._add_charged(con, extra)

    def _add_charged(self, con, amount: Fraction) -> None:
        """Add to the audit row this view owns; views without a reservation have none."""
        if self.audit_id is None:
            return
        row = con.execute("SELECT epsilon_charged FROM audit WHERE id = ?", (self.audit_id,)).fetchone()
        if row is not None:
            con.execute("UPDATE audit SET epsilon_charged = ? WHERE id = ?", (str(Fraction(row[0]) + amount), self.audit_id))

    def finish(self, outcome: str, error_code: str | None, response: str | None,
               rewritten_sql: str | None = None, sensitivities: str | None = None) -> None:
        """Close the audit row of this request; a request that never reserved (rejected before the ledger) gets one here."""
        if self.request_id is None:
            return
        with self._db(write=True) as con:
            updated = 0
            if self.audit_id is not None:  # only the row this view opened: another consumer's or an admin's row is never touched
                updated = con.execute(
                    "UPDATE audit SET outcome = ?, error_code = ?, response = ?, rewritten_sql = COALESCE(?, rewritten_sql), "
                    "sensitivities = COALESCE(?, sensitivities) WHERE id = ? AND outcome = 'in_progress'",
                    (outcome, error_code, response, rewritten_sql, sensitivities, self.audit_id)).rowcount
            if updated == 0:  # rejected before any reservation (or finished twice): still one audit row per request
                self.audit_id = con.execute(
                    "INSERT INTO audit (request_id, dataset, epoch, consumer, ts, sql, rewritten_sql, sensitivities, epsilon_requested, epsilon_charged, "
                    "outcome, error_code, response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '0', '0', ?, ?, ?)",
                    (self.request_id, self.dataset, self.epoch, self.consumer or "", _now(), self.sql, rewritten_sql, sensitivities,
                     outcome, error_code, response)).lastrowid

    # -- lookups and admin ------------------------------------------------------

    def lookup(self, request_id: str, consumer: str) -> tuple[str, str | None] | None:
        """("committed", response) for a finished request, ("failed", error_code) for one the backend failed,
        ("reserved", None) for one in flight, None if unknown."""
        with self._db() as con:
            row = con.execute("SELECT state FROM ledger WHERE dataset = ? AND consumer = ? AND request_id = ? AND kind = 'query'",
                              (self.dataset, consumer, request_id)).fetchone()
            if row is None or row[0] == "released":
                return None  # unknown, or released before execution: the request may run
            if row[0] == "reserved":
                return ("reserved", None)
            audit = con.execute("SELECT outcome, error_code, response FROM audit WHERE request_id = ? AND dataset = ? AND consumer = ? "
                                "AND outcome != 'in_progress' ORDER BY id DESC LIMIT 1", (request_id, self.dataset, consumer)).fetchone()
            if audit is None:
                return ("committed", None)  # crashed between commit and finish: reconcile lists it
            return ("committed", audit[2]) if audit[0] == "ok" else ("failed", audit[1])

    def reserved_rows(self) -> list[LedgerRow]:
        with self._db() as con:
            rows = con.execute("SELECT id, dataset, epoch, consumer, epsilon, state, kind, request_id, created_at, updated_at FROM ledger "
                               "WHERE dataset = ? AND state = 'reserved' ORDER BY id", (self.dataset,)).fetchall()  # every epoch
        return [LedgerRow(r[0], r[1], r[2], r[3], Fraction(r[4]), *r[5:]) for r in rows]

    def release_reservation(self, ledger_id: int, reason: str) -> None:
        """Admin: free a reservation a crashed worker left behind, with a written reason in the audit log."""
        with self._db(write=True) as con:
            row = con.execute("SELECT consumer, epsilon, request_id FROM ledger WHERE id = ? AND state = 'reserved' AND dataset = ?",
                              (ledger_id, self.dataset)).fetchone()
            if row is None:
                raise ValueError(f"ledger row {ledger_id} is not a reserved row of {self.dataset}")
            con.execute("UPDATE ledger SET state = 'released', updated_at = ? WHERE id = ?", (_now(), ledger_id))
            con.execute("UPDATE audit SET outcome = 'released_by_admin' WHERE request_id = ? AND dataset = ? AND consumer = ? AND outcome = 'in_progress'",
                        (row[2], self.dataset, row[0]))
            con.execute(
                "INSERT INTO audit (request_id, dataset, epoch, consumer, ts, sql, epsilon_requested, epsilon_charged, outcome, error_code, response) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, '0', 'released_by_admin', NULL, ?)",
                (row[2], self.dataset, self.epoch, row[0], _now(), None, str(Fraction(row[1])), reason))

    def reconcile(self) -> list[Reconciliation]:
        """Per epoch and consumer of this dataset: committed ledger ε (queries and penalties) versus audit `epsilon_charged`.

        Exact sums in Python: the columns hold fractions as text. Reserved ledger rows and `in_progress` audit rows are
        counted, not summed; they are the rows an admin looks at after a crash.
        """
        with self._db() as con:
            keys = sorted({tuple(r) for r in con.execute(
                "SELECT epoch, consumer FROM ledger WHERE dataset = ? UNION SELECT epoch, consumer FROM audit WHERE dataset = ?",
                (self.dataset, self.dataset))})
            out = []
            for epoch, consumer in keys:
                args = (self.dataset, epoch, consumer)
                committed = sum((Fraction(e) for (e,) in con.execute(
                    "SELECT epsilon FROM ledger WHERE dataset = ? AND epoch = ? AND consumer = ? AND state = 'committed'", args)), Fraction(0))
                charged = sum((Fraction(e) for (e,) in con.execute(
                    "SELECT epsilon_charged FROM audit WHERE dataset = ? AND epoch = ? AND consumer = ?", args)), Fraction(0))
                reserved = con.execute("SELECT COUNT(*) FROM ledger WHERE dataset = ? AND epoch = ? AND consumer = ? AND state = 'reserved'",
                                       args).fetchone()[0]
                stuck = con.execute("SELECT COUNT(*) FROM audit WHERE dataset = ? AND epoch = ? AND consumer = ? AND outcome = 'in_progress'",
                                    args).fetchone()[0]
                out.append(Reconciliation(epoch, consumer, committed, charged, reserved, stuck))
        return out

    def audit_rows(self) -> list[AuditRow]:
        with self._db() as con:
            rows = con.execute(
                "SELECT id, request_id, dataset, epoch, consumer, ts, sql, rewritten_sql, epsilon_requested, epsilon_charged, "
                "sensitivities, outcome, error_code, response FROM audit WHERE dataset = ? ORDER BY id", (self.dataset,)).fetchall()
        return [AuditRow(*r[:8], Fraction(r[8]), Fraction(r[9]), *r[10:]) for r in rows]
