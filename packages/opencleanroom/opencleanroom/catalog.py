# SPDX-License-Identifier: Apache-2.0
"""The catalog: one SQLite file holding backends, applied policies, consumers and hashed API keys.

Plain functions over `sqlite3` (ADR 0006); the ledger and the audit log live in
the same file (`ledger_store.py`). Policy applies follow ADR 0004: inside one budget epoch the
dataset budget can be lowered, never raised.
"""
from __future__ import annotations

import hashlib
import os
import re
import secrets
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction

from cleanroom_sql import PolicyError, loads_policy

KEY_PREFIX = "ocr_"

SCHEMA = """
CREATE TABLE IF NOT EXISTS backends (
    name TEXT PRIMARY KEY, kind TEXT NOT NULL, location TEXT NOT NULL, created_at TEXT NOT NULL,
    query_timeout REAL NOT NULL DEFAULT 30);
CREATE TABLE IF NOT EXISTS policies (
    dataset TEXT PRIMARY KEY, version INTEGER NOT NULL, epoch TEXT NOT NULL, budget_epsilon TEXT NOT NULL,
    text TEXT NOT NULL, sha256 TEXT NOT NULL, applied_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS policy_history (
    dataset TEXT NOT NULL, version INTEGER NOT NULL, epoch TEXT NOT NULL, budget_epsilon TEXT NOT NULL,
    text TEXT NOT NULL, sha256 TEXT NOT NULL, applied_at TEXT NOT NULL, PRIMARY KEY (dataset, version));
CREATE TABLE IF NOT EXISTS consumers (
    id INTEGER PRIMARY KEY, dataset TEXT NOT NULL, name TEXT NOT NULL, key_sha256 TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL, revoked_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS consumers_active ON consumers(dataset, name) WHERE revoked_at IS NULL;
CREATE TABLE IF NOT EXISTS ledger (
    id INTEGER PRIMARY KEY, dataset TEXT NOT NULL, epoch TEXT NOT NULL, consumer TEXT NOT NULL,
    epsilon TEXT NOT NULL, delta TEXT NOT NULL DEFAULT '0',
    state TEXT NOT NULL CHECK (state IN ('reserved', 'committed', 'released')),
    kind TEXT NOT NULL DEFAULT 'query' CHECK (kind IN ('query', 'penalty')),
    request_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS ledger_request ON ledger(dataset, consumer, request_id) WHERE request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ledger_epoch ON ledger(dataset, epoch, state);
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY, request_id TEXT, dataset TEXT NOT NULL, epoch TEXT NOT NULL, consumer TEXT NOT NULL, ts TEXT NOT NULL,
    sql TEXT, rewritten_sql TEXT, epsilon_requested TEXT NOT NULL, epsilon_charged TEXT NOT NULL,
    sensitivities TEXT, outcome TEXT NOT NULL, error_code TEXT, response TEXT);
CREATE INDEX IF NOT EXISTS audit_request ON audit(dataset, request_id);
CREATE TABLE IF NOT EXISTS epoch_log (
    dataset TEXT NOT NULL, epoch TEXT NOT NULL, snapshot TEXT NOT NULL, reason TEXT NOT NULL, ts TEXT NOT NULL,
    PRIMARY KEY (dataset, epoch));
CREATE TABLE IF NOT EXISTS materializations (
    dataset TEXT NOT NULL, tbl TEXT NOT NULL, backend TEXT NOT NULL, epoch TEXT NOT NULL, snapshot TEXT NOT NULL,
    k INTEGER NOT NULL, order_by TEXT NOT NULL, privacy_id TEXT NOT NULL, physical_name TEXT NOT NULL, created_at TEXT NOT NULL,
    definition_sha256 TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (dataset, tbl, epoch, k));
"""


@dataclass(frozen=True)
class BackendRow:
    name: str
    kind: str
    location: str
    created_at: str
    query_timeout: float  # seconds a backend query may run before it is interrupted


@dataclass(frozen=True)
class StoredPolicy:
    dataset: str
    version: int
    epoch: str
    budget: Fraction
    text: str
    sha256: str
    applied_at: str

    @classmethod
    def _from_row(cls, row: tuple) -> StoredPolicy:
        dataset, version, epoch, budget, text, sha, applied = row
        return cls(dataset, version, epoch, Fraction(budget), text, sha, applied)


@dataclass(frozen=True)
class AppliedPolicy:
    dataset: str
    version: int
    epoch: str


@dataclass(frozen=True)
class EpochRow:
    dataset: str
    epoch: str
    snapshot: str
    reason: str
    ts: str


@dataclass(frozen=True)
class MaterializationRow:
    """A materialized table opencleanroom created itself; only these are ever read in place of the truncation window."""
    dataset: str
    tbl: str
    backend: str
    epoch: str
    snapshot: str
    k: int
    order_by: str
    privacy_id: str
    physical_name: str
    created_at: str
    definition_sha256: str  # SHA-256 of the SELECT it was created from: k, order, privacy id and columns in one value


@dataclass(frozen=True)
class ConsumerRow:
    dataset: str
    name: str
    created_at: str
    revoked_at: str | None


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@contextmanager
def _db(path: str | os.PathLike, write: bool = False, timeout: float = 5.0):
    con = sqlite3.connect(path, isolation_level=None, timeout=timeout)
    try:
        if write:
            con.execute("BEGIN IMMEDIATE")  # one writer at a time (ADR 0006); a second writer waits, then gets "database is locked"
        yield con
        if write:
            con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


# -- init and backends ------------------------------------------------------

def init(path: str | os.PathLike) -> None:
    """Create the catalog, or bring an older one up to date: `opencleanroom init` is safe to re-run."""
    with _db(path) as con:
        con.executescript(SCHEMA)
        if "query_timeout" not in {r[1] for r in con.execute("PRAGMA table_info(backends)")}:
            con.execute("ALTER TABLE backends ADD COLUMN query_timeout REAL NOT NULL DEFAULT 30")
        if "definition_sha256" not in {r[1] for r in con.execute("PRAGMA table_info(materializations)")}:
            con.execute("ALTER TABLE materializations ADD COLUMN definition_sha256 TEXT NOT NULL DEFAULT ''")  # '' never matches


def add_backend(path, name: str, kind: str, location: str, timeout: float = 5.0, query_timeout: float = 30.0) -> None:
    from .backends import checked_timeout  # one rule for the catalog and for the backend that reads it
    query_timeout = checked_timeout(query_timeout)
    if kind not in ("duckdb", "spark"):
        raise ValueError(f"unknown backend kind {kind!r}; supported: duckdb, spark")
    if kind == "spark" and not re.fullmatch(r"sc://[^/;:\s]+(:\d+)?/?(;[^;\s]*)*", location):
        raise ValueError(f"a Spark backend needs a Spark Connect URL like sc://host:15002, got {location!r}")
    with _db(path, write=True, timeout=timeout) as con:
        if con.execute("SELECT 1 FROM backends WHERE name = ?", (name,)).fetchone():
            raise ValueError(f"backend {name} is already registered")
        con.execute("INSERT INTO backends (name, kind, location, created_at, query_timeout) VALUES (?, ?, ?, ?, ?)",
                    (name, kind, location, _now(), query_timeout))


_TOKEN = re.compile(r"(;token=)[^;]*")


def redact_location(location: str) -> str:
    """A location fit to print: a Spark Connect token in the URL becomes ***."""
    return _TOKEN.sub(r"\1***", location)


def list_backends(path) -> list[BackendRow]:
    with _db(path) as con:
        return [BackendRow(*r) for r in con.execute("SELECT name, kind, location, created_at, query_timeout FROM backends ORDER BY name")]


def get_backend(path, name: str) -> BackendRow | None:
    with _db(path) as con:
        row = con.execute("SELECT name, kind, location, created_at, query_timeout FROM backends WHERE name = ?", (name,)).fetchone()
    return BackendRow(*row) if row else None


# -- policies ---------------------------------------------------------------

def apply_policy(path, text: str) -> AppliedPolicy:
    """Validate like `cleanroom-sql policy check`, then the catalog rules, then store; every fault is reported with its path."""
    policy = loads_policy(text)  # raises PolicyError with paths
    ds = policy.dataset
    errors: list[tuple[str, str]] = []
    epoch = ds.epoch.strip() if ds.epoch is not None else None
    if not epoch:
        errors.append(("dataset.budget.epoch", "a budget epoch is required to apply a policy (ADR 0004)"))
    with _db(path, write=True) as con:
        kind = con.execute("SELECT kind FROM backends WHERE name = ?", (ds.backend,)).fetchone() if ds.backend else None
        if kind is None:
            errors.append(("dataset.backend", f"{ds.backend!r} is not a registered backend"))
        elif ds.dialect != kind[0]:  # the rewriter prints SQL for the policy's dialect: it must be the backend's
            errors.append(("dataset.dialect", f"dialect {ds.dialect!r} does not match backend {ds.backend} ({kind[0]}); set dialect: {kind[0]}"))
        current = con.execute("SELECT version, epoch FROM policies WHERE dataset = ?", (ds.name,)).fetchone()
        epoch_ok = True
        if epoch and not EPOCH_LABEL.fullmatch(epoch):
            epoch_ok = False
            errors.append(("dataset.budget.epoch", f"epoch label {epoch!r} may use letters, digits, '.', '_' and '-' only; labels compare as text"))
        if current is not None and epoch_ok and epoch and epoch != current[1]:
            # A renewal is a written decision (ADR 0004): the label must be recorded first, and never move backwards.
            if epoch < current[1]:
                epoch_ok = False
                errors.append(("dataset.budget.epoch",
                               f"epoch {epoch} is older than the current epoch {current[1]}; the policy file is behind the catalog (ADR 0004)"))
            elif con.execute("SELECT 1 FROM epoch_log WHERE dataset = ? AND epoch = ?", (ds.name, epoch)).fetchone() is None:
                epoch_ok = False
                errors.append(("dataset.budget.epoch",
                               f"epoch {epoch} is not recorded: run `opencleanroom budget new-epoch {ds.name} --epoch {epoch} "
                               f"--snapshot <id> --reason <text>` first (ADR 0004)"))
        # The ceiling of an epoch is the largest budget ever applied in it, current or past (ADR 0004).
        ceiling = max((Fraction(b) for (b,) in con.execute(
            "SELECT budget_epsilon FROM policies WHERE dataset = ? AND epoch = ? "
            "UNION ALL SELECT budget_epsilon FROM policy_history WHERE dataset = ? AND epoch = ?",
            (ds.name, epoch, ds.name, epoch))), default=None)
        if epoch_ok and ceiling is not None and ds.budget > ceiling:
            errors.append(("dataset.budget.epsilon",
                           f"raises the budget from {ceiling} to {ds.budget} inside epoch {epoch}; start a new epoch (ADR 0004)"))
        if current is not None:
            active = [r[0] for r in con.execute(
                "SELECT name FROM consumers WHERE dataset = ? AND revoked_at IS NULL", (ds.name,))]
            dropped = sorted(n for n in active if n not in policy.consumers)
            for name in dropped:
                errors.append(("consumers", f"consumer {name} has an active key; revoke it first"))
        if errors:
            raise PolicyError(errors)
        new_version = 1 if current is None else current[0] + 1
        if current is not None:
            con.execute(
                "INSERT INTO policy_history SELECT dataset, version, epoch, budget_epsilon, text, sha256, applied_at "
                "FROM policies WHERE dataset = ?", (ds.name,))
        con.execute(
            "INSERT OR REPLACE INTO policies VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ds.name, new_version, epoch, str(ds.budget), text, _hash(text), _now()),
        )
    return AppliedPolicy(ds.name, new_version, epoch)


_POLICY_COLS = "dataset, version, epoch, budget_epsilon, text, sha256, applied_at"


EPOCH_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")  # plain text, so "2026-10" sorts before "2026-11" and nothing looks alike


def new_epoch(path, dataset: str, epoch: str, snapshot: str, reason: str) -> EpochRow:
    """Record a budget renewal before the policy file with the new label is applied (ADR 0004). Nothing else changes."""
    epoch, snapshot, reason = epoch.strip(), snapshot.strip(), reason.strip()
    if not epoch or not snapshot or not reason:
        raise ValueError("an epoch label, a snapshot id and a reason are required")
    if not EPOCH_LABEL.fullmatch(epoch):
        raise ValueError(f"epoch label {epoch!r} may use letters, digits, '.', '_' and '-' only; labels compare as text")
    with _db(path, write=True) as con:
        current = con.execute("SELECT epoch FROM policies WHERE dataset = ?", (dataset,)).fetchone()
        if current is None:
            raise ValueError(f"no policy applied for dataset {dataset}; apply the first policy with its epoch instead")
        if epoch <= current[0]:
            raise ValueError(f"epoch {epoch} is not after the current epoch {current[0]}")
        if con.execute("SELECT 1 FROM epoch_log WHERE dataset = ? AND epoch = ?", (dataset, epoch)).fetchone():
            raise ValueError(f"epoch {epoch} is already recorded for {dataset}")
        row = EpochRow(dataset, epoch, snapshot, reason, _now())
        con.execute("INSERT INTO epoch_log VALUES (?, ?, ?, ?, ?)", (row.dataset, row.epoch, row.snapshot, row.reason, row.ts))
    return row


def epoch_log(path, dataset: str) -> list[EpochRow]:
    with _db(path) as con:
        return [EpochRow(*r) for r in con.execute(
            "SELECT dataset, epoch, snapshot, reason, ts FROM epoch_log WHERE dataset = ? ORDER BY epoch", (dataset,))]


def record_materialization(path, dataset: str, tbl: str, backend: str, epoch: str, snapshot: str, k: int,
                           order_by: str, privacy_id: str, physical_name: str, definition_sha256: str) -> None:
    with _db(path, write=True) as con:
        if con.execute("SELECT 1 FROM materializations WHERE dataset = ? AND tbl = ? AND epoch = ? AND k = ?",
                       (dataset, tbl, epoch, k)).fetchone():
            raise ValueError(f"{dataset}.{tbl} is already materialized for epoch {epoch} at k = {k}")
        con.execute("INSERT INTO materializations (dataset, tbl, backend, epoch, snapshot, k, order_by, privacy_id, physical_name, "
                    "created_at, definition_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (dataset, tbl, backend, epoch, snapshot, k, order_by, privacy_id, physical_name, _now(), definition_sha256))


def materializations(path, dataset: str) -> list[MaterializationRow]:
    with _db(path) as con:
        return [MaterializationRow(*r) for r in con.execute(
            "SELECT dataset, tbl, backend, epoch, snapshot, k, order_by, privacy_id, physical_name, created_at, definition_sha256 "
            "FROM materializations WHERE dataset = ? ORDER BY tbl, epoch, k", (dataset,))]


def ledger_dataset(path, ledger_id: int) -> str | None:
    """The dataset a ledger row belongs to; None for an unknown id."""
    with _db(path) as con:
        row = con.execute("SELECT dataset FROM ledger WHERE id = ?", (ledger_id,)).fetchone()
    return row[0] if row else None


def get_policy(path, dataset: str) -> StoredPolicy | None:
    with _db(path) as con:
        row = con.execute(f"SELECT {_POLICY_COLS} FROM policies WHERE dataset = ?", (dataset,)).fetchone()
    return StoredPolicy._from_row(row) if row else None


def list_policies(path) -> list[StoredPolicy]:
    with _db(path) as con:
        return [StoredPolicy._from_row(r) for r in con.execute(f"SELECT {_POLICY_COLS} FROM policies ORDER BY dataset")]


def policy_history(path, dataset: str) -> list[StoredPolicy]:
    with _db(path) as con:
        return [StoredPolicy._from_row(r) for r in con.execute(
            f"SELECT {_POLICY_COLS} FROM policy_history WHERE dataset = ? ORDER BY version", (dataset,))]


# -- consumers and keys ------------------------------------------------------

def create_consumer(path, dataset: str, name: str) -> str:
    """Return the API key once; only its SHA-256 is stored."""
    with _db(path, write=True) as con:
        row = con.execute("SELECT text FROM policies WHERE dataset = ?", (dataset,)).fetchone()
        if row is None:
            raise ValueError(f"no policy applied for dataset {dataset}")
        if name not in loads_policy(row[0]).consumers:
            raise ValueError(f"consumer {name} is not in the policy of {dataset}")
        if con.execute("SELECT 1 FROM consumers WHERE dataset = ? AND name = ? AND revoked_at IS NULL", (dataset, name)).fetchone():
            raise ValueError(f"consumer {name} already has an active key; revoke it first")
        key = KEY_PREFIX + secrets.token_urlsafe(32)
        con.execute("INSERT INTO consumers (dataset, name, key_sha256, created_at) VALUES (?, ?, ?, ?)",
                    (dataset, name, _hash(key), _now()))
    return key


def resolve_key(path, key: str) -> tuple[str, str] | None:
    """(dataset, consumer) for an active key; None for unknown and revoked alike."""
    with _db(path) as con:
        row = con.execute("SELECT dataset, name FROM consumers WHERE key_sha256 = ? AND revoked_at IS NULL", (_hash(key),)).fetchone()
    return (row[0], row[1]) if row else None


def revoke_consumer(path, dataset: str, name: str) -> None:
    with _db(path, write=True) as con:
        n = con.execute("UPDATE consumers SET revoked_at = ? WHERE dataset = ? AND name = ? AND revoked_at IS NULL",
                        (_now(), dataset, name)).rowcount
    if n == 0:
        raise ValueError(f"consumer {name} of {dataset} has no active key")


def list_consumers(path, dataset: str) -> list[ConsumerRow]:
    with _db(path) as con:
        return [ConsumerRow(*r) for r in con.execute(
            "SELECT dataset, name, created_at, revoked_at FROM consumers WHERE dataset = ? ORDER BY id", (dataset,))]
