# SPDX-License-Identifier: Apache-2.0
"""Trusted executor: runs the rewritten SQL, adds OpenDP noise, tracks budget.

Trust boundary: this object must run where the consumer cannot observe it
(server / proxy). It is the only component that sees raw aggregates.
"""
from __future__ import annotations

import itertools
import math
import re
from dataclasses import dataclass, field
from fractions import Fraction

import opendp.prelude as dp

from .ledger import BudgetExhaustedException, Ledger
from .policy import ConsumerPolicy, KeysetRef, Policy
from .rewriter import Aggregate, Rewriter, RewrittenQuery
from .schema import Schema
from .sqlast import distinct_values

dp.enable_features("contrib")

KeyTuple = tuple


def _frac(x: float) -> Fraction:
    """Exact budget arithmetic on the decimal the caller wrote (0.1 -> 1/10)."""
    return Fraction(repr(x)) if isinstance(x, float) else Fraction(x)
Keyset = list[KeyTuple]


class PolicyViolation(ValueError):
    """The query is valid SQL but outside the consumer's policy. Raised before any budget moves."""


class QueryFailed(RuntimeError):
    """The backend refused or failed the rewritten query. The backend's message is never forwarded, and the ε is charged."""


MAX_KEYSET_SIZE = 1_000_000  # cartesian product of GROUP BY keysets built in memory
MAX_LAPLACE_SCALE = 2.0**62  # below this the discrete Laplace scale is finite for every sensitivity we accept


_METADATA = re.compile(
    r"^\s*(?:(?:describe|desc|show\s+columns\s+(?:from|in))\s+([A-Za-z_][A-Za-z0-9_]*)|(show\s+tables))\s*;?\s*$",
    re.IGNORECASE,
)


def is_metadata_query(sql: str) -> tuple[str, str | None] | None:
    """("describe", table) for DESCRIBE <table> / SHOW COLUMNS FROM <table>, ("tables", None) for SHOW TABLES; else None.

    Metadata is answered from the policy (admin-declared columns), never from the backend, and costs no budget.
    """
    m = _METADATA.match(sql)
    if not m:
        return None
    return ("tables", None) if m.group(2) else ("describe", m.group(1))


@dataclass(frozen=True)
class Result:
    rows: list[tuple]  # SELECT order; keys from keyset, aggregates noisy
    rewritten_sql: str
    epsilon_spent: float
    epsilon_per_measurement: float  # an AVG has two measurements
    sensitivities: tuple[int, ...]  # L1, one per measurement
    ci95_halfwidth: tuple[float, ...]  # one per measurement
    request_id: str | None = None  # set by a platform runtime; a replay returns the same id


@dataclass
class Session:
    con: object  # DB-API-like connection with .execute(sql).fetchall()
    schema: Schema
    epsilon: float  # display only: the total budget, or the consumer cap under a policy; accounting is Fraction
    k: int  # max rows per privacy unit (truncation threshold)
    dialect: str = "duckdb"
    # set by from_policy: the consumer's rules, the dataset keysets and the shared ledger
    policy: ConsumerPolicy | None = None
    keysets: dict[tuple[str, str], list | KeysetRef] | None = None
    public_tables: frozenset[str] = frozenset()  # keyset sources: public tables of the dataset, visible to the consumer or not
    ledger: Ledger | None = None
    materialized: dict[str, str] | None = None  # truncated table -> its materialized table (platform-created, see Rewriter)
    _spent: Fraction = field(default=Fraction(0), init=False)  # exact accounting without a ledger

    @classmethod
    def from_policy(cls, policy: Policy, consumer: str, con: object, ledger: Ledger | None = None,
                    materialized: dict[str, str] | None = None) -> Session:
        """A session for one consumer under a policy.

        Every session on the same policy object shares one ledger, so the
        dataset budget bounds all consumers together (ADR 0004). Pass a
        ledger only to share across policy objects (for example after a reload).
        """
        rules = policy.consumers[consumer]
        ds = policy.dataset
        # The rewriter sees only the consumer's tables, so a forbidden table and an unknown one fail the same way.
        visible = Schema(tables={n: t for n, t in ds.schema.tables.items() if n in rules.tables})
        return cls(
            con=con, schema=visible, epsilon=float(rules.cap if rules.cap is not None else ds.budget), k=ds.k,
            dialect=ds.dialect, policy=rules, keysets=ds.keysets,
            public_tables=frozenset(n for n, t in ds.schema.tables.items() if t.is_public),
            ledger=ledger if ledger is not None else Ledger.for_policy(policy), materialized=materialized,
        )

    @property
    def spent(self) -> float:
        return float(self.ledger.spent(self.policy.name) if self.ledger else self._spent)

    @property
    def remaining(self) -> float:
        return float(self.ledger.remaining(self.policy.name) if self.ledger else _frac(self.epsilon) - self._spent)

    def query(self, sql: str, epsilon: float, keyset: Keyset | list | None = None) -> Result:
        if not isinstance(epsilon, (int, float)) or isinstance(epsilon, bool) or not math.isfinite(epsilon) or epsilon <= 0:
            raise ValueError("epsilon must be a finite positive number")
        if self.ledger is None and _frac(epsilon) > _frac(self.epsilon) - self._spent:
            raise BudgetExhaustedException(f"requested {epsilon}, remaining {self.remaining}")

        rq = Rewriter(self.schema, self.k, self.dialect, self.materialized).rewrite(sql)
        if self.policy is not None:
            self._check_policy(rq, epsilon, keyset)
            keyset = self._policy_keyset(rq)
        elif rq.key_positions and keyset is None:
            raise ValueError("GROUP BY requires a public keyset")

        measurements = rq.measurements
        eps_each = epsilon / len(measurements)  # basic sequential composition
        if max(m.sensitivity for m in measurements) / eps_each > MAX_LAPLACE_SCALE:
            raise ValueError("epsilon too small for this query: the noise scale would overflow")
        mechanisms = [self._mechanism(m, eps_each) for m in measurements]

        reservation = self.ledger.reserve(self.policy.name, _frac(epsilon)) if self.ledger else None
        try:
            keyset, vectors = self._true_vectors(rq, keyset)
            try:
                noisy = [m(v) for m, v in zip(mechanisms, vectors)]
            except Exception:  # OpenDP refused a value (not representable): the message would carry the true aggregate
                raise QueryFailed("the mechanism could not run on the query result") from None
            del vectors
        except QueryFailed:
            # The backend saw the data: whether it fails can depend on it. Charge the per-query maximum, not the
            # requested ε, so a failure channel costs as much as the largest query the policy allows.
            if reservation is not None:
                self.ledger.commit(reservation)
                self.ledger.charge_up_to(self.policy.name, self.policy.max_epsilon_per_query - _frac(epsilon))
            else:
                self._spent += _frac(epsilon)
            raise
        except BaseException:
            if reservation is not None:
                self.ledger.release(reservation)
            raise

        if reservation is not None:
            self.ledger.commit(reservation)
        else:
            self._spent += _frac(epsilon)
        rows = self._assemble(rq, keyset, noisy)
        return Result(
            rows=rows,
            rewritten_sql=rq.sql,
            epsilon_spent=epsilon,
            epsilon_per_measurement=eps_each,
            sensitivities=tuple(m.sensitivity for m in measurements),
            ci95_halfwidth=tuple(m.sensitivity / eps_each * math.log(20) for m in measurements),
        )

    # -- metadata, from the policy, no budget --------------------------------

    def _visible_tables(self) -> list[str]:
        names = sorted(self.schema.tables)
        return [n for n in names if self.policy is None or n in self.policy.tables]

    def describe(self, table: str) -> list[tuple[str, str, str]]:
        """(column, type, clamp) for every admin-declared column of a table the consumer may read."""
        if table not in self._visible_tables():
            raise PolicyViolation(f"table {table} is not in the policy" + (f" of {self.policy.name}" if self.policy else ""))
        t = self.schema.table(table)
        return [(c.name, c.type, f"[{c.lower}, {c.upper}]" if c.lower is not None and c.upper is not None else "") for c in t.columns]

    def tables(self) -> list[tuple[str, str]]:
        """(table, kind) for every table the consumer may read; the kind says how truncation applies."""
        out = []
        for name in self._visible_tables():
            t = self.schema.table(name)
            kind = "public" if t.is_public else ("private, keyed (one row per privacy unit)" if t.keyed_by_privacy_id
                                                   else f"private, truncated to {self.k} rows per privacy unit")
            out.append((name, kind))
        return out

    # -- policy -------------------------------------------------------------

    def _check_policy(self, rq: RewrittenQuery, epsilon: float, keyset) -> None:
        """Tables, aggregates, ε per query and keysets, after rewriting and before any budget moves."""
        rules = self.policy
        if self.ledger is not None:  # a re-applied policy changes the rules for every open session, not only new ones
            rules = self.ledger.rules.get(self.policy.name)
            if rules is None:
                raise PolicyViolation(f"consumer {self.policy.name} is no longer in the policy")
        for table in rq.tables:
            if table not in rules.tables:
                raise PolicyViolation(f"table {table} is not in the policy of {rules.name}")
        for a in rq.aggregates:
            if a.kind not in rules.aggregates:
                raise PolicyViolation(f"aggregate {a.kind} is not allowed by the policy of {rules.name}")
        if _frac(epsilon) > rules.max_epsilon_per_query:
            raise PolicyViolation(f"epsilon {epsilon} exceeds max_epsilon_per_query {rules.max_epsilon_per_query}")
        if keyset is not None:
            raise PolicyViolation("keysets come from the policy, not from the consumer (ADR 0005)")
        for col in rq.group_columns:
            if col not in self.keysets:
                raise PolicyViolation(f"GROUP BY {col[0]}.{col[1]} has no keyset declared in the policy")

    def _policy_keyset(self, rq: RewrittenQuery) -> Keyset | None:
        """The declared keyset of each GROUP BY column, combined as a cartesian product in GROUP BY order."""
        if not rq.group_columns:
            return None
        per_column, size = [], 1
        for col in rq.group_columns:
            spec = self.keysets[col]
            if isinstance(spec, KeysetRef):  # read in clear: no budget, so the executor itself checks the table is public
                if spec.table not in self.public_tables:
                    raise PolicyViolation(f"keyset {spec.table}.{spec.column} is not a public column")
                sql = distinct_values(spec.table, spec.column).sql(dialect=self.dialect)
                try:
                    spec = [r[0] for r in self.con.execute(sql).fetchall()]
                except Exception:  # a timeout or a backend error: generic, like any backend failure; nothing reserved yet
                    raise QueryFailed("the backend could not read the keyset") from None
            size *= len(spec)
            if size > MAX_KEYSET_SIZE:  # checked as each column is resolved, before the product is built
                raise PolicyViolation(f"keyset product exceeds {MAX_KEYSET_SIZE} keys")
            per_column.append(list(spec))
        return list(itertools.product(*per_column))

    # -- helpers ------------------------------------------------------------

    def _true_vectors(self, rq: RewrittenQuery, keyset) -> tuple[Keyset, list[list[int]]]:
        """Run the rewritten query and align it to the keyset: one true integer vector per measurement.

        The only place raw data is touched. The sensitivity property test calls
        this on a dataset and its neighbour; `query` adds noise to its output.
        """
        # Duplicate keys would let one privacy unit move two entries: L1 would be 2Δ.
        keyset = list(dict.fromkeys(self._normalize_keyset(keyset, len(rq.key_positions))))
        if len(keyset) > MAX_KEYSET_SIZE:
            raise PolicyViolation(f"keyset exceeds {MAX_KEYSET_SIZE} keys")
        try:
            raw = self.con.execute(rq.sql).fetchall()
            by_key = {tuple(r[i] for i in rq.key_positions): r for r in raw}
            vectors = [  # SUM over no non-NULL value is NULL in SQL: that is 0
                [int(by_key[key][m.position] or 0) if key in by_key else 0 for key in keyset]
                for m in rq.measurements
            ]
        except Exception:  # the backend's text or a bad value can carry raw data (CAST failures, error()): never forward it
            raise QueryFailed("the backend could not run the query") from None
        del raw, by_key
        return keyset, vectors

    @staticmethod
    def _normalize_keyset(keyset, n_keys: int) -> Keyset:
        if n_keys == 0:
            return [()]
        return [k if isinstance(k, tuple) else (k,) for k in keyset]

    @staticmethod
    def _mechanism(agg: Aggregate, eps: float):
        """Discrete Laplace on an integer vector under L1 distance.

        OpenDP's privacy map is the proof: map(Δ) must not exceed eps.
        """
        scale = agg.sensitivity / eps
        meas = dp.m.make_laplace(
            dp.vector_domain(dp.atom_domain(T="i64")), dp.l1_distance(T="i64"), scale=scale
        )
        if meas.map(agg.sensitivity) > eps * (1 + 1e-9):
            raise RuntimeError("OpenDP privacy map exceeds requested epsilon")
        return meas

    @staticmethod
    def _assemble(rq: RewrittenQuery, keyset: Keyset, noisy: list[list[int]]) -> list[tuple]:
        width = len(rq.key_positions) + len(rq.aggregates)
        rows = []
        for i, key in enumerate(keyset):
            row: list = [None] * width
            for pos, val in zip(rq.key_positions, key):
                row[pos] = val
            vectors = iter(noisy)  # one per measurement, in rq.measurements order
            for a in rq.aggregates:
                v = next(vectors)[i]
                if a.kind == "avg":  # post-processing: finite even when the noisy count is <= 0
                    row[a.position] = v / max(next(vectors)[i], 1)
                else:
                    row[a.position] = max(v, 0) if a.kind == "count" else v
            rows.append(tuple(row))
        return rows
