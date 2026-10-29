# SPDX-License-Identifier: Apache-2.0
"""Materialized tables (technical design §6): the first k rows per privacy unit of a truncated table, stored once per epoch.

On Spark the truncation window shuffles the whole table on every query. `materialize` runs the rewriter's own
truncation subquery once, as CREATE TABLE ... AS SELECT through the backend's trusted admin session, after checking
that every declared column has its declared type in the data and every declared primary key is unique (a
duplicated key, or two values a join's implicit cast makes equal, multiply joined rows past the bound). The catalog
records what it created with the SHA-256 of that SELECT; the runtime hands the rewriter only records whose name and
definition match the stored policy, the dataset's backend and the current epoch, and the rewriter uses one only when
no WHERE conjunct is pushed into that table.
"""
from __future__ import annotations

import hashlib
import os
import re

from cleanroom_sql import Policy, loads_policy
from cleanroom_sql.schema import Table
from cleanroom_sql.sqlast import truncated

from . import catalog
from .backends import open_backend


def physical_name(table: str, k: int, epoch: str, snapshot: str) -> str:
    """A bare identifier; the hash keeps `s-1` and `s_1`, or two epochs, from landing on one name."""
    tag = hashlib.sha256(f"{epoch}\0{snapshot}".encode()).hexdigest()[:8]
    return f"{table}__ocr_k{k}_{re.sub(r'[^A-Za-z0-9_]', '_', snapshot)}_{tag}"


def definition(table: Table, k: int, dialect: str = "spark") -> str:
    """The SELECT a materialized table is created from: the rewriter's truncation subquery for this table and k."""
    return truncated(table, table.name, k).this.sql(dialect=dialect)


def definition_sha256(table: Table, k: int) -> str:
    return hashlib.sha256(definition(table, k).encode()).hexdigest()


# Backend type names that hold each declared column type exactly; anything else (a string holding numbers, a double)
# lets an implicit cast make two different stored values equal in a join.
_STORED = {
    "int": {"int", "integer", "bigint", "smallint", "tinyint", "long", "short", "byte", "hugeint"},
    "string": {"string", "varchar"},
    "bool": {"boolean", "bool"},
    "date": {"date"},
}


def check_declared_types(run, policy: Policy) -> None:
    """Every declared column exists with a backend type that holds its declared type; refuses naming each mismatch."""
    bad = []
    for name, table in sorted(policy.dataset.schema.tables.items()):
        stored = {}
        for row in run(f"DESCRIBE {name}"):
            column = str(row[0] or "").strip()
            if column and not column.startswith("#"):
                stored.setdefault(column.lower(), str(row[1]).lower().split("(")[0].strip())
        for col in table.columns:
            found = stored.get(col.name.lower())
            if found not in _STORED[col.type]:
                bad.append(f"{name}.{col.name} is {found or 'missing'}, declared {col.type}")
    if bad:
        raise ValueError("stored types differ from the policy: " + "; ".join(bad) + "; the sensitivity bound assumes the declared types")


def check_primary_keys(run, policy: Policy) -> None:
    """`run(sql) -> rows` on a trusted connection: declared types first, then every declared primary key unique and not NULL."""
    check_declared_types(run, policy)
    bad = []
    for name, table in sorted(policy.dataset.schema.tables.items()):
        if table.primary_key is None:
            continue
        total, distinct = run(f"SELECT COUNT(*), COUNT(DISTINCT {table.primary_key}) FROM {name}")[0]
        if total != distinct:
            bad.append(name)
    if bad:
        raise ValueError(f"declared primary keys are not unique (or are NULL) in: {', '.join(bad)}; "
                         "fix the data or the policy: the sensitivity bound assumes unique keys")


def materialize(path: str | os.PathLike, dataset: str, snapshot: str) -> list[str]:
    """Create the materialized table of every truncated table of `dataset` for its current epoch; returns their names."""
    stored = catalog.get_policy(path, dataset)
    if stored is None:
        raise ValueError(f"no policy applied for dataset {dataset}")
    policy = loads_policy(stored.text)
    recorded = {r.epoch: r.snapshot for r in catalog.epoch_log(path, dataset)}
    if stored.epoch in recorded and recorded[stored.epoch] != snapshot:
        raise ValueError(f"epoch {stored.epoch} was recorded with snapshot {recorded[stored.epoch]}, not {snapshot}")
    row = catalog.get_backend(path, policy.dataset.backend)
    if row is None or row.kind != "spark":
        raise ValueError("materialization needs a Spark backend; on DuckDB the truncation window stays")
    backend = open_backend(row.kind, row.location, row.query_timeout)
    try:
        return _create(path, dataset, snapshot, stored, policy, row, backend)
    finally:
        backend.close()


def _create(path, dataset, snapshot, stored, policy, row, backend) -> list[str]:
    check_primary_keys(backend.admin_sql, policy)
    k, created = policy.dataset.k, []
    for name, table in sorted(policy.dataset.schema.tables.items()):
        if not table.is_truncated:
            continue
        if any(m.tbl == name and m.epoch == stored.epoch and m.k == k for m in catalog.materializations(path, dataset)):
            raise ValueError(f"{dataset}.{name} is already materialized for epoch {stored.epoch} at k = {k}")
        target = physical_name(name, k, stored.epoch, snapshot)
        try:  # USING parquet: Spark 3.x would otherwise try a Hive table, which needs Hive support
            backend.admin_sql(f"CREATE TABLE {target} USING parquet AS {definition(table, k, backend.dialect)}")
        except Exception as e:  # e.g. already there (by hand, or a run that died before its record): never adopt it
            reason = str(e).strip().splitlines()[0][:200] if str(e).strip() else type(e).__name__
            raise ValueError(f"could not create {target}: {reason}") from None  # admin command: the reason is the admin's to read
        catalog.record_materialization(path, dataset, name, row.name, stored.epoch, snapshot, k, table.order_by, table.privacy_id,
                                       target, definition_sha256(table, k))
        created.append(target)
    return created
