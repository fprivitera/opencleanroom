# SPDX-License-Identifier: Apache-2.0
"""Every SQLGlot node the rewriter builds, in one place.

A SQLGlot upgrade that renames an argument (in 30.24 FROM is `from_`) breaks
this module and tests/test_sqlast.py, nothing else. The rewriter only
reads, matches and replaces nodes.
"""
from __future__ import annotations

import sqlglot
from sqlglot import exp

from .schema import Table

INPUT_DIALECT = "duckdb"  # consumer SQL is one dialect per deployment; the backend dialect shapes the output only


def parse(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, read=INPUT_DIALECT)


def from_table(tree: exp.Select) -> exp.Table:
    """The relation in FROM (the arg is `from_` in the pinned SQLGlot; a rename raises KeyError here)."""
    return tree.args["from_"].this


def _literal(n: int) -> exp.Literal:
    return exp.Literal.number(n)


def sum_of(expr: exp.Expression) -> exp.Sum:
    return exp.Sum(this=expr)


def count_of(expr: exp.Expression) -> exp.Count:
    """COUNT(expr): non-NULL rows, the same rows SUM(expr) adds up."""
    return exp.Count(this=expr)


def clamp(col: exp.Expression, lower: int, upper: int) -> exp.Expression:
    """CASE WHEN col IS NULL THEN NULL ELSE LEAST(GREATEST(col, lower), upper) END.

    DuckDB, Spark and Postgres all skip NULL inside GREATEST/LEAST, which
    would clamp NULL to `lower`. The explicit guard keeps NULL as NULL on
    every backend, so SUM and COUNT(col) skip the same rows. The bounds
    are literals, never NULL, so one guard on the column is exact.
    `ignore_nulls=True` tells SQLGlot not to add its own DuckDB guard.
    """
    clamped = exp.Least(
        this=exp.Greatest(this=col, expressions=[_literal(lower)], ignore_nulls=True),
        expressions=[_literal(upper)],
        ignore_nulls=True,
    )
    return exp.Case(ifs=[exp.If(this=exp.Is(this=col.copy(), expression=exp.Null()), true=exp.Null())], default=clamped)


def distinct_values(table: str, column: str) -> exp.Select:
    """SELECT DISTINCT column FROM table: the keyset of a public column."""
    return exp.select(exp.column(column)).distinct().from_(table)


def conjunction(conjuncts: list[exp.Expression]) -> exp.Expression | None:
    """c1 AND c2 AND ..., or None for an empty list."""
    return exp.and_(*conjuncts) if conjuncts else None


def set_where(tree: exp.Select, condition: exp.Expression | None) -> None:
    """Replace the WHERE clause; None removes it."""
    tree.set("where", exp.Where(this=condition) if condition is not None else None)


def truncated(table: Table, alias: str, k: int, where: exp.Expression | None = None) -> exp.Subquery:
    """SELECT cols FROM (SELECT cols, ROW_NUMBER() OVER (PARTITION BY pid ORDER BY order_by) _rn FROM t [WHERE where]) WHERE _rn <= k.

    The WHERE runs before ROW_NUMBER, so the k rows kept per privacy unit match it.
    """
    rn = exp.Window(
        this=exp.RowNumber(),
        partition_by=[exp.column(table.privacy_id)],
        order=exp.Order(expressions=[exp.Ordered(this=exp.column(table.order_by))]),
    )
    taken = {c.name.lower() for c in table.columns}  # the backend compares names case-insensitively
    rn_name = "_rn"
    while rn_name in taken:  # a declared column named _rn would shadow the row number in the filter
        rn_name += "_"
    inner = exp.select(*[exp.column(c.name) for c in table.columns], exp.alias_(rn, rn_name)).from_(table.name)
    if where is not None:
        inner = inner.where(where)
    outer = (
        exp.select(*[exp.column(c.name) for c in table.columns])
        .from_(inner.subquery("_t"))
        .where(exp.LTE(this=exp.column(rn_name), expression=_literal(k)))
    )
    return outer.subquery(alias)
