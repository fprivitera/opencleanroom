# SPDX-License-Identifier: Apache-2.0
"""SQLGlot-based analyzer + rewriter.

Input: consumer SQL (SELECT with COUNT(*)/SUM(col)/AVG(col), optional WHERE,
GROUP BY, inner equi-joins on FK -> PK).
Output: a RewrittenQuery with
  * SQL where every per-user table is truncated to k rows per user and
    every SUM argument is clamped,
  * the L1 sensitivity of each measurement (rows one privacy unit can own × bound);
    AVG(col) is two measurements, SUM(col) and COUNT(col), over the same rows,
  * the layout of the SELECT list (which positions are keys/aggregates).

No noise is added here: noise is applied by the executor with OpenDP.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from sqlglot import exp
from sqlglot.errors import ParseError

from .schema import Schema, Table
from .sqlast import clamp, conjunction, count_of, from_table, parse, set_where, sum_of, truncated


class UnsupportedQuery(ValueError):
    pass


# Every node type the supported grammar needs, nothing else. A function, an arithmetic operator, a CAST or a
# CASE anywhere in the query is refused before analysis: the backend must never evaluate a consumer expression
# that can fail or take data-dependent time: an error or a delay would be an oracle on the data.
# A consumer query is a handful of aggregates and predicates. Larger text or trees only cost planning time, which no
# backend timeout can stop (DuckDB plans before it can be interrupted): refused before any budget moves.
MAX_SQL_CHARS = 50_000
MAX_QUERY_NODES = 2_000

ALLOWED_NODES = frozenset({
    exp.Select, exp.From, exp.Where, exp.Group, exp.Join, exp.Table, exp.TableAlias, exp.Alias, exp.Identifier,
    exp.Column, exp.Star, exp.Literal, exp.Null, exp.Neg,
    exp.Count, exp.Sum, exp.Avg,
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.And, exp.Or, exp.Not, exp.Paren, exp.Is, exp.In, exp.Between,
})


@dataclass(frozen=True)
class Aggregate:
    """One item of the consumer's SELECT list, and the noisy measurement(s) behind it."""

    position: int  # column in the rewritten SQL; for the consumer's items, also the result row index
    kind: str  # "count" | "sum" | "avg"
    column: str | None
    sensitivity: int  # L1 sensitivity of the measurement; for avg, of its SUM
    count_position: int | None = None  # avg only: column of COUNT(col) in the rewritten SQL
    count_sensitivity: int | None = None  # avg only

    @property
    def measurements(self) -> tuple[Aggregate, ...]:
        """The noisy measurements, each with its column in the rewritten SQL."""
        if self.kind != "avg":
            return (self,)
        return (
            Aggregate(self.position, "sum", self.column, self.sensitivity),
            Aggregate(self.count_position, "count", self.column, self.count_sensitivity),
        )


@dataclass(frozen=True)
class RewrittenQuery:
    sql: str
    key_positions: tuple[int, ...]
    aggregates: tuple[Aggregate, ...]
    rows_per_user: int  # Δ_R of the joined relation
    tables: tuple[str, ...]  # schema table names the query reads, sorted
    group_columns: tuple[tuple[str, str], ...]  # (table, column) of each GROUP BY item, in order

    @property
    def measurements(self) -> tuple[Aggregate, ...]:
        """Every noisy measurement, SELECT-list order, with each AVG's SUM then COUNT; position is its SQL column."""
        return tuple(m for a in self.aggregates for m in a.measurements)


class Rewriter:
    def __init__(self, schema: Schema, k: int, dialect: str = "duckdb", materialized: Mapping[str, str] | None = None):
        """`materialized` maps a truncated table to its materialized table, which holds exactly the rows its truncation
        keeps at this `k` (created by the platform from `sqlast.truncated`); the caller vouches for that, the rewriter only
        decides when the substitution is exact."""
        self.schema = schema
        self.k = k
        self.dialect = dialect
        self.materialized = dict(materialized or {})

    # -- public -----------------------------------------------------------

    def rewrite(self, sql: str) -> RewrittenQuery:
        tree = self._parse(sql)
        if not isinstance(tree, exp.Select):
            raise UnsupportedQuery("only SELECT statements are supported")
        for banned in ("having", "limit", "offset", "distinct", "qualify", "with", "order", "windows"):
            if tree.args.get(banned):
                raise UnsupportedQuery(f"{banned.upper()} is not supported")
        if tree.find(exp.Subquery) or tree.find(exp.Window):
            raise UnsupportedQuery("subqueries and window functions are not supported")
        if any(node is not tree for node in tree.find_all(exp.Select)):  # EXISTS, ANY/ALL, IN (SELECT), scalar subqueries
            raise UnsupportedQuery("nested SELECT is not supported")
        self._whitelist(tree)
        if (where := tree.args.get("where")) is not None and where.find(exp.AggFunc):
            raise UnsupportedQuery("aggregates are not allowed in WHERE")

        aliases = self._aliases(tree)  # alias -> Table
        for col in tree.find_all(exp.Column):  # every column must resolve to the schema
            self._resolve(col, aliases)
        self._check_types(tree, aliases)
        rows_per_user = self._join_sensitivity(tree, aliases)
        key_positions, aggregates, group_columns = self._select_layout(tree, aliases, rows_per_user)

        self._expand_avgs(tree)
        self._clamp_sums(tree, aliases)
        self._truncate_tables(tree, aliases)
        return RewrittenQuery(
            sql=tree.sql(dialect=self.dialect),
            key_positions=tuple(key_positions),
            aggregates=tuple(aggregates),
            rows_per_user=rows_per_user,
            tables=tuple(sorted({t.name for t in aliases.values()})),
            group_columns=tuple(group_columns),
        )

    def check_public(self, sql: str, limit: int = 10_000) -> str:
        """A rows query over public tables only: the whitelist plus DISTINCT and LIMIT, at most two tables, a hard LIMIT.

        No aggregate is required and nothing is noised: public data costs no
        budget. Any private table, any node outside the whitelist, or more than
        two tables is `UnsupportedQuery`. Used for keysets and lookups.
        """
        tree = self._parse(sql)
        if not isinstance(tree, exp.Select):
            raise UnsupportedQuery("only SELECT statements are supported")
        if any(node is not tree for node in tree.find_all(exp.Select)):
            raise UnsupportedQuery("nested SELECT is not supported")
        self._whitelist(tree, extra=frozenset({exp.Distinct, exp.Limit}))
        aliases = self._aliases(tree)
        if len(aliases) > 2:
            raise UnsupportedQuery("a public query joins at most two tables")
        for table in aliases.values():
            if not table.is_public:
                raise UnsupportedQuery(f"{table.name} is not a public table")
        for col in tree.find_all(exp.Column):
            self._resolve(col, aliases)
        self._check_types(tree, aliases)
        for join in tree.args.get("joins") or []:
            on = join.args.get("on")
            if not isinstance(on, exp.EQ) or not isinstance(on.this, exp.Column) or not isinstance(on.expression, exp.Column):
                raise UnsupportedQuery("join condition must be col = col")
        current = tree.args.get("limit")
        if current is not None and not (isinstance(current.expression, exp.Literal) and current.expression.is_int):
            raise UnsupportedQuery("LIMIT must be an integer literal")
        cap = min(limit, int(current.expression.name)) if current is not None else limit
        tree.set("limit", None)
        return tree.limit(cap).sql(dialect=self.dialect)

    # -- analysis ---------------------------------------------------------

    @staticmethod
    def _parse(sql: str) -> exp.Expression:
        if len(sql) > MAX_SQL_CHARS:
            raise UnsupportedQuery(f"the query is longer than {MAX_SQL_CHARS} characters")
        try:
            tree = parse(sql)
        except ParseError:  # fail closed with the library's own error type; the parser's text is not forwarded
            raise UnsupportedQuery("the query could not be parsed") from None
        if sum(1 for _ in tree.walk()) > MAX_QUERY_NODES:
            raise UnsupportedQuery(f"the query has more than {MAX_QUERY_NODES} terms")
        return tree

    @staticmethod
    def _whitelist(tree: exp.Select, extra: frozenset = frozenset()) -> None:
        for node in tree.walk():
            if type(node) not in ALLOWED_NODES | extra:
                raise UnsupportedQuery(f"{type(node).__name__} is not supported")
            if isinstance(node, exp.Neg) and not isinstance(node.this, exp.Literal):
                raise UnsupportedQuery("arithmetic is not supported")
            if isinstance(node, exp.In) and not all(isinstance(e, exp.Literal) for e in node.expressions):
                raise UnsupportedQuery("IN takes literals only")
            if isinstance(node, exp.Between) and not (isinstance(node.args["low"], exp.Literal) and isinstance(node.args["high"], exp.Literal)):
                raise UnsupportedQuery("BETWEEN takes literals only")

    _COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.In, exp.Between)

    def _check_types(self, tree: exp.Select, aliases: dict[str, Table]) -> None:
        """Every comparison, IN and BETWEEN pairs operands of one declared type: no implicit cast can fail on the data."""
        def kind(e: exp.Expression) -> str:
            if isinstance(e, exp.Column):
                t, c = self._resolve(e, aliases)
                return t.column(c).type
            if isinstance(e, exp.Neg):
                e = e.this
            if isinstance(e, exp.Literal):
                return "string" if e.is_string else "int"
            if isinstance(e, exp.Null):
                return "null"
            raise UnsupportedQuery(f"{type(e).__name__} is not supported in a comparison")
        def check_condition(e: exp.Expression) -> None:
            """A predicate position holds a boolean: a string or int there is cast by the backend and can fail on the data."""
            if isinstance(e, (exp.And, exp.Or)):
                check_condition(e.this)
                check_condition(e.expression)
            elif isinstance(e, (exp.Not, exp.Paren)):
                check_condition(e.this)
            elif isinstance(e, exp.Is):
                if not isinstance(e.expression, exp.Null):
                    raise UnsupportedQuery("IS takes NULL only")
            elif isinstance(e, self._COMPARISONS):
                pass
            elif not (isinstance(e, exp.Column) and kind(e) == "bool"):
                raise UnsupportedQuery(f"{e.sql()} is not a boolean condition")
        if (where := tree.args.get("where")) is not None:
            check_condition(where.this)
        for node in tree.find_all(*self._COMPARISONS):
            operands = [node.this] + ([node.expression] if node.args.get("expression") is not None else [])
            operands += list(node.expressions) if isinstance(node, exp.In) else []
            operands += [node.args["low"], node.args["high"]] if isinstance(node, exp.Between) else []
            kinds = {kind(o) for o in operands} - {"null"}
            if len(kinds) > 1:
                raise UnsupportedQuery(f"comparison mixes types {sorted(kinds)}: {node.sql()}")

    def _aliases(self, tree: exp.Select) -> dict[str, Table]:
        aliases: dict[str, Table] = {}
        for node in tree.find_all(exp.Table):
            if not isinstance(node.this, exp.Identifier):
                raise UnsupportedQuery(f"unsupported table expression {node.sql()}")
            if node.db or node.catalog:
                raise UnsupportedQuery(f"qualified table name {node.sql()} is not supported; use the declared name")
            if node.args.get("alias") is not None and node.args["alias"].args.get("columns"):  # renames columns by position
                raise UnsupportedQuery("a column list on a table alias is not supported")
            try:
                aliases[node.alias_or_name] = self.schema.table(node.name)
            except KeyError as e:
                raise UnsupportedQuery(str(e)) from None
        if not aliases:
            raise UnsupportedQuery("no FROM clause")
        return aliases

    def _resolve(self, col: exp.Column, aliases: dict[str, Table]) -> tuple[Table, str]:
        if col.table:
            if col.table not in aliases:
                raise UnsupportedQuery(f"unknown table alias {col.table}")
            table = aliases[col.table]
            if not any(c.name == col.name for c in table.columns):
                raise UnsupportedQuery(f"column {col.sql()} is unknown")
            return table, col.name
        owners = [t for t in aliases.values() if any(c.name == col.name for c in t.columns)]
        if len(owners) != 1:
            raise UnsupportedQuery(f"column {col.name} is ambiguous or unknown")
        return owners[0], col.name

    def _rows_owned(self, table: Table) -> int:
        """Rows of this table one privacy unit can own: 0 for a public table, 1 keyed, k truncated."""
        if table.is_public:
            return 0
        return 1 if table.keyed_by_privacy_id else self.k

    def _join_sensitivity(self, tree: exp.Select, aliases: dict[str, Table]) -> int:
        """Row-ownership rule: every accepted join keeps each row inside one privacy unit.

        A private ⋈ keyed join on the privacy ID matches a unit's rows only
        with that unit's single keyed row; a public table on the PK side
        matches each row exactly once. So the joined relation owns at most
        the product of the rows each private side owns, whichever table the
        consumer writes first. 0 means the query reads no private table.
        Joins that could leave the privacy unit are rejected (fail closed).
        """
        base_alias = from_table(tree).alias_or_name
        joined_so_far = {base_alias}
        delta = self._rows_owned(aliases[base_alias])  # 0 until a private table is read
        for join in tree.args.get("joins") or []:
            if (join.side or join.kind) not in ("", "INNER") or any(join.args.get(k) for k in ("using", "method", "hint", "match_condition", "pivots")):
                raise UnsupportedQuery("only INNER equi-joins with ON are supported")
            on = join.args.get("on")
            if not isinstance(on, exp.EQ) or not isinstance(on.this, exp.Column) or not isinstance(on.expression, exp.Column):
                raise UnsupportedQuery("join condition must be col = col")
            joined_alias = join.this.alias_or_name
            on_aliases = {self._owner_alias(on.this, aliases), self._owner_alias(on.expression, aliases)}
            if len(on_aliases) != 2 or joined_alias not in on_aliases or not (on_aliases - {joined_alias}) <= joined_so_far:
                raise UnsupportedQuery("join condition must bind the joined table to a table already in the join")
            joined_so_far.add(joined_alias)
            (t_a, c_a), (t_b, c_b) = self._resolve(on.this, aliases), self._resolve(on.expression, aliases)
            if t_a.foreign_keys.get(c_a) == (t_b.name, c_b):
                fk_t, fk_c, pk_t, pk_c = t_a, c_a, t_b, c_b
            elif t_b.foreign_keys.get(c_b) == (t_a.name, c_a):
                fk_t, fk_c, pk_t, pk_c = t_b, c_b, t_a, c_a
            else:
                raise UnsupportedQuery(f"join {t_a.name}.{c_a} = {t_b.name}.{c_b} is not a tracked FK -> PK")
            if pk_c != pk_t.primary_key:
                raise UnsupportedQuery("join must target the primary key")
            if fk_t.is_public:
                raise UnsupportedQuery("public table on the FK side: max frequency unbounded")
            if not pk_t.is_public:
                if fk_c != fk_t.privacy_id:
                    raise UnsupportedQuery("FK column must be the privacy_id (max frequency otherwise unbounded)")
                if not pk_t.keyed_by_privacy_id:
                    raise UnsupportedQuery("private PK side must be keyed by its privacy_id (rows would leave the privacy unit)")
            rows = self._rows_owned(aliases[joined_alias])
            if rows == 0:  # public table on its primary key: each row matches once
                continue
            delta = delta * rows if delta else rows
        return delta

    def _select_layout(self, tree: exp.Select, aliases: dict[str, Table], rows_per_user: int):
        group_cols: list[tuple[str, str]] = []
        for g in (tree.args.get("group").expressions if tree.args.get("group") else []):
            if not isinstance(g, exp.Column):
                raise UnsupportedQuery("GROUP BY must list plain columns")
            t, c = self._resolve(g, aliases)
            if (t.name, c) in group_cols:
                raise UnsupportedQuery(f"GROUP BY lists {g.sql()} twice")
            group_cols.append((t.name, c))

        key_slots: dict[int, int] = {}  # GROUP BY index -> SELECT position
        aggregates = []
        next_extra = len(tree.expressions)  # COUNT(col) of each AVG is appended to the SELECT list
        for i, item in enumerate(tree.expressions):
            inner = item.this if isinstance(item, exp.Alias) else item
            if isinstance(inner, exp.Column):
                t, c = self._resolve(inner, aliases)
                if (t.name, c) not in group_cols:
                    raise UnsupportedQuery(f"non-aggregated column {inner.sql()} outside GROUP BY")
                slot = group_cols.index((t.name, c))
                if slot in key_slots:
                    raise UnsupportedQuery(f"SELECT lists {inner.sql()} twice")
                key_slots[slot] = i
            elif isinstance(inner, exp.Count):
                if isinstance(inner.this, exp.Distinct):
                    raise UnsupportedQuery("COUNT(DISTINCT) is not supported in the PoC")
                aggregates.append(Aggregate(i, "count", None, rows_per_user))
            elif isinstance(inner, (exp.Sum, exp.Avg)):
                name = type(inner).__name__.upper()
                if not isinstance(inner.this, exp.Column):
                    raise UnsupportedQuery(f"{name} argument must be a column")
                table, col = self._resolve(inner.this, aliases)
                try:
                    bound = table.column(col).bound
                except ValueError as e:
                    raise UnsupportedQuery(str(e)) from None
                if isinstance(inner, exp.Sum):
                    aggregates.append(Aggregate(i, "sum", col, rows_per_user * bound))
                else:
                    aggregates.append(Aggregate(i, "avg", col, rows_per_user * bound, next_extra, rows_per_user))
                    next_extra += 1
            elif isinstance(inner, exp.AggFunc):
                raise UnsupportedQuery(f"{type(inner).__name__} is not supported (use COUNT/SUM)")
            else:
                raise UnsupportedQuery(f"unsupported SELECT item {item.sql()}")
        if not aggregates:
            raise UnsupportedQuery("query must contain at least one aggregate")
        if len(key_slots) != len(group_cols):  # an unprojected key would merge groups in the keyed result
            raise UnsupportedQuery("every GROUP BY column must be in the SELECT list")
        return [key_slots[s] for s in range(len(group_cols))], aggregates, group_cols  # GROUP BY order = keyset order

    # -- mutation ---------------------------------------------------------

    def _expand_avgs(self, tree: exp.Select) -> None:
        """AVG(col) becomes SUM(col) in place plus COUNT(col) appended to the SELECT list.

        Walks the SELECT list in order, so the appended counts line up with
        the count_position each AVG was given in _select_layout.
        """
        for item in list(tree.expressions):
            node = item.this if isinstance(item, exp.Alias) else item
            if isinstance(node, exp.Avg):
                col = node.this
                node.replace(sum_of(col.copy()))
                tree.append("expressions", count_of(col.copy()))

    def _clamp_sums(self, tree: exp.Select, aliases: dict[str, Table]) -> None:
        for node in list(tree.find_all(exp.Sum)):
            table, col = self._resolve(node.this, aliases)
            c = table.column(col)
            node.set("this", clamp(node.this.copy(), c.lower, c.upper))

    def _truncate_tables(self, tree: exp.Select, aliases: dict[str, Table]) -> None:
        pushed = self._pushdown(tree, aliases)
        for node in list(tree.find_all(exp.Table)):
            table = self.schema.table(node.name)
            if not table.is_truncated:
                continue
            alias = node.alias_or_name
            physical = self.materialized.get(table.name)
            if physical is not None and not pushed.get(alias):
                # Nothing filters this table before truncation, so the materialized table holds the window's rows.
                node.replace(exp.table_(physical, alias=alias))
            else:  # a pushed conjunct changes which k rows are kept: only the window computes that
                node.replace(truncated(table, alias, self.k, conjunction(pushed.get(alias, []))))

    def _pushdown(self, tree: exp.Select, aliases: dict[str, Table]) -> dict[str, list[exp.Expression]]:
        """Split WHERE on top-level AND; conjuncts on exactly one truncated table move inside it.

        Runs after sensitivity analysis: pushdown changes which rows survive
        truncation, never how many a privacy unit can own.
        """
        where = tree.args.get("where")
        if where is None:
            return {}
        pushed: dict[str, list[exp.Expression]] = {}
        kept = []
        condition = where.this
        for conjunct in (condition.flatten() if isinstance(condition, exp.And) else [condition]):
            owners = {self._owner_alias(c, aliases) for c in conjunct.find_all(exp.Column)}
            alias = next(iter(owners)) if len(owners) == 1 else None
            if alias is not None and aliases[alias].is_truncated:
                inner = conjunct.copy()
                for c in inner.find_all(exp.Column):  # the truncation subquery reads the bare table
                    c.set("table", None)
                pushed.setdefault(alias, []).append(inner)
            else:
                kept.append(conjunct.copy())
        set_where(tree, conjunction(kept))
        return pushed

    def _owner_alias(self, col: exp.Column, aliases: dict[str, Table]) -> str:
        if col.table:
            return col.table
        table, _ = self._resolve(col, aliases)
        # A table under two aliases makes every unqualified column ambiguous in _resolve,
        # so an unqualified column here always has exactly one alias.
        return next(a for a, t in aliases.items() if t is table)
