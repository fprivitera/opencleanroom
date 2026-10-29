# SPDX-License-Identifier: Apache-2.0
"""Backends run rewritten SQL. Contract kept small (technical design §6): a dialect and a connection.

DuckDB is opened read-only in process; Spark is reached through Spark Connect (ADR 0007). Each hands a
session a connection that runs one SELECT over tables and nothing else.
"""
from __future__ import annotations

import json
import math
import re
import threading
import uuid
from typing import Protocol

import duckdb
import sqlglot
from cleanroom_sql import UnsupportedQuery
from cleanroom_sql.rewriter import ALLOWED_NODES
from sqlglot import exp

MIN_QUERY_TIMEOUT, MAX_QUERY_TIMEOUT = 0.01, 86_400.0  # seconds; outside this a timer either never lands or never fires


def checked_timeout(seconds: float) -> float:
    if not (isinstance(seconds, (int, float)) and math.isfinite(seconds) and MIN_QUERY_TIMEOUT <= seconds <= MAX_QUERY_TIMEOUT):
        raise ValueError(f"the query timeout must be between {MIN_QUERY_TIMEOUT} and {MAX_QUERY_TIMEOUT:g} seconds, got {seconds!r}")
    return float(seconds)


class _Watchdog:
    """Interrupts a running query after `seconds`, and keeps interrupting until it stops.

    An interrupt sent before the engine has started the query is dropped, so one shot is not enough. `stop()` joins
    the thread: no interrupt can land after the query returned, on the next query of the same connection.
    """

    def __init__(self, seconds: float, interrupt):
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(seconds, interrupt), daemon=True)
        self._thread.start()

    def _run(self, seconds: float, interrupt) -> None:
        if self._done.wait(seconds):
            return
        while not self._done.is_set():
            interrupt()
            self._done.wait(0.05)

    def stop(self) -> None:
        self._done.set()
        self._thread.join()


class Backend(Protocol):
    dialect: str

    def session_con(self) -> object:
        """A connection for one session: DB-API-like, .execute(sql).fetchall(); safe to use on its own thread."""


class DuckDBBackend:
    dialect = "duckdb"

    # read_only stops writes; the config stops reads outside the file, extension downloads and autoloading,
    # and lock_configuration stops any SET from undoing it.
    CONFIG = {"enable_external_access": False, "autoinstall_known_extensions": False, "autoload_known_extensions": False,
              "lock_configuration": True}

    def __init__(self, path: str, query_timeout: float = 30.0):
        self.query_timeout = checked_timeout(query_timeout)
        self.con = duckdb.connect(path, read_only=True, config=self.CONFIG)

    def session_con(self):
        return _SelectOnly(self.con.cursor(), self.query_timeout)  # a DuckDB cursor is an independent connection

    def admin_sql(self, sql: str) -> list[tuple]:
        """Trusted admin path (key checks); the file is read-only, so nothing can be created through it."""
        return self.con.cursor().execute(sql).fetchall()

    def close(self) -> None:
        self.con.close()


class _SelectOnly:
    """The connection a session gets: one SELECT made of what the rewriter emits, checked by DuckDB's parser.

    The rewriter's whitelist is the first gate; this one holds if it ever lets something through. It is an allow-list:
    one statement of type SELECT, every node of its parsed tree a kind the rewriter produces, every function
    one the rewriter calls, every table a bare name. Anything else, known or not, fails.
    """

    def __init__(self, cursor, query_timeout: float):
        self._cursor = cursor
        self._timeout = checked_timeout(query_timeout)
        self._rows: list[tuple] = []

    def execute(self, sql: str, parameters=None):
        check_duckdb_select(sql, self._cursor)
        watchdog = _Watchdog(self._timeout, self._cursor.interrupt)
        try:
            result = self._cursor.execute(sql) if parameters is None else self._cursor.execute(sql, parameters)
            self._rows = result.fetchall()
        finally:
            watchdog.stop()
        return self

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


# Node kinds and functions in DuckDB's serialized tree of every query the rewriter can produce: the truncation window,
# the NULL-propagating clamp, the consumer's comparisons, the public rows path. Every golden must pass (test_backend_guards).
_DUCKDB_NODES = frozenset({
    "SELECT_NODE", "SUBQUERY", "BASE_TABLE", "JOIN", "STAR", "COLUMN_REF", "VALUE_CONSTANT", "FUNCTION", "CASE_EXPR",
    "WINDOW_ROW_NUMBER", "ORDER_MODIFIER", "ORDER_DEFAULT", "DISTINCT_MODIFIER", "LIMIT_MODIFIER",
    "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "OPERATOR_NOT", "CONJUNCTION_AND", "CONJUNCTION_OR",
    "COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_LESSTHAN", "COMPARE_GREATERTHAN", "COMPARE_LESSTHANOREQUALTO",
    "COMPARE_GREATERTHANOREQUALTO", "COMPARE_IN", "COMPARE_BETWEEN",
})
_DUCKDB_FUNCTIONS = frozenset({"count", "count_star", "sum", "greatest", "least", "row_number"})
_BARE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Names an engine resolves to a function when no column carries them; an unqualified one is refused (a table column of
# that name would have to be referenced through its table alias, which the rewriter does).
_PSEUDO_COLUMNS = frozenset({"user", "current_user", "session_user", "current_role", "current_catalog", "current_schema",
                             "current_database", "current_date", "current_time", "current_timestamp", "localtime", "localtimestamp"})


def check_duckdb_select(sql: str, con) -> None:
    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error:
        raise UnsupportedQuery("the backend could not parse the statement") from None
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise UnsupportedQuery("the backend runs one SELECT statement only")
    tree = json.loads(con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])
    if tree.get("error"):
        raise UnsupportedQuery("the backend could not parse the statement")
    for node in _duckdb_nodes(tree["statements"]):
        if node.get("cte_map", {}).get("map"):
            raise UnsupportedQuery("the backend does not run WITH")
        kind = node["type"]
        if kind not in _DUCKDB_NODES:
            raise UnsupportedQuery(f"the backend does not run {kind}")
        if kind == "FUNCTION" and node.get("function_name") not in _DUCKDB_FUNCTIONS:
            raise UnsupportedQuery("the backend does not run that function")
        if kind == "BASE_TABLE" and (node.get("schema_name") or node.get("catalog_name") or not _BARE_NAME.fullmatch(node.get("table_name", ""))):
            raise UnsupportedQuery("the backend reads tables of its own database only")
        if kind == "COLUMN_REF" and len(node.get("column_names", [])) == 1 and node["column_names"][0].lower() in _PSEUDO_COLUMNS:
            raise UnsupportedQuery("the backend does not run that function")


def _duckdb_nodes(node):
    """Every dict in DuckDB's serialized tree that names a node kind."""
    if isinstance(node, dict):
        if isinstance(node.get("type"), str):
            yield node
        for v in node.values():
            yield from _duckdb_nodes(v)
    elif isinstance(node, list):
        for v in node:
            yield from _duckdb_nodes(v)


class SparkBackend:
    """A Spark Connect server, `sc://host:port`; `;database=<name>` in the URL sets the current database."""

    dialect = "spark"

    def __init__(self, url: str, query_timeout: float = 30.0):
        from pyspark.sql.connect.session import SparkSession  # the `spark` extra; DuckDB-only installs never import it

        self._builder = SparkSession.builder
        self._remote, self._database = split_database(url)
        self._lock = threading.Lock()
        self.query_timeout = checked_timeout(query_timeout)
        self.spark = self._connect()

    def _connect(self):
        spark = self._builder.remote(self._remote).create()  # its own session: two backends never share state
        if self._database:
            spark.catalog.setCurrentDatabase(self._database)
        return spark

    def close(self) -> None:
        """Stop the session explicitly: the 3.5 client can deadlock when a session is left to the garbage collector."""
        with self._lock:
            self.spark.stop()

    def admin_sql(self, sql: str) -> list[tuple]:
        """Trusted admin path (materialization, key checks): no guard, never reachable from a consumer request."""
        with self._lock:
            if self.spark.is_stopped:
                self.spark = self._connect()
            spark = self.spark
        return [tuple(r) for r in spark.sql(sql).collect()]

    def session_con(self):
        # A server restart or an idle timeout closes the client for good; a new session replaces it, so one restart
        # does not turn every later query into a charged failure.
        with self._lock:
            if self.spark.is_stopped:
                self.spark = self._connect()
            spark = self.spark
        return SparkSelectOnly(spark, self.query_timeout)


class SparkSelectOnly:
    """The connection a session gets on Spark: one SELECT made of what the rewriter emits.

    Checked with sqlglot in the Spark dialect before anything reaches the server, as an allow-list: every node is a
    kind the rewriter produces and every table a bare name. Script transforms (TRANSFORM ... USING), functions that
    call Java or read server state, files read as tables, other databases, CTEs and set operations all fail, as does
    any node nobody has thought of yet.
    """

    def __init__(self, spark, query_timeout: float):
        self._spark = spark
        self._timeout = checked_timeout(query_timeout)
        self._rows: list[tuple] = []

    def execute(self, sql: str):
        check_spark_select(sql)
        # the tag marks this thread's operations (tags are per thread); the watchdog interrupts them on the server
        tag = f"ocr-{uuid.uuid4().hex}"
        self._spark.addTag(tag)
        watchdog = _Watchdog(self._timeout, lambda: self._spark.interruptTag(tag))
        try:
            self._rows = [tuple(r) for r in self._spark.sql(sql).collect()]
        finally:
            watchdog.stop()
            self._spark.removeTag(tag)
        return self

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


# The consumer's expressions (the rewriter's own whitelist) plus what the rewriter adds around them: the truncation
# window, the NULL-propagating clamp, the public rows path. Every golden must pass (test_backend_guards).
_SPARK_NODES = ALLOWED_NODES | frozenset({
    exp.Subquery, exp.Window, exp.RowNumber, exp.Order, exp.Ordered, exp.Case, exp.If, exp.Least, exp.Greatest,
    exp.Distinct, exp.Limit,
})


def check_spark_select(sql: str) -> None:
    try:
        trees = [t for t in sqlglot.parse(sql, read="spark") if t is not None]
    except sqlglot.errors.SqlglotError:
        raise UnsupportedQuery("the backend could not parse the statement") from None
    if len(trees) != 1 or not isinstance(trees[0], exp.Select):
        raise UnsupportedQuery("the backend runs one SELECT statement only")
    for node in trees[0].walk():
        if type(node) not in _SPARK_NODES:
            raise UnsupportedQuery(f"the backend does not run {type(node).__name__}")
        if isinstance(node, exp.Table) and (not isinstance(node.this, exp.Identifier) or node.args.get("db") or node.args.get("catalog")):
            raise UnsupportedQuery("the backend reads tables of its own database only")
        if isinstance(node, exp.Column) and not node.table and node.name.lower() in _PSEUDO_COLUMNS:
            raise UnsupportedQuery("the backend does not run that function")


_DATABASE = re.compile(r";database=([^;]*)")


def split_database(url: str) -> tuple[str, str | None]:
    """`sc://h:15002/;database=nyc;token=t` -> (`sc://h:15002/;token=t`, `nyc`); the rest goes to Spark Connect."""
    match = _DATABASE.search(url)
    if match is None:
        return url, None
    return url[:match.start()] + url[match.end():], match.group(1) or None


def open_backend(kind: str, location: str, query_timeout: float = 30.0) -> Backend:
    if kind == "duckdb":
        return DuckDBBackend(location, query_timeout)
    if kind == "spark":
        return SparkBackend(location, query_timeout)
    raise ValueError(f"unknown backend kind {kind!r}")
