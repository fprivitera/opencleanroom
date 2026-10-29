# SPDX-License-Identifier: Apache-2.0
"""Policy file: what is true about the data, and what each consumer may do.

`load_policy` reads YAML, validates it against `policy.schema.json`, then
applies the semantic rules the rewriter and executor rely on. Every fault
is collected; one `PolicyError` lists them all with their YAML path. The
loader is the only place that builds a `Schema`, `k` and keysets from a
file; the rewriter never sees YAML.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from fractions import Fraction
from importlib import resources
from pathlib import Path
from typing import Any

import jsonschema
import yaml

from .schema import Column, Schema, Table

AGGREGATES = ("count", "sum", "avg")
SENSITIVITY_LIMIT = 2**63  # i64: rows_per_unit × bound must stay below it


class PolicyError(ValueError):
    """Every fault of a policy file, as (yaml_path, message) pairs."""

    def __init__(self, errors: list[tuple[str, str]]):
        self.errors = errors
        super().__init__("\n".join(f"{path}: {message}" for path, message in errors))


@dataclass(frozen=True)
class KeysetRef:
    """A keyset read from a public column at query time."""

    table: str
    column: str


@dataclass(frozen=True)
class Dataset:
    name: str
    schema: Schema
    k: int
    budget: Fraction
    keysets: dict[tuple[str, str], list[Any] | KeysetRef]
    dialect: str = "duckdb"
    backend: str | None = None  # the platform's backend name; unused by the library
    epoch: str | None = None  # budget epoch label (ADR 0004); the platform enforces it


@dataclass(frozen=True)
class ConsumerPolicy:
    name: str
    tables: frozenset[str]
    aggregates: frozenset[str]
    max_epsilon_per_query: Fraction
    cap: Fraction | None  # consumer cap, drawn from the dataset budget


@dataclass(frozen=True, eq=False)  # identity: one loaded policy is one dataset with one ledger
class Policy:
    dataset: Dataset
    consumers: dict[str, ConsumerPolicy]


def _fraction(value: Any) -> Fraction:
    """Exact value of the decimal text the admin wrote; a float is read from its repr, never its binary value."""
    return Fraction(str(value))


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses a mapping key written twice instead of keeping the last value."""

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    None, None, f"duplicate key {key!r}", key_node.start_mark)
            seen.add(key)
        return super().construct_mapping(node, deep)


def load_policy(source: str | os.PathLike) -> Policy:
    return loads_policy(Path(source).read_text())


def loads_policy(text: str) -> Policy:
    try:
        doc = yaml.load(text, Loader=_StrictLoader)
    except yaml.YAMLError as e:
        raise PolicyError([("", f"invalid YAML: {e}")]) from None
    with resources.files(__package__).joinpath("policy.schema.json").open() as f:
        schema = json.load(f)
    validator = jsonschema.Draft202012Validator(schema)
    errors = [(_path(e), e.message) for e in sorted(validator.iter_errors(doc), key=lambda e: list(map(str, e.absolute_path)))]
    if errors:  # the semantic rules need the shape the schema guarantees
        raise PolicyError(errors)
    return _Builder(doc).build()


def _path(error: jsonschema.ValidationError) -> str:
    return ".".join(str(p) for p in error.absolute_path)


class _Builder:
    def __init__(self, doc: dict):
        self.doc = doc
        self.errors: list[tuple[str, str]] = []

    def fail(self, path: str, message: str) -> None:
        self.errors.append((path, message))

    def epsilon(self, value: Any, path: str) -> Fraction:
        try:
            eps = _fraction(value)
        except (ValueError, ZeroDivisionError):
            self.fail(path, f"{value!r} is not a finite positive decimal")
            return Fraction(0)
        if eps <= 0:
            self.fail(path, f"{value!r} is not positive")
        return eps

    def build(self) -> Policy:
        ds = self.doc["dataset"]
        tables = self._tables(ds)
        keysets = self._keysets(ds, tables)
        budget = self.epsilon(ds["budget"]["epsilon"], "dataset.budget.epsilon")
        consumers = {name: self._consumer(name, c, tables, budget) for name, c in self.doc["consumers"].items()}
        if self.errors:
            raise PolicyError(self.errors)
        dataset = Dataset(
            name=ds["name"], schema=Schema(tables=tables), k=ds["truncation"]["k"], budget=budget, keysets=keysets,
            dialect=ds.get("dialect", "duckdb"), backend=ds.get("backend"), epoch=ds["budget"].get("epoch"),
        )
        return Policy(dataset=dataset, consumers=consumers)

    # -- dataset ----------------------------------------------------------

    def _tables(self, ds: dict) -> dict[str, Table]:
        k = ds["truncation"]["k"]
        unit_column = ds["privacy_unit"]["column"]
        tables: dict[str, Table] = {}
        for name, t in ds["tables"].items():
            at = f"dataset.tables.{name}"
            columns = t["columns"]
            public = t.get("public", False)
            cols = []
            for cname, c in columns.items():
                lower = upper = None
                if "clamp" in c:
                    if c["type"] != "int":
                        self.fail(f"{at}.columns.{cname}.clamp", "clamp bounds apply to int columns only")
                    else:
                        lower, upper = c["clamp"]
                        if lower > upper:
                            self.fail(f"{at}.columns.{cname}.clamp", "lower bound above upper bound")
                        if k * max(abs(lower), abs(upper)) >= SENSITIVITY_LIMIT:
                            self.fail(f"{at}.columns.{cname}.clamp", f"k × max(|a|,|b|) reaches {SENSITIVITY_LIMIT}")
                cols.append(Column(cname, lower, upper, type=c["type"]))
            pk = t["primary_key"]
            if pk not in columns:
                self.fail(f"{at}.primary_key", f"{pk} is not a declared column")
            privacy_id = None
            if not public:
                privacy_id = t.get("privacy_id", unit_column)
                if privacy_id not in columns:
                    self.fail(at, f"private table has no privacy ID column {privacy_id}; declare it or set privacy_id")
            elif "privacy_id" in t:
                self.fail(f"{at}.privacy_id", "a public table has no privacy ID")
            order_by = t.get("order_by", pk)
            if order_by != pk and not columns.get(order_by, {}).get("unique", False):
                self.fail(f"{at}.order_by", f"{order_by} is neither the primary key nor a column marked unique: true")
            if pk in columns and order_by in columns:
                tables[name] = Table(
                    name=name, primary_key=pk, columns=tuple(cols), privacy_id=privacy_id,
                    foreign_keys={c: tuple(ref.split(".", 1)) for c, ref in t.get("foreign_keys", {}).items()},
                    order_by=order_by,
                )
        self._foreign_keys(ds, tables)
        self._privacy_units(ds, tables, unit_column)
        return tables

    def _privacy_units(self, ds: dict, tables: dict[str, Table], unit_column: str) -> None:
        """Every private table must reach, through privacy-ID foreign keys, a table whose privacy ID is the dataset's.

        Otherwise a `privacy_id` override could rename the privacy unit to an
        unrelated column (for example a row ID) and shrink the sensitivity.
        """
        private = [t for t in tables.values() if not t.is_public]
        parent = {t.name: t.name for t in private}

        def root(n: str) -> str:
            while parent[n] != n:
                n = parent[n]
            return n

        for t in private:
            for col, (target_name, target_col) in t.foreign_keys.items():
                target = tables.get(target_name)
                if target is not None and not target.is_public and col == t.privacy_id and target_col == target.privacy_id:
                    parent[root(t.name)] = root(target_name)
        anchored = {root(t.name) for t in private if t.privacy_id == unit_column}
        for t in private:
            if root(t.name) not in anchored:
                self.fail(f"dataset.tables.{t.name}", f"privacy_id {t.privacy_id} is not linked by foreign keys to the privacy unit column {unit_column}")

    def _foreign_keys(self, ds: dict, tables: dict[str, Table]) -> None:
        for name, t in ds["tables"].items():
            at = f"dataset.tables.{name}.foreign_keys"
            table = tables.get(name)
            for col, ref in t.get("foreign_keys", {}).items():
                target_name, target_col = ref.split(".", 1)
                if col not in t["columns"]:
                    self.fail(f"{at}.{col}", f"{col} is not a declared column")
                    continue
                if t.get("public", False):
                    self.fail(f"{at}.{col}", "a public table cannot be on the foreign-key side (max frequency unbounded)")
                    continue
                target = tables.get(target_name)
                if target is None:
                    self.fail(f"{at}.{col}", f"unknown table {target_name}")
                    continue
                if target_col != target.primary_key:
                    self.fail(f"{at}.{col}", f"{ref} is not the primary key of {target_name}")
                if table is not None and not target.is_public:
                    if col != table.privacy_id:
                        self.fail(f"{at}.{col}", "a foreign key between private tables must be the privacy ID")
                    if target_col != target.privacy_id:
                        self.fail(f"{at}.{col}", f"{ref} is not the privacy ID of {target_name}: the keyed side must be keyed by its privacy ID")

    def _keysets(self, ds: dict, tables: dict[str, Table]) -> dict[tuple[str, str], list[Any] | KeysetRef]:
        out: dict[tuple[str, str], list[Any] | KeysetRef] = {}
        for key, spec in ds.get("keysets", {}).items():
            at = f"dataset.keysets.{key}"
            tname, cname = key.split(".", 1)
            if not self._declared(tables, tname, cname):
                self.fail(at, f"{key} is not a declared column")
                continue
            if isinstance(spec, dict):
                ftable, fcol = spec["from"].split(".", 1)
                if not self._declared(tables, ftable, fcol) or not tables[ftable].is_public:
                    self.fail(at, f"a keyset must come from a public table column, not {spec['from']} (ADR 0005)")
                    continue
                out[(tname, cname)] = KeysetRef(ftable, fcol)
            else:
                out[(tname, cname)] = list(spec)
        return out

    @staticmethod
    def _declared(tables: dict[str, Table], tname: str, cname: str) -> bool:
        return tname in tables and any(c.name == cname for c in tables[tname].columns)

    # -- consumers --------------------------------------------------------

    def _consumer(self, name: str, c: dict, tables: dict[str, Table], budget: Fraction) -> ConsumerPolicy:
        at = f"consumers.{name}"
        unknown = [t for t in c["tables"] if t not in tables]
        if unknown:
            self.fail(f"{at}.tables", f"unknown tables {unknown}")
        bad = [a for a in c["aggregates"] if a not in AGGREGATES]
        if bad:
            self.fail(f"{at}.aggregates", f"unknown aggregates {bad}; allowed {list(AGGREGATES)}")
        cap = self.epsilon(c["budget"]["epsilon"], f"{at}.budget.epsilon") if "budget" in c else None
        if cap is not None and cap > budget:
            self.fail(f"{at}.budget.epsilon", f"consumer cap {cap} exceeds the dataset budget {budget}")
        per_query = self.epsilon(c["max_epsilon_per_query"], f"{at}.max_epsilon_per_query")
        ceiling = cap if cap is not None else budget
        if per_query > ceiling:
            self.fail(f"{at}.max_epsilon_per_query", f"{per_query} exceeds {'the consumer cap' if cap is not None else 'the dataset budget'} {ceiling}")
        return ConsumerPolicy(name, frozenset(c["tables"]), frozenset(c["aggregates"]), per_query, cap)
