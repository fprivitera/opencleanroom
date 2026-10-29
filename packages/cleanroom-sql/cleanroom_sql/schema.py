# SPDX-License-Identifier: Apache-2.0
"""Schema metadata: the only thing the rewriter knows about the data.

Values are integers (e.g. amounts in cents) so that OpenDP discrete
Laplace applies without floating-point side channels.
"""
from __future__ import annotations

from dataclasses import dataclass, field

COLUMN_TYPES = ("int", "string", "bool", "date")


@dataclass(frozen=True)
class Column:
    name: str
    lower: int | None = None  # clamping bound for int columns
    upper: int | None = None
    type: str = "int"  # one of COLUMN_TYPES; only int columns can be aggregated

    def __post_init__(self) -> None:
        if self.type not in COLUMN_TYPES:
            raise ValueError(f"column {self.name}: unknown type {self.type!r}")

    @property
    def bound(self) -> int:
        if self.type != "int":
            raise ValueError(f"column {self.name} is {self.type}, only int columns can be aggregated")
        if self.lower is None or self.upper is None:
            raise ValueError(f"column {self.name} has no clamping bounds")
        return max(abs(self.lower), abs(self.upper))


@dataclass(frozen=True)
class Table:
    name: str
    primary_key: str
    columns: tuple[Column, ...]
    # column identifying the privacy unit; None = public table (no privacy)
    privacy_id: str | None = None
    # column -> (referenced table, referenced column)
    foreign_keys: dict[str, tuple[str, str]] = field(default_factory=dict)
    # unique column that orders rows inside a privacy unit before truncation; default the primary key
    order_by: str | None = None  # None at construction means the primary key; never None afterwards

    def __post_init__(self) -> None:
        if self.order_by is None:
            object.__setattr__(self, "order_by", self.primary_key)
        try:
            self.column(self.order_by)
        except KeyError:
            raise ValueError(f"{self.name}: order_by {self.order_by!r} is not a declared column") from None

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.name}.{name}")

    @property
    def is_public(self) -> bool:
        return self.privacy_id is None

    @property
    def keyed_by_privacy_id(self) -> bool:
        """One row per privacy unit: no truncation needed, Δ_R = 1."""
        return self.privacy_id is not None and self.primary_key == self.privacy_id

    @property
    def is_truncated(self) -> bool:
        """A private table that may hold many rows per privacy unit: truncated to k rows."""
        return not (self.is_public or self.keyed_by_privacy_id)


@dataclass(frozen=True)
class Schema:
    tables: dict[str, Table]

    def table(self, name: str) -> Table:
        try:
            return self.tables[name]
        except KeyError:
            raise KeyError(f"unknown table {name}") from None
