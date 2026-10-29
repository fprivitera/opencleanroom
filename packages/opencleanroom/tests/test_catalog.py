# SPDX-License-Identifier: Apache-2.0
"""The catalog: backends, applied policies under the ADR 0004 rule, consumers with hashed keys."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import yaml
from cleanroom_sql import PolicyError
from opencleanroom import catalog

TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()


def _policy_text(**changes) -> str:
    doc = yaml.safe_load(TAXI_YAML)
    for dotted, value in changes.items():
        node = doc
        *parents, last = dotted.split(".")
        for p in parents:
            node = node[p]
        if value is None:
            node.pop(last)
        else:
            node[last] = value
    return yaml.safe_dump(doc, sort_keys=False)


@pytest.fixture
def cat(tmp_path):
    path = tmp_path / "catalog.sqlite"
    catalog.init(path)
    catalog.add_backend(path, "duckdb-dev", "duckdb", str(tmp_path / "taxi.duckdb"))
    return path


def test_init_creates_an_empty_catalog(tmp_path):
    path = tmp_path / "c.sqlite"
    catalog.init(path)
    assert catalog.list_backends(path) == [] and catalog.list_policies(path) == []
    catalog.init(path)  # idempotent


def test_backend_add_and_list(cat):
    assert [(b.name, b.kind) for b in catalog.list_backends(cat)] == [("duckdb-dev", "duckdb")]


def test_apply_stores_text_and_versions(cat):
    a = catalog.apply_policy(cat, _policy_text())
    assert (a.dataset, a.version, a.epoch) == ("nyc_taxi", 1, "2026-10")
    b = catalog.apply_policy(cat, _policy_text(**{"dataset.truncation.k": 40}))
    assert b.version == 2
    stored = catalog.get_policy(cat, "nyc_taxi")
    assert stored.version == 2 and "k: 40" in stored.text and len(stored.sha256) == 64
    assert [p.version for p in catalog.policy_history(cat, "nyc_taxi")] == [1]


def test_apply_rejects_a_raised_budget_in_the_same_epoch(cat):
    catalog.apply_policy(cat, _policy_text())
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epsilon": "12.0"}))
    assert [p for p, _ in e.value.errors] == ["dataset.budget.epsilon"]
    assert catalog.get_policy(cat, "nyc_taxi").version == 1


def test_apply_accepts_a_raised_budget_in_a_new_epoch(cat):
    catalog.apply_policy(cat, _policy_text())
    catalog.new_epoch(cat, "nyc_taxi", "2026-11", "snap-2026-11", "monthly renewal")
    a = catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epsilon": "12.0", "dataset.budget.epoch": "2026-11"}))
    assert a.version == 2 and a.epoch == "2026-11"


def test_apply_accepts_a_lowered_budget_in_the_same_epoch(cat):
    catalog.apply_policy(cat, _policy_text())
    catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epsilon": "8.0", "consumers.analytics-team.budget.epsilon": 4}))


def test_apply_requires_an_epoch(cat):
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": None}))
    assert [p for p, _ in e.value.errors] == ["dataset.budget.epoch"]


def test_apply_requires_a_registered_backend(cat):
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"dataset.backend": "nope"}))
    assert [p for p, _ in e.value.errors] == ["dataset.backend"]


def test_apply_reports_loader_errors_with_paths(cat):
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"consumers.analytics-team.aggregates": ["max"]}))
    assert "consumers.analytics-team.aggregates" in [p for p, _ in e.value.errors]


def test_consumer_key_is_stored_hashed_and_resolves(cat):
    catalog.apply_policy(cat, _policy_text())
    key = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    assert key.startswith("ocr_") and len(key) > 40
    assert catalog.resolve_key(cat, key) == ("nyc_taxi", "analytics-team")
    raw = sqlite3.connect(cat).execute("SELECT key_sha256 FROM consumers").fetchall()
    assert raw and all(key not in r[0] and len(r[0]) == 64 for r in raw)
    assert key.encode() not in Path(cat).read_bytes()


def test_consumer_name_must_be_in_the_policy(cat):
    catalog.apply_policy(cat, _policy_text())
    with pytest.raises(ValueError, match="nobody"):
        catalog.create_consumer(cat, "nyc_taxi", "nobody")


def test_create_on_an_active_consumer_fails(cat):
    catalog.apply_policy(cat, _policy_text())
    catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    with pytest.raises(ValueError, match="active"):
        catalog.create_consumer(cat, "nyc_taxi", "analytics-team")


def test_revoke_then_recreate_keeps_history(cat):
    catalog.apply_policy(cat, _policy_text())
    k1 = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    catalog.revoke_consumer(cat, "nyc_taxi", "analytics-team")
    assert catalog.resolve_key(cat, k1) is None
    k2 = catalog.create_consumer(cat, "nyc_taxi", "analytics-team")
    assert k2 != k1 and catalog.resolve_key(cat, k2) == ("nyc_taxi", "analytics-team")
    rows = catalog.list_consumers(cat, "nyc_taxi")
    assert [(r.name, r.revoked_at is None) for r in rows] == [("analytics-team", False), ("analytics-team", True)]


def test_unknown_key_does_not_resolve(cat):
    assert catalog.resolve_key(cat, "ocr_nothing") is None


def test_apply_dropping_a_consumer_with_an_active_key_is_rejected(cat):
    catalog.apply_policy(cat, _policy_text())
    catalog.create_consumer(cat, "nyc_taxi", "counts-only")
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"consumers.counts-only": None}))
    assert [p for p, _ in e.value.errors] == ["consumers"]
    catalog.revoke_consumer(cat, "nyc_taxi", "counts-only")
    catalog.apply_policy(cat, _policy_text(**{"consumers.counts-only": None}))


# -- review findings ---------------------------------------------------------------

def test_apply_cannot_raise_a_budget_by_returning_to_an_old_epoch(cat):
    """ADR 0004: the ceiling of an epoch is the largest budget ever applied in it, current or past."""
    catalog.apply_policy(cat, _policy_text())  # 2026-10, 10.0
    catalog.new_epoch(cat, "nyc_taxi", "2026-11", "snap-2026-11", "monthly renewal")
    catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": "2026-11", "dataset.budget.epsilon": "1.0",
                                              "consumers.analytics-team.budget.epsilon": 1, "consumers.analytics-team.max_epsilon_per_query": 0.5}))
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epsilon": "100.0"}))  # back to 2026-10
    assert [p for p, _ in e.value.errors] == ["dataset.budget.epoch"]  # an older label is refused before the ceiling is looked at
    with pytest.raises(PolicyError):
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epsilon": "10.0"}))  # the unchanged git file of the old epoch: refused too


def test_epoch_strings_are_stripped(cat):
    catalog.apply_policy(cat, _policy_text())
    with pytest.raises(PolicyError):
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": " 2026-10 ", "dataset.budget.epsilon": "12.0"}))


def test_second_writer_sees_the_lock_error_not_a_rollback_error(cat, tmp_path):
    holder = sqlite3.connect(cat, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            catalog.add_backend(cat, "other", "duckdb", str(tmp_path / "o.duckdb"), timeout=0.2)
    finally:
        holder.execute("ROLLBACK")
        holder.close()


def test_duplicate_backend_is_a_value_error(cat, tmp_path):
    with pytest.raises(ValueError, match="duckdb-dev"):
        catalog.add_backend(cat, "duckdb-dev", "duckdb", str(tmp_path / "x.duckdb"))


def test_stored_budget_is_a_fraction(cat):
    from fractions import Fraction
    catalog.apply_policy(cat, _policy_text())
    assert catalog.get_policy(cat, "nyc_taxi").budget == Fraction(10)


# -- a renewal is a written decision (ADR 0004) -------------------------------------

def test_new_epoch_is_recorded_before_the_policy_moves(cat):
    catalog.apply_policy(cat, _policy_text())
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": "2026-11"}))
    assert [p for p, _ in e.value.errors] == ["dataset.budget.epoch"] and "new-epoch" in e.value.errors[0][1]
    row = catalog.new_epoch(cat, "nyc_taxi", "2026-11", "snap-42", "monthly renewal")
    assert (row.epoch, row.snapshot, row.reason) == ("2026-11", "snap-42", "monthly renewal")
    assert catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": "2026-11"})).epoch == "2026-11"
    assert [r.epoch for r in catalog.epoch_log(cat, "nyc_taxi")] == ["2026-11"]


def test_new_epoch_rejects_nonsense(cat):
    with pytest.raises(ValueError):
        catalog.new_epoch(cat, "nyc_taxi", "2026-11", "s", "r")  # no policy yet: the first apply carries its own epoch
    catalog.apply_policy(cat, _policy_text())
    for epoch, snapshot, reason in [("2026-09", "s", "r"), ("2026-10", "s", "r"), ("", "s", "r"), ("2026-11", "", "r"), ("2026-11", "s", " ")]:
        with pytest.raises(ValueError):
            catalog.new_epoch(cat, "nyc_taxi", epoch, snapshot, reason)
    catalog.new_epoch(cat, "nyc_taxi", "2026-11", "s", "r")
    with pytest.raises(ValueError):
        catalog.new_epoch(cat, "nyc_taxi", "2026-11", "s", "again")


def test_older_epoch_is_refused_even_when_recorded(cat):
    catalog.apply_policy(cat, _policy_text())
    catalog.new_epoch(cat, "nyc_taxi", "2026-11", "s", "r")
    catalog.new_epoch(cat, "nyc_taxi", "2026-12", "s", "r")
    catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": "2026-12"}))
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": "2026-11"}))
    assert "older" in e.value.errors[0][1]
    assert catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": "2026-12", "dataset.truncation.k": 40})).version == 3


def test_epoch_labels_are_plain_text(cat):
    catalog.apply_policy(cat, _policy_text())
    for bad in ["2026-10\u200b", "2026 11", "é", "-x", "a/b"]:
        with pytest.raises(ValueError):
            catalog.new_epoch(cat, "nyc_taxi", bad, "s", "r")
    with pytest.raises(PolicyError) as e:
        catalog.apply_policy(cat, _policy_text(**{"dataset.budget.epoch": "2026-10\u200b", "dataset.budget.epsilon": "100.0"}))
    assert [p for p, _ in e.value.errors] == ["dataset.budget.epoch"]
    catalog.new_epoch(cat, "nyc_taxi", "2026-11.r2_x", "s", "r")  # letters, digits, dot, underscore, dash
