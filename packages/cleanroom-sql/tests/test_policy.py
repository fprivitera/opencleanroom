# SPDX-License-Identifier: Apache-2.0
"""The policy loader: a YAML file becomes a Policy, or one PolicyError that lists every fault with its path."""
from __future__ import annotations

import copy
from fractions import Fraction

import pytest
import yaml
from cleanroom_sql import PolicyError, loads_policy
from cleanroom_sql.taxi import SCHEMA

VALID = yaml.safe_load("""
version: 1
dataset:
  name: nyc_taxi
  backend: duckdb-dev
  privacy_unit: {column: hack_license}
  truncation: {k: 50}
  tables:
    taxi_zones:
      public: true
      primary_key: location_id
      columns: {location_id: {type: int}, borough: {type: string}, zone: {type: string}}
    drivers:
      primary_key: hack_license
      columns: {hack_license: {type: string}, fleet: {type: string}}
    trips:
      primary_key: trip_id
      foreign_keys: {hack_license: drivers.hack_license, pu_location_id: taxi_zones.location_id}
      columns:
        trip_id: {type: int}
        hack_license: {type: string}
        pu_location_id: {type: int}
        fare_amount: {type: int, clamp: [0, 20000]}
        trip_distance: {type: int, clamp: [0, 500]}
  budget: {epsilon: "10.0", epoch: "2026-10"}
  keysets:
    taxi_zones.borough: {from: taxi_zones.borough}
    drivers.fleet: [medallion, fleet_A, fleet_B]
consumers:
  analytics-team:
    tables: [trips, taxi_zones, drivers]
    aggregates: [count, sum, avg]
    max_epsilon_per_query: 0.5
    budget: {epsilon: 5.0}
""")


def _policy(mutate=None) -> str:
    """The valid policy as YAML text, after an in-place mutation of the dict."""
    doc = copy.deepcopy(VALID)
    if mutate:
        mutate(doc)
    return yaml.safe_dump(doc, sort_keys=False)  # column order is the declared order


def _errors(mutate) -> list[str]:
    with pytest.raises(PolicyError) as e:
        loads_policy(_policy(mutate))
    return [path for path, _ in e.value.errors]


def test_valid_policy_round_trips_taxi_schema():
    p = loads_policy(_policy())
    assert p.dataset.schema == SCHEMA
    assert p.dataset.k == 50 and p.dataset.name == "nyc_taxi" and p.dataset.backend == "duckdb-dev"
    assert p.dataset.dialect == "duckdb" and p.dataset.epoch == "2026-10"
    assert p.dataset.keysets[("drivers", "fleet")] == ["medallion", "fleet_A", "fleet_B"]
    assert p.dataset.keysets[("taxi_zones", "borough")].table == "taxi_zones"
    c = p.consumers["analytics-team"]
    assert c.tables == frozenset({"trips", "taxi_zones", "drivers"}) and c.aggregates == frozenset({"count", "sum", "avg"})


def test_epsilons_are_exact_fractions():
    p = loads_policy(_policy(lambda d: d["consumers"]["analytics-team"].__setitem__("max_epsilon_per_query", 0.1)))
    assert p.dataset.budget == Fraction(10)
    assert p.consumers["analytics-team"].max_epsilon_per_query == Fraction(1, 10)
    assert p.consumers["analytics-team"].cap == Fraction(5)


def test_consumer_without_cap():
    p = loads_policy(_policy(lambda d: d["consumers"]["analytics-team"].pop("budget")))
    assert p.consumers["analytics-team"].cap is None


# -- schema-level rejections (one per rule) ------------------------------------

def test_rejects_unknown_key_at_top_level():
    assert any(p.startswith("dataset") or p == "" for p in _errors(lambda d: d["dataset"].__setitem__("bogus", 1)))


def test_rejects_clamp_on_non_int_column():
    assert "dataset.tables.drivers.columns.fleet.clamp" in _errors(
        lambda d: d["dataset"]["tables"]["drivers"]["columns"]["fleet"].__setitem__("clamp", [0, 1]))


def test_rejects_sensitivity_overflow():
    assert "dataset.tables.trips.columns.fare_amount.clamp" in _errors(
        lambda d: d["dataset"]["tables"]["trips"]["columns"]["fare_amount"].__setitem__("clamp", [0, 2**62]))


def test_rejects_private_fk_that_is_not_the_privacy_id():
    def m(d):
        d["dataset"]["tables"]["trips"]["foreign_keys"]["trip_id"] = "drivers.hack_license"
    assert "dataset.tables.trips.foreign_keys.trip_id" in _errors(m)


def test_rejects_public_table_on_fk_side():
    def m(d):
        d["dataset"]["tables"]["taxi_zones"]["foreign_keys"] = {"location_id": "drivers.hack_license"}
    assert "dataset.tables.taxi_zones.foreign_keys.location_id" in _errors(m)


def test_rejects_fk_target_that_is_not_a_primary_key():
    def m(d):
        d["dataset"]["tables"]["trips"]["foreign_keys"]["pu_location_id"] = "taxi_zones.borough"
    assert "dataset.tables.trips.foreign_keys.pu_location_id" in _errors(m)


def test_rejects_order_by_that_is_neither_pk_nor_unique():
    assert "dataset.tables.trips.order_by" in _errors(
        lambda d: d["dataset"]["tables"]["trips"].__setitem__("order_by", "fare_amount"))


def test_accepts_order_by_on_unique_column():
    def m(d):
        d["dataset"]["tables"]["trips"]["columns"]["fare_amount"]["unique"] = True
        d["dataset"]["tables"]["trips"]["order_by"] = "fare_amount"
    assert loads_policy(_policy(m)).dataset.schema.table("trips").order_by == "fare_amount"


def test_rejects_keyset_from_private_column():
    assert "dataset.keysets.drivers.fleet" in _errors(
        lambda d: d["dataset"]["keysets"].__setitem__("drivers.fleet", {"from": "drivers.fleet"}))


def test_rejects_keyset_on_undeclared_column():
    assert "dataset.keysets.trips.nope" in _errors(lambda d: d["dataset"]["keysets"].__setitem__("trips.nope", [1]))


def test_rejects_private_table_without_privacy_column():
    def m(d):
        d["dataset"]["tables"]["drivers"]["columns"] = {"driver_key": {"type": "string"}, "fleet": {"type": "string"}}
        d["dataset"]["tables"]["drivers"]["primary_key"] = "driver_key"
    assert "dataset.tables.drivers" in _errors(m)


def test_privacy_id_override_per_table():
    def m(d):
        d["dataset"]["tables"]["drivers"]["columns"] = {"driver_key": {"type": "string"}, "fleet": {"type": "string"}}
        d["dataset"]["tables"]["drivers"]["primary_key"] = "driver_key"
        d["dataset"]["tables"]["drivers"]["privacy_id"] = "driver_key"
        d["dataset"]["tables"]["trips"]["foreign_keys"]["hack_license"] = "drivers.driver_key"
        d["dataset"]["keysets"].pop("drivers.fleet")
    assert loads_policy(_policy(m)).dataset.schema.table("drivers").privacy_id == "driver_key"


# -- consumer-level rejections ---------------------------------------------------

def test_rejects_unknown_consumer_table():
    assert "consumers.analytics-team.tables" in _errors(
        lambda d: d["consumers"]["analytics-team"].__setitem__("tables", ["trips", "ghost"]))


def test_rejects_unknown_aggregate():
    assert "consumers.analytics-team.aggregates" in _errors(
        lambda d: d["consumers"]["analytics-team"].__setitem__("aggregates", ["count", "max"]))


def test_rejects_cap_above_dataset_budget():
    assert "consumers.analytics-team.budget.epsilon" in _errors(
        lambda d: d["consumers"]["analytics-team"]["budget"].__setitem__("epsilon", 11))


def test_rejects_per_query_max_above_cap():
    assert "consumers.analytics-team.max_epsilon_per_query" in _errors(
        lambda d: d["consumers"]["analytics-team"].__setitem__("max_epsilon_per_query", 6))


def test_rejects_per_query_max_above_dataset_budget_when_no_cap():
    def m(d):
        d["consumers"]["analytics-team"].pop("budget")
        d["consumers"]["analytics-team"]["max_epsilon_per_query"] = 11
    assert "consumers.analytics-team.max_epsilon_per_query" in _errors(m)


def test_several_errors_in_one_run():
    def m(d):
        d["dataset"]["tables"]["trips"]["order_by"] = "fare_amount"
        d["consumers"]["analytics-team"]["aggregates"] = ["count", "max"]
        d["consumers"]["analytics-team"]["budget"]["epsilon"] = 11
    paths = _errors(m)
    assert {"dataset.tables.trips.order_by", "consumers.analytics-team.aggregates",
            "consumers.analytics-team.budget.epsilon"} <= set(paths)


def test_error_string_lists_one_fault_per_line():
    with pytest.raises(PolicyError) as e:
        loads_policy(_policy(lambda d: d["consumers"]["analytics-team"].__setitem__("aggregates", ["max"])))
    assert "consumers.analytics-team.aggregates" in str(e.value)


def test_load_policy_reads_a_file(tmp_path):
    from cleanroom_sql import load_policy
    f = tmp_path / "p.yaml"
    f.write_text(_policy())
    assert load_policy(f).dataset.name == "nyc_taxi"


# -- review findings -------------------------------------------------------------

def test_rejects_privacy_id_override_not_linked_to_the_privacy_unit():
    """Renaming the privacy ID to an unrelated column would redefine the privacy unit."""
    assert "dataset.tables.trips" in _errors(lambda d: d["dataset"]["tables"]["trips"].__setitem__("privacy_id", "trip_id"))


def test_rejects_override_chain_that_never_reaches_the_unit_column():
    def m(d):
        d["dataset"]["tables"]["trips"]["privacy_id"] = "trip_id"
        d["dataset"]["tables"]["tips"] = {
            "primary_key": "tip_id", "privacy_id": "trip_id",
            "foreign_keys": {"trip_id": "trips.trip_id"},
            "columns": {"tip_id": {"type": "int"}, "trip_id": {"type": "int"}},
        }
    paths = _errors(m)
    assert "dataset.tables.trips" in paths and "dataset.tables.tips" in paths


def test_rejects_private_fk_whose_target_is_not_the_targets_privacy_id():
    """Same rule as the rewriter: the private PK side must be keyed by its privacy ID."""
    def m(d):
        d["dataset"]["tables"]["tips"] = {
            "primary_key": "tip_id",
            "foreign_keys": {"hack_license": "trips.trip_id"},
            "columns": {"tip_id": {"type": "int"}, "hack_license": {"type": "string"}},
        }
    assert "dataset.tables.tips.foreign_keys.hack_license" in _errors(m)


def test_accepts_two_private_tables_keyed_by_one_driver_with_different_column_names():
    def m(d):
        d["dataset"]["tables"]["tips"] = {
            "primary_key": "tip_id", "privacy_id": "driver",
            "foreign_keys": {"driver": "drivers.hack_license"},
            "columns": {"tip_id": {"type": "int"}, "driver": {"type": "string"}, "amount": {"type": "int", "clamp": [0, 5000]}},
        }
    assert loads_policy(_policy(m)).dataset.schema.table("tips").privacy_id == "driver"


@pytest.mark.parametrize("value", ["0", 0.0, float("inf"), float("nan")])
def test_rejects_non_positive_or_non_finite_epsilon(value):
    paths = _errors(lambda d: d["consumers"]["analytics-team"].__setitem__("max_epsilon_per_query", value))
    assert any(p.startswith("consumers.analytics-team.max_epsilon_per_query") for p in paths)


def test_rejects_duplicate_yaml_keys():
    text = _policy().replace("    max_epsilon_per_query: 0.5\n", "    max_epsilon_per_query: 0.5\n    max_epsilon_per_query: 0.2\n")
    with pytest.raises(PolicyError) as e:
        loads_policy(text)
    assert "max_epsilon_per_query" in str(e.value)


def test_rejects_epoch_on_a_consumer_budget():
    assert any(p.startswith("consumers.analytics-team.budget") for p in _errors(
        lambda d: d["consumers"]["analytics-team"]["budget"].__setitem__("epoch", "x")))
