# SPDX-License-Identifier: Apache-2.0
"""The console: one Streamlit app with an Admin section and a Consumer section.

Started with `opencleanroom console [--catalog PATH]`, which sets
OPENCLEANROOM_CATALOG and runs `streamlit run` on this file. The console
calls the catalog and the runtime only; it never touches the rewriter or
the executor, and it runs where the executor runs (not a trust boundary).
Budgets are shared through the catalog ledger with every process on the file.
"""
from __future__ import annotations

import hashlib
import os
import re
from fractions import Fraction

import duckdb
import streamlit as st
from cleanroom_sql import BudgetExhaustedException, PolicyError, PolicyViolation, UnsupportedQuery
from cleanroom_sql.session import is_metadata_query

from opencleanroom import catalog
from opencleanroom.runtime import AuthError, Runtime

CATALOG = os.environ.get("OPENCLEANROOM_CATALOG", "catalog.sqlite")
FOOTER = "Budgets are shared through the catalog ledger; every process on this catalog sees the same spend."

# Ready examples for the taxi dataset (examples/taxi.yaml), Gradio-style: (group, title, sql, epsilon, note).
# "passes" run under analytics-team; "blocked" show the rewriter and the policy refusing on purpose.
EXAMPLES = [
    ("passes", "What can I query?", "SHOW TABLES", 0.1, "Metadata from the policy: free, no ε."),
    ("passes", "Columns of trips", "SHOW COLUMNS FROM trips", 0.1, "Declared columns, types and clamp bounds."),
    ("passes", "Count of trips", "SELECT COUNT(*) FROM trips", 0.1, "Sensitivity k = 50: one driver owns at most 50 truncated rows."),
    ("passes", "Count and total fare", "SELECT COUNT(*), SUM(fare_amount) FROM trips", 0.2,
     "Two measurements, ε split in two; the SUM is clamped to [0, 20000]."),
    ("passes", "Average fare", "SELECT AVG(fare_amount) FROM trips", 0.2, "AVG is a noisy SUM over a noisy COUNT of the same rows."),
    ("passes", "Trips per borough", "SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough",
     0.2, "Keyset read from the public taxi_zones table; small boroughs show pure noise."),
    ("passes", "Drivers per fleet", "SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", 0.1, "Keyset declared inline by the admin; keyed table, Δ = 1."),
    ("passes", "Long, cheap trips", "SELECT COUNT(*), SUM(fare_amount) FROM trips WHERE trip_distance > 50 AND fare_amount < 1000", 0.2,
     "Both conjuncts read trips only: pushed inside the truncation, before ROW_NUMBER."),
    ("passes", "Fleet × borough with a filter",
     "SELECT d.fleet, z.borough, COUNT(*), SUM(t.fare_amount) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
     "JOIN taxi_zones z ON t.pu_location_id = z.location_id WHERE t.trip_distance BETWEEN 5 AND 100 GROUP BY d.fleet, z.borough",
     0.3, "Three tables, row-ownership join (Δ = k, not k + 1), cartesian keyset 3 × 6."),
    ("passes", "Average fare per borough with counts",
     "SELECT z.borough, AVG(t.fare_amount), COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough",
     0.3, "Three measurements per group; ε split in three."),
    ("blocked", "MAX is not an aggregate we noise", "SELECT MAX(fare_amount) FROM trips", 0.1, "UnsupportedQuery: only COUNT, SUM, AVG."),
    ("blocked", "COUNT(DISTINCT)", "SELECT COUNT(DISTINCT hack_license) FROM trips", 0.1, "UnsupportedQuery: no distinct counting in 0.1."),
    ("blocked", "Subquery", "SELECT COUNT(*) FROM (SELECT * FROM trips) s", 0.1, "UnsupportedQuery: no nested SELECT anywhere."),
    ("blocked", "CTE smuggling", "WITH x AS (SELECT * FROM trips) SELECT COUNT(*) FROM x", 0.1, "UnsupportedQuery."),
    ("blocked", "UNION", "SELECT COUNT(*) FROM trips UNION ALL SELECT COUNT(*) FROM drivers", 0.1, "UnsupportedQuery."),
    ("blocked", "GROUP BY without a keyset", "SELECT pu_location_id, COUNT(*) FROM trips GROUP BY pu_location_id", 0.1,
     "PolicyViolation: the policy declares no keyset for trips.pu_location_id (ADR 0005)."),
    ("blocked", "Hidden cross join",
     "SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license JOIN taxi_zones z ON t.hack_license = d.hack_license",
     0.1, "UnsupportedQuery: the ON must bind the joined table."),
    ("blocked", "Arithmetic in WHERE (error oracle)", "SELECT COUNT(*) FROM trips WHERE fare_amount * 9223372036854775807 > 0", 0.1,
     "UnsupportedQuery: expressions outside the whitelist never reach the backend."),
    ("blocked", "CAST as an oracle", "SELECT COUNT(*) FROM trips WHERE CAST(hack_license AS INT) > 0", 0.1, "UnsupportedQuery: no CAST."),
    ("blocked", "SUM of an expression", "SELECT SUM(fare_amount * 1000) FROM trips", 0.1, "UnsupportedQuery: the clamp applies to a column."),
    ("blocked", "ε above the per-query maximum", "SELECT COUNT(*) FROM trips", 5.0, "PolicyViolation: analytics-team may spend 0.5 per query."),
    ("blocked", "Table outside the policy (use the counts-only key)", "SELECT COUNT(*) FROM drivers", 0.1,
     "With the counts-only key the table is unknown to the consumer's rewriter."),
]


@st.cache_resource
def runtime(path: str) -> Runtime:
    """One Runtime per process: reruns and every browser session share the ledgers."""
    return Runtime(path)


def _f(x: Fraction) -> float:
    return float(x)


# -- admin ------------------------------------------------------------------

def admin() -> None:
    st.header("Admin")
    rt = runtime(CATALOG)

    uploaded = st.file_uploader("Apply a policy file", type=["yaml", "yml"])
    if uploaded is not None:
        text = uploaded.getvalue().decode()
        digest = hashlib.sha256(text.encode()).hexdigest()
        # Streamlit reruns the script on every click while the uploader still holds the file: apply once per upload.
        if st.session_state.get("applied_sha") != digest:
            try:
                applied = catalog.apply_policy(CATALOG, text)
                st.session_state["applied_sha"] = digest
                st.session_state["applied_msg"] = f"applied {applied.dataset} version {applied.version} epoch {applied.epoch}"
            except PolicyError as e:
                st.session_state.pop("applied_msg", None)
                for path, message in e.errors:
                    st.error(f"{path or '<root>'}: {message}")
        if st.session_state.get("applied_sha") == digest:
            st.success(st.session_state["applied_msg"])

    st.subheader("Backends")
    st.dataframe([{"name": b.name, "kind": b.kind, "location": catalog.redact_location(b.location)} for b in catalog.list_backends(CATALOG)])

    st.subheader("Policies")
    rows = []
    for p in catalog.list_policies(CATALOG):
        b = rt.budgets(p.dataset)
        rows.append({"dataset": p.dataset, "version": p.version, "epoch": p.epoch,
                     "budget ε": _f(b["budget"]), "spent ε": _f(b["spent"]), "remaining ε": _f(b["remaining"])})
    st.dataframe(rows)

    datasets = [p.dataset for p in catalog.list_policies(CATALOG)]
    st.subheader("Consumers")
    dataset = st.selectbox("Dataset", datasets) if datasets else None
    if dataset:
        st.dataframe([{"name": c.name, "created": c.created_at, "revoked": c.revoked_at or "",
                       "remaining ε": _f(rt.remaining(dataset, c.name)) if c.revoked_at is None else None}
                      for c in catalog.list_consumers(CATALOG, dataset)])
        name = st.text_input("Consumer name (from the policy's consumers block)")
        if st.button("Create key"):
            try:
                key = catalog.create_consumer(CATALOG, dataset, name)
                st.warning("Copy the key now; it is shown once and stored only as a hash.")
                st.code(key)
            except ValueError as e:
                st.error(str(e))
        active = [c.name for c in catalog.list_consumers(CATALOG, dataset) if c.revoked_at is None]
        for c in active:
            if st.button(f"Revoke {c}", key=f"revoke-{dataset}-{c}"):
                catalog.revoke_consumer(CATALOG, dataset, c)
                st.rerun()
    st.caption(FOOTER)


# -- consumer ---------------------------------------------------------------

def consumer() -> None:
    st.header("Consumer")
    rt = runtime(CATALOG)
    st.session_state.setdefault("sql", "SELECT COUNT(*) FROM trips")
    st.session_state.setdefault("epsilon", 0.1)
    st.session_state.setdefault("note", "")
    pending = st.session_state.pop("pending_example", None)  # set by an example button on the previous run
    if pending:
        st.session_state.update(sql=pending[0], epsilon=pending[1], note=pending[2])
    key = st.text_input("API key", type="password")
    sql = st.text_area("SQL", key="sql", height=120)
    epsilon = st.number_input("ε for this query", min_value=0.001, step=0.01, format="%.3f", key="epsilon")
    if st.session_state["note"]:
        st.caption(st.session_state["note"])
    run = st.button("Run", type="primary")

    if run:
        st.session_state["last"] = _run(rt, key, sql, epsilon)  # the outcome stays in the session: a rerun does not drop it
    last = st.session_state.get("last")
    if last is not None:
        _show(last)

    _examples(rt, key)
    st.caption(FOOTER)


def _run(rt: Runtime, key: str, sql: str, epsilon: float) -> dict:
    """Run one query and return what to show: the SQL it ran for, and either an outcome or an error with its next step."""
    out = {"sql": sql}
    try:
        meta = is_metadata_query(sql)
        if meta is not None:  # answered from the policy, no budget
            kind, _ = meta
            rows = rt.query(key, sql)
            out.update(kind="metadata", columns=("column", "type", "clamp") if kind == "describe" else ("table", "kind"),
                       rows=[list(r) for r in rows], remaining=float(rt.remaining_for_key(key)))
            return out
        res = rt.query(key, sql, epsilon=round(float(epsilon), 3))  # the widget shows 3 decimals; spend that number exactly
        out.update(kind="result", rows=[list(r) for r in res.rows], epsilon=res.epsilon_spent, rewritten=res.rewritten_sql,
                   measurements=[{"measurement": i, "sensitivity": s, "ε": res.epsilon_per_measurement, "95% CI ±": round(h, 1)}
                                 for i, (s, h) in enumerate(zip(res.sensitivities, res.ci95_halfwidth))],
                   remaining=float(rt.remaining_for_key(key)))
    except AuthError:
        out.update(kind="error", title="We couldn't connect with this key.", detail="Check the API key above and run again.", code="AuthError")
    except UnsupportedQuery as e:
        out.update(kind="error", title="This SQL feature isn't supported.", detail=f"{e}. Edit the query above; it is kept as you wrote it.",
                   code="UnsupportedQuery")
    except PolicyViolation as e:
        out.update(kind="error", title="The policy doesn't allow this request.", detail=f"{e}. Change the query or ε above and run again.",
                   code="PolicyViolation")
    except BudgetExhaustedException:
        out.update(kind="error", title="This query couldn't reserve the requested budget.",
                   detail="Lower ε or ask the admin about your consumer cap.", code="BudgetExhaustedException")
    except (ValueError, RuntimeError, duckdb.Error) as e:
        out.update(kind="error", title="The query didn't complete.", detail=str(e), code=type(e).__name__)
    return out


def _show(last: dict) -> None:
    if last["kind"] == "error":
        st.error(last["title"])
        st.write(last["detail"])
        st.caption(f"code: {last['code']}")
        return
    if last["kind"] == "metadata":
        st.success("accepted (metadata, no ε spent)")
        st.dataframe([dict(zip(last["columns"], r)) for r in last["rows"]])
    else:
        st.success(f"accepted: {len(last['rows'])} row(s), ε {last['epsilon']:g} spent")
        st.dataframe(last["rows"])
        with st.expander("Rewritten query and measurements"):
            st.code(last["rewritten"], language="sql")
            st.table(last["measurements"])
    st.metric("remaining ε", f"{last['remaining']:g}")
    if last["sql"] != st.session_state.get("sql"):
        st.caption("The SQL above has changed since this result.")


def _examples(rt: Runtime, key: str) -> None:
    """Examples in closed panels. The passing ones are limited to the tables this key can read."""
    try:
        visible = {r[0] for r in rt.query(key, "SHOW TABLES")} if key else None
    except (AuthError, ValueError, RuntimeError, PolicyViolation, UnsupportedQuery, duckdb.Error):
        visible = None  # no usable key yet: show them all
    with st.expander("Examples"):
        if visible is not None and not any(_readable(sql, visible) for g, _, sql, _, _ in EXAMPLES if g == "passes"):
            st.caption("No example matches the tables of this policy; try SHOW TABLES, then DESCRIBE a table.")
        for g, name, example_sql, eps, note in EXAMPLES:
            if g == "passes" and (visible is None or _readable(example_sql, visible)) and st.button(name, key=f"ex-{g}-{name}"):
                st.session_state["pending_example"] = (example_sql, eps, note)  # widgets are already drawn: apply on rerun
                st.rerun()
    with st.expander("Blocked on purpose"):
        st.caption("Queries the rewriter or the policy refuse, to show how.")
        for g, name, example_sql, eps, note in EXAMPLES:
            if g == "blocked" and st.button(name, key=f"ex-{g}-{name}"):
                st.session_state["pending_example"] = (example_sql, eps, note)
                st.rerun()


def _readable(sql: str, visible: set[str]) -> bool:
    return all(t in visible for t in re.findall(r"\b(?:FROM|JOIN)\s+(\w+)", sql, flags=re.IGNORECASE))


section = st.sidebar.radio("Section", ["Admin", "Consumer"])
st.sidebar.caption(f"catalog: {CATALOG}")
if section == "Admin":
    admin()
else:
    consumer()
