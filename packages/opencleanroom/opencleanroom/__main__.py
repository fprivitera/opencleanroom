# SPDX-License-Identifier: Apache-2.0
"""The `opencleanroom` command: catalog, policies, consumers and a query from the terminal.

Exit codes as `cleanroom-sql policy check`: 0 ok, 1 with messages on stderr, 2
for a file or catalog that cannot be read.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import sqlite3
import sys
from pathlib import Path

import duckdb
import yaml
from cleanroom_sql import BudgetExhaustedException, PolicyError, PolicyViolation, UnsupportedQuery, loads_policy
from cleanroom_sql.session import is_metadata_query

from . import catalog
from .backends import open_backend
from .jsonable import jsonable
from .runtime import AuthError, Runtime

NOTE = "note: budgets are shared through the catalog ledger"


def _jsonable(value):
    if dataclasses.is_dataclass(value):
        return {k: _jsonable(v) for k, v in dataclasses.asdict(value).items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return jsonable(value)


def _require_catalog(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"catalog not found: {path}\n"
                                f"Create one with: opencleanroom --catalog {path} init\n"
                                "Or choose an existing catalog with: opencleanroom --catalog PATH ...")


def cmd_init(a):
    catalog.init(a.catalog)
    print(f"initialised {a.catalog}")


def cmd_backend_add(a):
    _require_catalog(a.catalog)
    if a.duckdb is not None:
        if a.database:
            raise ValueError("--database applies to a Spark backend only")
        if not Path(a.duckdb).is_file():
            raise FileNotFoundError(f"DuckDB file not found: {a.duckdb}")
        catalog.add_backend(a.catalog, a.name, "duckdb", a.duckdb, query_timeout=a.timeout)
    else:
        url = a.spark.rstrip("/") + (f"/;database={a.database}" if a.database else "")
        catalog.add_backend(a.catalog, a.name, "spark", url, query_timeout=a.timeout)
    print(f"registered backend {a.name}")


def cmd_backend_list(a):
    _require_catalog(a.catalog)
    rows = catalog.list_backends(a.catalog)
    if a.json:
        print(json.dumps([{**_jsonable(b), "location": catalog.redact_location(b.location)} for b in rows]))
    else:
        for b in rows:
            print(f"{b.name}\t{b.kind}\t{catalog.redact_location(b.location)}")


def cmd_policy_apply(a):
    _require_catalog(a.catalog)
    applied = catalog.apply_policy(a.catalog, Path(a.file).read_text())
    print(f"applied {applied.dataset} version {applied.version} epoch {applied.epoch}")


def cmd_policy_check(a):
    loaded = loads_policy(Path(a.file).read_text())
    print(f"OK: {loaded.dataset.name}, {len(loaded.consumers)} consumers")


def _flatten(node, prefix=""):
    """{dotted.path: leaf}; lists and scalars are leaves."""
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _flatten(v, f"{prefix}.{k}" if prefix else str(k))
    else:
        yield prefix, node


def cmd_policy_diff(a):
    """What `policy apply` would change: the file is validated first, nothing is written."""
    _require_catalog(a.catalog)
    text = Path(a.file).read_text()
    new = loads_policy(text)
    stored = catalog.get_policy(a.catalog, new.dataset.name)
    if stored is None:
        print(f"{new.dataset.name}: new dataset, no policy applied yet")
        return
    old, fresh = dict(_flatten(yaml.safe_load(stored.text))), dict(_flatten(yaml.safe_load(text)))
    lines = [f"- {k}: {old[k]!r}" for k in old.keys() - fresh.keys()]
    lines += [f"+ {k}: {fresh[k]!r}" for k in fresh.keys() - old.keys()]
    lines += [f"~ {k}: {old[k]!r} -> {fresh[k]!r}" for k in old.keys() & fresh.keys() if old[k] != fresh[k]]
    print(f"{new.dataset.name}: version {stored.version} (epoch {stored.epoch}) -> file" + ("" if lines else ": no changes"))
    if lines:
        print("\n".join(sorted(lines, key=lambda l: l[2:])))


def cmd_policy_show(a):
    _require_catalog(a.catalog)
    stored = catalog.get_policy(a.catalog, a.dataset)
    if stored is None:
        raise ValueError(f"no policy applied for dataset {a.dataset}")
    print(f"# {stored.dataset} version {stored.version} epoch {stored.epoch} applied {stored.applied_at} sha256 {stored.sha256}")
    print(stored.text, end="")


def cmd_consumer_create(a):
    _require_catalog(a.catalog)
    print(catalog.create_consumer(a.catalog, a.dataset, a.name))  # the key, once


def cmd_consumer_revoke(a):
    _require_catalog(a.catalog)
    catalog.revoke_consumer(a.catalog, a.dataset, a.name)
    print(f"revoked {a.name}")


def cmd_consumer_list(a):
    _require_catalog(a.catalog)
    rows = catalog.list_consumers(a.catalog, a.dataset)
    if a.json:
        print(json.dumps(_jsonable(rows)))
    else:
        for c in rows:
            print(f"{c.name}\tcreated {c.created_at}\t{'revoked ' + c.revoked_at if c.revoked_at else 'active'}")


def _query_text(a) -> str:
    """The SQL from the argument, a file, or stdin (`--file -`); exactly one source."""
    if a.sql is not None and a.file is not None:
        raise ValueError("give the SQL as an argument or with --file, not both")
    if a.file is None:
        if a.sql is None:
            raise ValueError("no SQL given: pass it as an argument or with --file PATH (or --file - for stdin)")
        return a.sql
    text = sys.stdin.read() if a.file == "-" else Path(a.file).read_text()
    if not text.strip():
        raise ValueError(f"{'stdin' if a.file == '-' else a.file} contains no SQL")
    return text


def cmd_query(a):
    _require_catalog(a.catalog)
    import os
    key = a.key or os.environ.get("OPENCLEANROOM_KEY")
    if not key:
        raise ValueError("--key or OPENCLEANROOM_KEY is required (create a key with: opencleanroom consumer create DATASET NAME)")
    a.key = key
    a.sql = _query_text(a)
    runtime = Runtime(a.catalog)
    if is_metadata_query(a.sql) is not None:  # DESCRIBE <table> / SHOW TABLES: from the policy, no budget
        rows = runtime.query(a.key, a.sql)
        if a.json:
            print(json.dumps(_jsonable(rows)))
        else:
            for row in rows:
                print("\t".join(str(v) for v in row))
        return
    if a.epsilon is None:
        raise ValueError("--epsilon is required for a SELECT, e.g. --epsilon 0.1 (DESCRIBE and SHOW TABLES cost nothing)")
    res = runtime.query(a.key, a.sql, epsilon=a.epsilon)
    remaining = runtime.remaining_for_key(a.key)
    print(NOTE, file=sys.stderr)
    if a.json:
        out = {
            "rows": _jsonable(res.rows), "rewritten_sql": res.rewritten_sql, "epsilon_spent": res.epsilon_spent,
            "epsilon_per_measurement": res.epsilon_per_measurement, "sensitivities": list(res.sensitivities),
            "ci95_halfwidth": list(res.ci95_halfwidth), "remaining": float(remaining),
        }
        print(json.dumps(out))
        return
    for row in res.rows:
        print("\t".join(str(v) for v in row))
    print(f"\nrewritten: {res.rewritten_sql}")
    print(f"sensitivities: {res.sensitivities}  epsilon per measurement: {res.epsilon_per_measurement}")
    print(f"remaining: {float(remaining)}")


def cmd_console(a):
    import os
    import subprocess
    _require_catalog(a.catalog)
    script = Path(__file__).with_name("console.py")
    env = {**os.environ, "OPENCLEANROOM_CATALOG": str(a.catalog)}
    # The console has no login: bind to the loopback interface unless the operator says otherwise.
    return subprocess.call([sys.executable, "-m", "streamlit", "run", str(script), "--server.address", a.host], env=env)


def cmd_new_epoch(a):
    _require_catalog(a.catalog)
    row = catalog.new_epoch(a.catalog, a.dataset, a.epoch, a.snapshot, a.reason)
    print(f"epoch {row.epoch} recorded for {row.dataset}; apply the policy file with budget.epoch: {row.epoch}")


def cmd_audit_reconcile(a):
    _require_catalog(a.catalog)
    rows = Runtime(a.catalog).reconcile(a.dataset)
    bad = False
    for r in rows:
        mark = "ok" if r.ok else "MISMATCH"
        bad = bad or not r.ok
        print(f"{r.epoch}\t{r.consumer}\tledger committed {r.ledger_committed}\taudit charged {r.audit_charged}\t{mark}"
              f"\treserved rows {r.reserved_rows}\tin_progress rows {r.in_progress_rows}")
    if not rows:
        print("nothing recorded yet")
    return 1 if bad else 0


def cmd_audit_list(a):
    _require_catalog(a.catalog)
    rows = Runtime(a.catalog).audit(a.dataset)
    if a.outcome:
        rows = [r for r in rows if r.outcome == a.outcome]
    if a.json:
        print(json.dumps([_jsonable(r) for r in rows]))
        return
    for r in rows:
        print(f"{r.id}\t{r.ts}\t{r.consumer}\t{r.request_id}\t{r.outcome}\t{r.error_code or ''}\t"
              f"requested {r.epsilon_requested}\tcharged {r.epsilon_charged}\t{(r.sql or '')[:80]}")


def cmd_ledger_list(a):
    _require_catalog(a.catalog)
    for r in Runtime(a.catalog).reserved(a.dataset):
        print(f"{r.id}\t{r.epoch}\t{r.consumer}\t{r.epsilon}\t{r.request_id}\t{r.created_at}")


def cmd_backend_materialize(a):
    _require_catalog(a.catalog)
    from .materialize import materialize
    for name in materialize(a.catalog, a.dataset, a.snapshot):
        print(f"created {name}")


def cmd_backend_check_keys(a):
    _require_catalog(a.catalog)
    from .materialize import check_primary_keys
    stored = catalog.get_policy(a.catalog, a.dataset)
    if stored is None:
        raise ValueError(f"no policy applied for dataset {a.dataset}")
    policy = loads_policy(stored.text)
    row = catalog.get_backend(a.catalog, policy.dataset.backend)
    backend = open_backend(row.kind, row.location, row.query_timeout)
    try:
        check_primary_keys(backend.admin_sql, policy)
    finally:
        backend.close()
    print(f"declared primary keys of {a.dataset} are unique")


def cmd_ledger_release(a):
    _require_catalog(a.catalog)
    Runtime(a.catalog).release_reservation(a.id, a.reason)
    print(f"ledger row {a.id} released")


def cmd_serve(a):
    _require_catalog(a.catalog)
    import uvicorn

    from .api import create_app
    uvicorn.run(create_app(a.catalog, rate=a.rate), host=a.host, port=a.port)


def cmd_mcp(a):
    _require_catalog(a.catalog)
    from .mcp import create_server
    server = create_server(a.catalog, key_from_env=a.transport == "stdio", rate=a.rate)
    if a.transport == "stdio":
        server.run(transport="stdio", show_banner=False)
    else:
        server.run(transport="http", host=a.host, port=a.port, show_banner=False)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="opencleanroom")
    p.add_argument("--catalog", type=Path, default=Path("catalog.sqlite"))
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create an empty catalog").set_defaults(fn=cmd_init)

    backend = sub.add_parser("backend", help="register, list, materialize and check backends").add_subparsers(dest="action", required=True)
    add = backend.add_parser("add", help="register a DuckDB file or a Spark Connect server")
    add.add_argument("name")
    where = add.add_mutually_exclusive_group(required=True)
    where.add_argument("--duckdb", help="path of the DuckDB file")
    where.add_argument("--spark", help="Spark Connect URL, e.g. sc://host:15002 (needs the spark extra)")
    add.add_argument("--database", help="Spark database holding the dataset's tables (default: the server's current one)")
    add.add_argument("--timeout", type=float, default=30.0, help="seconds a query may run before it is stopped and charged as failed")
    add.set_defaults(fn=cmd_backend_add)
    ls = backend.add_parser("list")
    ls.add_argument("--json", action="store_true")
    ls.set_defaults(fn=cmd_backend_list)
    mat = backend.add_parser("materialize", help="store the first k rows per privacy unit of each truncated table (Spark)")
    mat.add_argument("dataset")
    mat.add_argument("--snapshot", required=True, help="the data snapshot; must match the epoch's recorded snapshot if there is one")
    mat.set_defaults(fn=cmd_backend_materialize)
    ck = backend.add_parser("check-keys", help="check that declared column types and primary keys hold in the data")
    ck.add_argument("dataset")
    ck.set_defaults(fn=cmd_backend_check_keys)

    policy = sub.add_parser("policy", help="validate, compare, apply and show policies").add_subparsers(dest="action", required=True)
    pc = policy.add_parser("check", help="validate a policy file; the catalog is not touched")
    pc.add_argument("file")
    pc.set_defaults(fn=cmd_policy_check)
    pd = policy.add_parser("diff", help="show what applying this file would change against the stored policy")
    pd.add_argument("file")
    pd.set_defaults(fn=cmd_policy_diff)
    apply = policy.add_parser("apply", help="validate and store a policy file")
    apply.add_argument("file")
    apply.set_defaults(fn=cmd_policy_apply)
    show = policy.add_parser("show", help="print the stored policy")
    show.add_argument("dataset")
    show.set_defaults(fn=cmd_policy_show)

    consumer = sub.add_parser("consumer", help="create, list and revoke consumer keys").add_subparsers(dest="action", required=True)
    create = consumer.add_parser("create", help="create a consumer key (printed once)")
    create.add_argument("dataset")
    create.add_argument("name")
    create.set_defaults(fn=cmd_consumer_create)
    revoke = consumer.add_parser("revoke")
    revoke.add_argument("dataset")
    revoke.add_argument("name")
    revoke.set_defaults(fn=cmd_consumer_revoke)
    clist = consumer.add_parser("list")
    clist.add_argument("dataset")
    clist.add_argument("--json", action="store_true")
    clist.set_defaults(fn=cmd_consumer_list)

    query = sub.add_parser("query", help="run a query under the key's policy")
    query.add_argument("sql", nargs="?", help="the query; or use --file")
    query.add_argument("--file", metavar="PATH", help="read the SQL from a file, or from stdin with -")
    query.add_argument("--key", default=None, help="consumer key; or set OPENCLEANROOM_KEY to keep it out of the shell history")
    query.add_argument("--epsilon", type=float, default=None, help="required for a SELECT; DESCRIBE and SHOW TABLES cost nothing")
    query.add_argument("--json", action="store_true", help="print rows and metadata as one JSON object on stdout")
    query.set_defaults(fn=cmd_query)
    query.epilog = ("examples:\n  opencleanroom query 'SHOW TABLES'\n  opencleanroom query 'SELECT COUNT(*) FROM trips' --epsilon 0.1\n"
                    "  opencleanroom query --file report.sql --epsilon 0.1 --json")
    query.formatter_class = argparse.RawDescriptionHelpFormatter

    console = sub.add_parser("console", help="start the Streamlit console (needs the ui extra)")
    console.add_argument("--host", default="127.0.0.1", help="interface to bind; the console has no login")
    console.set_defaults(fn=cmd_console)

    budget = sub.add_parser("budget", help="manage budget epochs").add_subparsers(dest="action", required=True)
    ne = budget.add_parser("new-epoch", help="record a budget renewal before applying the policy file with the new label (ADR 0004)")
    ne.add_argument("dataset")
    ne.add_argument("--epoch", required=True, help="the new label; must sort after the current one")
    ne.add_argument("--snapshot", required=True, help="id of the data snapshot the new epoch covers")
    ne.add_argument("--reason", required=True)
    ne.set_defaults(fn=cmd_new_epoch)

    audit = sub.add_parser("audit", help="read the audit log and reconcile charges").add_subparsers(dest="action", required=True)
    rec = audit.add_parser("reconcile", help="compare committed ledger epsilon with the audit log per epoch; exit 1 on a mismatch")
    rec.add_argument("dataset")
    rec.set_defaults(fn=cmd_audit_reconcile)
    al = audit.add_parser("list")
    al.add_argument("dataset")
    al.add_argument("--json", action="store_true")
    al.add_argument("--outcome", default=None, help="only rows with this outcome, e.g. in_progress after a crash")
    al.set_defaults(fn=cmd_audit_list)

    ledger = sub.add_parser("ledger", help="inspect and release open reservations").add_subparsers(dest="action", required=True)
    ll = ledger.add_parser("list", help="reservations still open (a crashed worker leaves one; it counts as spent)")
    ll.add_argument("dataset")
    ll.set_defaults(fn=cmd_ledger_list)
    lr = ledger.add_parser("release", help="free a reserved row left by a crash; the reason goes to the audit log")
    lr.add_argument("id", type=int)
    lr.add_argument("--reason", required=True)
    lr.set_defaults(fn=cmd_ledger_release)

    serve = sub.add_parser("serve", help="start the HTTP JSON API (uvicorn)")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--rate", default="60/60", help="requests per seconds per key, e.g. 60/60; unknown keys share one bucket")
    serve.set_defaults(fn=cmd_serve)

    mcp = sub.add_parser("mcp", help="start the MCP server (key: Authorization header on http, OPENCLEANROOM_KEY on stdio)")
    mcp.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    mcp.add_argument("--host", default="127.0.0.1")
    mcp.add_argument("--port", type=int, default=8001)
    mcp.add_argument("--rate", default="60/60", help="requests per seconds per key, e.g. 60/60")
    mcp.set_defaults(fn=cmd_mcp)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = args.fn(args)
        if isinstance(result, int):  # a subprocess exit code (console)
            return result
    except (OSError, UnicodeDecodeError) as e:
        print(e, file=sys.stderr)
        return 2
    except sqlite3.DatabaseError as e:  # the catalog file cannot be opened or is not a catalog
        print(f"catalog {args.catalog}: {e}", file=sys.stderr)
        return 2
    except PolicyError as e:
        for path, message in e.errors:
            print(f"{path or '<root>'}: {message}", file=sys.stderr)
        return 1
    except AuthError:
        print("invalid key", file=sys.stderr)
        return 1
    except (PolicyViolation, UnsupportedQuery, BudgetExhaustedException, RuntimeError, ValueError, duckdb.Error) as e:
        print(e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
