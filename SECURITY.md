# Security policy

## Reporting a vulnerability

Report privately on GitHub: **Security → Report a vulnerability** at <https://github.com/fprivitera/opencleanroom/security/advisories/new>. Do not open a public issue or pull request.

Include the policy, the SQL and the ledger state that reproduce the finding, on synthetic data. This is a personal project with one maintainer: reports are answered on a best-effort basis. A confirmed finding gets a fix and a red-team probe, credited to the reporter unless they ask otherwise.

## Supported versions

Pre-release. Fixes land on `main` only.

## In scope

Anything that lets a consumer learn more about a privacy unit than the noisy results and the budget allow:

- a sensitivity lower than the true one: rewriter, joins, truncation, clamping;
- ε spent past the budget or a consumer cap, or not charged: ledger, budget epochs;
- an oracle: error text, a released reservation, a result shape that depends on private data;
- SQL that reaches the backend outside the rewriter's allow-list;
- authentication of API keys on the CLI, the HTTP API and the MCP server;
- a vulnerability in a dependency that reaches one of the paths above.

## Known limits

Each has a probe marked `KNOWN` in `red_team.py` or `red_team_platform.py`. Reports that restate them are welcome only with a new exploit path.

- Timing side channels, including the noise sampler's, are not mitigated.
- The executor enables OpenDP's `contrib` features (`packages/cleanroom-sql/cleanroom_sql/session.py`): the mechanisms it calls are outside OpenDP's vetted set.
- The admin's privacy metadata is trusted: privacy ID, bounds, foreign keys, keysets. Declared primary keys are checked only when the admin runs `backend check-keys` or `backend materialize`; tables that change afterwards rest on the admin's claim again.
- The operator is trusted with raw data and with the catalog file: editing it by hand bypasses the checks `policy apply` makes.

## Dependencies

Every library imported by a process that runs the executor is trusted with raw data: it could read the private tables, skip the noise or move the ledger. What each entry point loads besides the standard library:

- the library, `cleanroom-sql`: `sqlglot`, `opendp`, `pyyaml`, `jsonschema` and their dependencies;
- the `opencleanroom` CLI: also `duckdb`; a Spark backend adds the Spark Connect client with pandas, PyArrow and gRPC;
- `opencleanroom serve`: also `starlette` and `uvicorn`;
- `opencleanroom mcp`: also `fastmcp` and its dependencies (`mcp`, `pydantic`, `authlib`, `httpx` and others);
- `opencleanroom console`: also Streamlit and its dependencies, in the same process as the executor.

Direct dependencies are pinned exactly in each `pyproject.toml`. CI installs every transitive dependency from `requirements/`, pinned with its hashes, and fails when those files do not match the pins ([`CONTRIBUTING.md`](CONTRIBUTING.md) installs the same environment). An install from PyPI resolves the transitive dependencies afresh.

## What the project does not claim

The software has had no independent audit. A differentially private result with a documented ε is evidence for an assessment of anonymity under the GDPR or any other law, not a conclusion: that assessment covers the whole deployment, which the admin owns. The software is provided under Apache-2.0, without warranty (sections 7 and 8 of [`LICENSE`](LICENSE)).
