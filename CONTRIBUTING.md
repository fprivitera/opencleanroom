# Contributing

## Sign-off

Every commit carries a `Signed-off-by` line. It certifies the [Developer Certificate of Origin 1.1](https://developercertificate.org/): you wrote the change, or you have the right to submit it under Apache-2.0. There is no CLA (ADR 0008).

```bash
git commit -s
```

Commits made before the repository went public have a single author and no sign-off.

## AI-assisted changes

Allowed. You sign off for them as for your own code: you have read the change, you can explain it, and you have the right to submit it. Name the tool in a `Co-Authored-By` trailer.

## Before a pull request

Install the environment CI tests, every package pinned with its hashes, then run the gates from the repository root:

```bash
uv venv .venv && uv pip sync --require-hashes requirements/gates.txt
uv pip install --no-deps -e packages/cleanroom-sql -e packages/opencleanroom
ruff check packages
cd packages/cleanroom-sql && python -m pytest -q && python red_team.py
cd packages/opencleanroom && python -m pytest -q && python red_team_platform.py
```

- A changed pin in a `pyproject.toml` comes with the lock files `scripts/lock.sh` writes, in the same commit; CI fails on a lock that does not match. `scripts/lock.sh --upgrade` moves the transitive dependencies to their latest releases.
- Use the terms in [`GLOSSARY.md`](GLOSSARY.md). A new concept gets an entry in the same change.
- A decision worth keeping gets an ADR in [`docs/adr/`](docs/adr/).
- Tests are the contract. A change to an existing test's logic or expected value goes in its own commit, apart from the code it follows.
- Privacy-critical files need a maintainer's review, with the evidence for the change (goldens unchanged for a pure move, the full Hypothesis profile for a sensitivity change, the exact accounting test for a budget change): `packages/cleanroom-sql/cleanroom_sql/{rewriter,sqlast,session,ledger,policy}.py` and `packages/opencleanroom/opencleanroom/{catalog,runtime,ledger_store,backends,materialize,mcp}.py`.

## Data

- Synthetic data only in tests and fixtures. Examples and benchmarks may use real data only through a loader from an official open-data portal, run on the user's machine, never committed to the repository; `packages/opencleanroom/examples/chicago/` works this way. No real personal data in the repository, and no public release that carries person identifiers: the 2013 NYC TLC trip data carries driver licence numbers.
- A benchmark built on a TPC workload is named "derived from TPC-H" (or TPC-DS) and never compared with official TPC results, as the [TPC fair use policy](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC_Fair_Use_Quick_Reference_v1.0.0.pdf) requires.

## Security issues

Never in an issue or a pull request: see [`SECURITY.md`](SECURITY.md).
