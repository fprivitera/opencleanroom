# cleanroom-sql

The library of the OpenCleanroom project: a SQL rewriter with a proven sensitivity bound, a policy loader and a differentially private executor on OpenDP. It depends on `sqlglot`, `opendp`, `pyyaml` and `jsonschema`, pinned exactly.

Pre-release: no independent audit. Known limits and how to report a vulnerability: [`SECURITY.md`](https://github.com/fprivitera/opencleanroom/blob/main/SECURITY.md).

From `packages/cleanroom-sql` in a checkout of the repository:

```bash
pip install -e ".[duckdb,test]"
python -m pytest -q
python red_team.py
cleanroom-sql policy check examples/taxi.yaml
```

The [repository README](https://github.com/fprivitera/opencleanroom#readme) documents the query flow, the policy format and the known limits.
