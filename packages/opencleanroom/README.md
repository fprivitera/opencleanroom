# opencleanroom

The platform around the `cleanroom-sql` library: catalog, CLI, backends, console and endpoints. `opencleanroom serve` (HTTP JSON API) and `opencleanroom mcp` run every request under a key's policy on the ledger in the catalog file.

Pre-release: no independent audit. Known limits and how to report a vulnerability: [`SECURITY.md`](https://github.com/fprivitera/opencleanroom/blob/main/SECURITY.md).

Not on PyPI yet: install from a checkout of the repository.

```bash
git clone https://github.com/fprivitera/opencleanroom && cd opencleanroom
pip install -e "packages/cleanroom-sql[duckdb]" -e packages/opencleanroom
```

The [repository README](https://github.com/fprivitera/opencleanroom#readme) has the quickstart, the policy format and the known limits.
