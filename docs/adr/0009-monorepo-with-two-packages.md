---
status: accepted
---
# One monorepo publishing two packages: `dpsql` and `opencleanroom`

The library (`dpsql`: schema, rewriter, mechanisms, ledger interface) and the platform (`opencleanroom`: catalog, CLI, gateway, backends, MCP) live in one repository under `packages/` and publish to PyPI independently. While the library API still changes, one repository avoids cross-repository churn, and the separate package keeps the library adoptable on its own: it depends on `sqlglot` and `opendp` only (amended by ADR 0010: plus `pyyaml` and `jsonschema` for policy files). Split the repository later if outside contributors need it. Both names were free on PyPI on 22 October 2026.

## Consequences

- The move into `packages/` happened on 22 October 2026, with the gates green before and after. The library runtime dependencies are `sqlglot`, `opendp`, `pyyaml` and `jsonschema` (ADR 0010).
- The library is renamed `cleanroom-sql` before the first public tag (ADR 0012).
