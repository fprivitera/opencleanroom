---
status: accepted
---
# The library is renamed `cleanroom-sql` before the first public tag

Three differentially private SQL projects already carry the name DPSQL: HKUST's [DPSQL](https://github.com/hkustDB/DPSQL), ByteDance's [Jeddak-DPSQL](https://github.com/bytedance/Jeddak-DPSQL) and LY Corporation's [DPSQL+](https://arxiv.org/abs/2602.22699). The PyPI name was free on 22 October 2026 (ADR 0009), but in this field the library would be taken for one of them. Before the first public tag the library becomes `cleanroom-sql` on PyPI, `cleanroom_sql` as the import package and `cleanroom-sql` as its command; the platform keeps `opencleanroom`. ADR 0009 stands otherwise: one monorepo, two packages published independently.

## Consequences

- Earlier ADRs keep the name `dpsql` where they mention it: they record what was true when they were written.
- `dpsql` is never published to PyPI.
