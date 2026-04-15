# TDX Data Collection

## Runtime Defaults

- `TdxRoot`: `{TdxRoot}`
- `OutputRoot`: `{OutputRoot}`
- `DatabasePath`: `{DatabasePath}`
- `DailyTime`: `{DailyTime}`
- `TaskName`: `{TaskName}`

## Runtime Files

- `RunnerPath`: `{RunnerPath}`
- `StateFile`: `{StateFile}`
- `LastSummaryJson`: `{LastSummaryJson}`

## Operations

- Full bootstrap: run `sync_tdx_to_duckdb.py --full-rebuild`
- Daily update: run scheduled incremental task or `run_tdx_incremental_daily.ps1`
- Rebuild rule: only rebuild when output root is empty/incomplete, database is damaged, state is damaged, or user explicitly requests rebuild

## Progressive Disclosure

Future agents should read in this order:

1. `SKILL.md`
2. `references/operations-playbook.md`
3. `references/duckdb-schema.md`
4. `references/project-guide-template.md`
5. `references/table-dictionary.md`
6. `scripts/*` only when execution or patching is required
