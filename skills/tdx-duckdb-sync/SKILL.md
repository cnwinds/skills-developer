---
name: tdx-duckdb-sync
description: Build and operate a TongDaXin data collection system that converts local bars and reference caches (`vipdoc/*/lday/*.day`, `vipdoc/*/fzline/*.lc5`, `T0002/hq_cache/*`) into a single DuckDB database file, then maintains daily incremental updates. Trigger when user asks to collect/sync TDX data, build a TongDaXin pipeline, or says phrases like "bang wo caiji tdx de shuju".
---

# TDX DuckDB Sync

Use this skill to set up and run a production-style TDX collection pipeline:

1. One-time full bootstrap into a single DuckDB file.
2. Daily incremental sync at fixed time.
3. Fast local querying from DuckDB without scanning partitioned shard trees.

## Mandatory Confirmation Rule

Before running any setup/bootstrap/sync command, confirm output directory with user.

1. Ask user to confirm `OutputRoot`.
2. If user did not provide a path, propose `C:\tdx_duckdb` and wait for explicit confirmation.
3. Do not execute commands that write files until output directory is confirmed.

## Mandatory Inspection Rule

After output directory is confirmed and before execution, inspect target directory state.

1. Check whether output root already has content.
2. Check whether `<output-root>\tdx.duckdb` exists.
3. Check whether `_state\tdx_sync_state.json` exists.
4. Decide whether to run full bootstrap or only complete missing runtime pieces.

Interpretation:

- Empty or incomplete output root: run full bootstrap and then create/update daily task.
- Existing complete output root: do not rebuild by default; only complete missing runner/task pieces unless user explicitly requests rebuild.

## Standard Workflow

When user asks to collect/sync TDX data (or equivalent), do this by default:

1. Confirm output directory with user first.
2. Inspect the confirmed output root and determine whether it is empty, partial, or already initialized.
3. Run one-command setup script (`setup_tdx_collection_system.ps1`) with confirmed output root.
4. Ensure full bootstrap is executed once when output root is empty or incomplete.
5. Ensure daily incremental task exists at `16:00`.
6. Update the user's current project global guide file with key runtime information.
7. Report output root, database path, task name, summary path, and guide file path.

Default parameters:

- `TdxRoot`: `C:\new_tdx64`
- `OutputRoot`: `C:\tdx_duckdb`
- `DatabasePath`: `C:\tdx_duckdb\tdx.duckdb`
- `DailyTime`: `16:00`
- `TaskName`: `TDX_DuckDB_Incremental_Daily_1600`

## One-Command Setup

Use this first unless user requests custom behavior:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1"
```

This command does:

1. Install dependencies from `scripts/requirements.txt` (with PyPI fallback for `duckdb`).
2. Inspect output root and decide whether a full bootstrap is needed.
3. Run full bootstrap (`--full-rebuild`) only when output root is empty/incomplete, or when forced.
4. Generate incremental runner script at `<output-root>\run_tdx_incremental_daily.ps1`.
5. Create/update daily scheduled task (`SYSTEM` account).

## Custom Setup Examples

### Custom paths and time

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1" `
  -TdxRoot "D:\new_tdx64" `
  -OutputRoot "E:\tdx_duckdb" `
  -DailyTime "15:50" `
  -TaskName "TDX_DuckDB_Incremental_Daily_1550"
```

### Only create schedule (skip full bootstrap)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1" `
  -SkipBootstrap
```

### Only full bootstrap, no schedule

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1" `
  -ForceBootstrap `
  -SkipSchedule
```

## Manual Run Commands

### Full bootstrap (manual)

```powershell
python "<skill_dir>\scripts\sync_tdx_to_duckdb.py" `
  --tdx-root "C:\new_tdx64" `
  --output-root "C:\tdx_duckdb" `
  --datasets daily,min5,reference `
  --markets sh,sz,bj `
  --full-rebuild `
  --summary-json "C:\tdx_duckdb\full_run_summary.json"
```

### Incremental sync (manual)

```powershell
python "<skill_dir>\scripts\sync_tdx_to_duckdb.py" `
  --tdx-root "C:\new_tdx64" `
  --output-root "C:\tdx_duckdb" `
  --datasets daily,min5,reference `
  --markets sh,sz,bj `
  --summary-json "C:\tdx_duckdb\last_run_summary.json"
```

## Update Strategy

- Bars (`daily/min5`): per-file append watermark in `_state\tdx_sync_state.json`, parse only new records unless a source file changes size/mtime without growth, in which case the corresponding symbol is rebuilt inside DuckDB.
- Reference tables: compare source file signatures `(size, mtime_ns)`, rebuild only when changed.
- Reference layer includes corporate actions (`gbbq`, via pytdx) and fund metadata snapshots (`specetfdata.txt`, `speclofdata.txt`, `specjjdata.txt`).
- Default daily operation: scheduled incremental sync at `16:00`.

## Current Project Guide File

After setup/sync is complete, update the current project's global guide file.

1. Prefer project-root `AGENTS.md`.
2. If project-root `AGENTS.md` does not exist, create it.
3. Add or update a `TDX Data Collection` section using [project-guide-template.md](references/project-guide-template.md).
4. Write the confirmed runtime values:
   - `TdxRoot`
   - `OutputRoot`
   - `DatabasePath`
   - `DailyTime`
   - `TaskName`
   - `RunnerPath`
   - `StateFile`
   - `LastSummaryJson`
5. Also include the progressive disclosure order for this skill so future agents know what to read first.

## Recovery

- State corruption or major source reset: run full bootstrap once.
- Strict append-only requirement: run manual command with `--fail-on-reset`.
- Temporary source file disappearance: run manual command with `--keep-stale-state`.

## Post-Run Checks

- Ensure `<output-root>\tdx.duckdb` exists.
- Ensure summary JSON exists under output root.
- Ensure state file exists: `<output-root>\_state\tdx_sync_state.json`.
- For schedule mode, verify task:
  - `schtasks /Query /TN "TDX_DuckDB_Incremental_Daily_1600" /V /FO LIST`

## References

- Read [operations-playbook.md](references/operations-playbook.md) for initialization/update SOP.
- Read [duckdb-schema.md](references/duckdb-schema.md) for database layout and query examples.
- Read [project-guide-template.md](references/project-guide-template.md) when updating the user's current project guide file.
- Read [table-dictionary.md](references/table-dictionary.md) for table columns and meanings.
