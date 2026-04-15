---
name: tdx-parquet-sync
description: Build and operate a full TongDaXin data collection system that converts local bars and reference caches (`vipdoc/*/lday/*.day`, `vipdoc/*/fzline/*.lc5`, `T0002/hq_cache/*`) into Parquet, then maintains daily incremental updates. Trigger when user asks to collect/sync TDX data, build a TongDaXin pipeline, or says phrases like "bang wo caiji tdx de shuju".
---

# TDX Parquet Sync

Use this skill to set up and run a production-style TDX collection pipeline:

1. One-time full bootstrap.
2. Daily incremental sync at fixed time.
3. Parquet output ready for DuckDB/Polars/pyarrow.

## Mandatory Confirmation Rule

Before running any setup/bootstrap/sync command, confirm output directory with user.

1. Ask user to confirm `OutputRoot`.
2. If user did not provide a path, propose `C:\tdx_parquet` and wait for explicit confirmation.
3. Do not execute commands that write files until output directory is confirmed.

## Standard Workflow

When user asks to collect/sync TDX data (or equivalent), do this by default:

1. Confirm output directory with user first.
2. Run one-command setup script (`setup_tdx_collection_system.ps1`) with confirmed output root.
3. Ensure full bootstrap is executed once.
4. Ensure daily incremental task exists at `16:00`.
5. Report output root and summary file paths.

Default parameters:

- `TdxRoot`: `C:\new_tdx64`
- `OutputRoot`: `C:\tdx_parquet`
- `DailyTime`: `16:00`
- `TaskName`: `TDX_Incremental_Daily_1600`

## One-Command Setup

Use this first unless user requests custom behavior:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1"
```

This command does:

1. Install dependencies from `scripts/requirements.txt`.
2. Run full bootstrap (`--full-rebuild`) for `daily,min5,reference`.
3. Generate incremental runner script at `<output-root>\run_tdx_incremental_daily.ps1`.
4. Create/update daily scheduled task (`SYSTEM` account).

## Custom Setup Examples

### Custom paths and time

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1" `
  -TdxRoot "D:\new_tdx64" `
  -OutputRoot "E:\tdx_parquet" `
  -DailyTime "15:50" `
  -TaskName "TDX_Incremental_Daily_1550"
```

### Only create schedule (skip full bootstrap)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1" `
  -SkipBootstrap
```

### Only full bootstrap, no schedule

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1" `
  -SkipSchedule
```

## Manual Run Commands

### Full bootstrap (manual)

```powershell
python "<skill_dir>\scripts\sync_tdx_full_to_parquet.py" `
  --tdx-root "C:\new_tdx64" `
  --output-root "C:\tdx_parquet" `
  --datasets daily,min5,reference `
  --markets sh,sz,bj `
  --full-rebuild `
  --summary-json "C:\tdx_parquet\full_run_summary.json"
```

### Incremental sync (manual)

```powershell
python "<skill_dir>\scripts\sync_tdx_full_to_parquet.py" `
  --tdx-root "C:\new_tdx64" `
  --output-root "C:\tdx_parquet" `
  --datasets daily,min5,reference `
  --markets sh,sz,bj `
  --summary-json "C:\tdx_parquet\last_run_summary.json"
```

## Update Strategy

- Bars (`daily/min5`): per-file append watermark in `_state/tdx_sync_state.json`, parse only new records.
- Reference (`reference`): compare source file signatures `(size, mtime_ns)`, rebuild only when changed.
- Default daily operation: scheduled incremental sync at `16:00`.

## Recovery

- State corruption or major source reset: run full bootstrap once.
- Strict append-only requirement: run manual command with `--fail-on-reset`.
- Temporary source file disappearance: run manual command with `--keep-stale-state`.

## Post-Run Checks

- Ensure summary JSON exists under output root.
- Ensure state file exists: `<output-root>\_state\tdx_sync_state.json`.
- For schedule mode, verify task:
  - `schtasks /Query /TN "TDX_Incremental_Daily_1600" /V /FO LIST`

## References

- Read [operations-playbook.md](references/operations-playbook.md) for initialization/update SOP.
- Read [parquet-schema.md](references/parquet-schema.md) for layout and query examples.
- Read [table-dictionary.md](references/table-dictionary.md) for table columns and meanings.
