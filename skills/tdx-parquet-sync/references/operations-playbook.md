# TDX Collection Operations Playbook

## Goal

Maintain a stable local pipeline:

1. Full bootstrap once.
2. Daily incremental update at `16:00`.
3. Parquet outputs always queryable.

## Required Confirmation

Before any setup or sync command, confirm output directory with user.

1. Ask user to confirm output root path.
2. If missing, propose `C:\tdx_parquet` and wait for explicit confirmation.
3. Start commands only after confirmation.

## Initialization Standard

Run once on a new machine or new output root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1"
```

Expected result:

- `C:\tdx_parquet\daily\...`
- `C:\tdx_parquet\min5\...`
- `C:\tdx_parquet\reference\...`
- `C:\tdx_parquet\_state\tdx_sync_state.json`
- `C:\tdx_parquet\run_tdx_incremental_daily.ps1`
- Task `TDX_Incremental_Daily_1600` (daily at 16:00, `SYSTEM`)

## Daily Update Standard

Incremental update is done by scheduled task:

- Task name: `TDX_Incremental_Daily_1600`
- Command target: `run_tdx_incremental_daily.ps1`
- Summary output: `C:\tdx_parquet\last_run_summary.json`

Manual run equivalent:

```powershell
python "<skill_dir>\scripts\sync_tdx_full_to_parquet.py" `
  --tdx-root "C:\new_tdx64" `
  --output-root "C:\tdx_parquet" `
  --datasets daily,min5,reference `
  --markets sh,sz,bj `
  --summary-json "C:\tdx_parquet\last_run_summary.json"
```

## Recovery Standard

- Source/state mismatch or damaged state: run full bootstrap (`--full-rebuild`) once.
- Need strict append-only checks: add `--fail-on-reset`.
- Temporary missing files should not clear state: add `--keep-stale-state`.

## Verification Checklist

1. Check task:
   - `schtasks /Query /TN "TDX_Incremental_Daily_1600" /V /FO LIST`
2. Check last summary JSON exists.
3. Check state file timestamp updates after run.
4. Spot check row counts in Parquet with DuckDB.
