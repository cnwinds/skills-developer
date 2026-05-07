# TDX Collection Operations Playbook

## Goal

Maintain a stable local pipeline:

1. Full bootstrap once.
2. Daily incremental update at `16:00`.
3. DuckDB outputs always queryable from a single database file.

## Required Confirmation

Before any setup or sync command, confirm output directory with user.

1. Ask user to confirm output root path.
2. If missing, propose `C:\tdx_duckdb` and wait for explicit confirmation.
3. Start commands only after confirmation.

## Required Inspection

After output root is confirmed, inspect current state before deciding what to run.

1. Check whether output root already contains files.
2. Check whether `tdx.duckdb` exists.
3. Check whether `_state\tdx_sync_state.json` exists.
4. Treat empty or incomplete output roots as bootstrap cases.
5. Treat complete output roots as completion/update cases.

## Initialization Standard

Run once on a new machine or on an empty/incomplete output root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "<skill_dir>\scripts\setup_tdx_collection_system.ps1"
```

Expected result:

- `C:\tdx_duckdb\tdx.duckdb`
- `C:\tdx_duckdb\_state\tdx_sync_state.json`
- `C:\tdx_duckdb\run_tdx_incremental_daily.ps1`
- Task `TDX_DuckDB_Incremental_Daily_1600` (daily at 16:00, `SYSTEM`)

If output root already contains a complete dataset and state file, do not rebuild by default. Only regenerate runner/task or run incremental sync unless user explicitly asks for full rebuild.

## Daily Update Standard

Incremental update is done by scheduled task:

- Task name: `TDX_DuckDB_Incremental_Daily_1600`
- Command target: `run_tdx_incremental_daily.ps1`
- Summary output: `C:\tdx_duckdb\last_run_summary.json`

Manual run equivalent:

```powershell
python "<skill_dir>\scripts\sync_tdx_to_duckdb.py" `
  --tdx-root "C:\new_tdx64" `
  --output-root "C:\tdx_duckdb" `
  --datasets daily,min5,reference `
  --markets sh,sz,bj `
  --summary-json "C:\tdx_duckdb\last_run_summary.json"
```

## Recovery Standard

- Source/state mismatch or damaged state: run full bootstrap (`--full-rebuild`) once.
- Need strict append-only checks: add `--fail-on-reset`.
- Temporary missing files should not clear state: add `--keep-stale-state`.
- If `corporate_action` is missing or empty after a run, first verify `pytdx` is installed in the Python environment used by the sync command, then rerun `--datasets reference` or the full selected sync.
- If `hfq_factor` / `hfq_open` / `hfq_high` / `hfq_low` / `hfq_close` are missing after a run, rerun `--datasets daily,reference` or at least any sync path that triggers the daily post-adjustment refresh.

## Data Caveats

- `gbbq` can provide corporate actions / ex-rights-ex-dividend events and is suitable for rebuilding adjustment factors.
- TDX raw files do not provide a historical daily ST flag table. Snapshot files such as `infoharbor_block.dat` or current security names must not be used as historical ST filters in a no-lookahead backtest.
- The current post-adjustment implementation writes `hfq_factor` and `hfq_open/high/low/close` into `daily`, using confirmed `gbbq` categories `1` and `11`.
- `hfq_ohlc` is appropriate for total-return style research. Raw OHLC should still be kept for limit-up/limit-down checks, auction logic, and exchange-rule analysis.

## Current Project Guide Update

After setup/update finishes, write key information into the user's current project global guide file.

1. Prefer project root `AGENTS.md`.
2. If missing, create project root `AGENTS.md`.
3. Add a `TDX Data Collection` section using `project-guide-template.md`.
4. Include:
   - confirmed output root
   - TDX root
   - database path
   - daily task time and task name
   - runner script path
   - state file path
   - summary JSON path
   - progressive disclosure order for this skill

## Verification Checklist

1. Check task:
   - `schtasks /Query /TN "TDX_DuckDB_Incremental_Daily_1600" /V /FO LIST`
2. Check `tdx.duckdb` exists.
3. Check last summary JSON exists.
4. Check state file timestamp updates after run.
5. Spot check row counts in DuckDB:
   - `select count(*) from daily;`
   - `select count(*) from min5;`
   - `select count(*) from corporate_action;`
   - `select min(ex_date), max(ex_date) from corporate_action;`
   - `select count(*) from daily where hfq_factor is null or hfq_open is null or hfq_high is null or hfq_low is null or hfq_close is null;`
