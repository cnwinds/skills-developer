# DuckDB Layout And Schema

## Output layout

- `output-root/tdx.duckdb`
- `output-root/_state/tdx_sync_state.json`
- `output-root/run_tdx_incremental_daily.ps1`
- `output-root/last_run_summary.json`

## Core tables

- `daily`
- `min5`
- `security_master`
- `security_profile`
- `security_industry_map`
- `block_definition`
- `block_member`
- `index_snapshot`
- `security_business`
- `map_offsets`
- `derivatives_meta`
- `source_manifest`

## Daily columns

- `market` string (`sh` / `sz` / `bj`)
- `symbol` string (e.g. `000001`)
- `secid` string (e.g. `sh000001`)
- `trade_date` int32 (`yyyymmdd`)
- `open`, `high`, `low`, `close` float32
- `amount` float64
- `volume` int64

## Min5 columns

- `market` string
- `symbol` string
- `secid` string
- `trade_date` int32 (`yyyymmdd`)
- `bar_time` int64 (`yyyymmddHHMM`)
- `open`, `high`, `low`, `close` float32
- `amount` float64
- `volume` int64

## Reference tables

Reference tables mirror the parsed `hq_cache` outputs and use one DuckDB table per logical dataset instead of one Parquet file per table.

For exact column meanings, read `references/table-dictionary.md`.

## Query examples

### DuckDB CLI / Python

```sql
-- Daily latest 20 rows
select *
from daily
where secid = 'sh000001'
order by trade_date desc
limit 20;
```

```sql
-- Min5 for one day
select secid, bar_time, open, high, low, close, volume
from min5
where trade_date = 20260414 and secid = 'sz000001'
order by bar_time;
```

```sql
-- Block members (A-share only)
select block_name, secid
from block_member
where secid is not null
order by block_name, secid
limit 200;
```
