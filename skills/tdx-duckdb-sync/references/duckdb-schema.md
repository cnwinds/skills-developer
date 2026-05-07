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
- `etf_meta`
- `lof_meta`
- `fund_nav_snapshot`
- `map_offsets`
- `derivatives_meta`
- `corporate_action`
- `source_manifest`

## Daily columns

- `market` string (`sh` / `sz` / `bj`)
- `symbol` string (e.g. `000001`)
- `secid` string (e.g. `sh000001`)
- `trade_date` int32 (`yyyymmdd`)
- `open`, `high`, `low`, `close` float32
- `amount` float64
- `volume` int64
- `hfq_factor` float64: 后复权累计因子
- `hfq_open` float32: 后复权开盘价（`open * hfq_factor`）
- `hfq_high` float32: 后复权最高价（`high * hfq_factor`）
- `hfq_low` float32: 后复权最低价（`low * hfq_factor`）
- `hfq_close` float32: 后复权收盘价（`close * hfq_factor`）

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

Important caveats:

- `corporate_action` depends on successful `gbbq` parsing via `pytdx`.
- Historical daily ST status is not part of this schema. Snapshot sources such as `security_master.name` and `block_member` should not be treated as point-in-time ST history.
- `hfq_factor` and `hfq_ohlc` currently use confirmed `gbbq` categories `1` and `11`. Keep raw OHLC for exchange-rule studies and use `hfq_ohlc` for total-return style research and backtests.

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
