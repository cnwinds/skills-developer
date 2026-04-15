# Parquet Layout And Schema

## Output layout

- `output-root/daily/market=<market>/trade_date=<yyyymmdd>/part-*.parquet`
- `output-root/min5/market=<market>/trade_date=<yyyymmdd>/part-*.parquet`
- `output-root/reference/*.parquet`
- `output-root/_state/tdx_sync_state.json`

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

- `reference/security_master.parquet`: stock master list from `shs/szs/bjs.tnf`
- `reference/security_profile.parquet`: base profile + financial fields from `base.dbf`
- `reference/security_industry_map.parquet`: stock to industry code map from `tdxhy.cfg`
- `reference/block_definition.parquet`: block definitions from `tdxzs*`, `infoharbor_block.dat`, `*block.dat`
- `reference/block_member.parquet`: block membership from `infoharbor_block.dat`, `*block.dat`
- `reference/index_snapshot.parquet`: per-index snapshot metrics from `tdxzsbase.cfg`
- `reference/security_business.parquet`: business summary extensions from `specgpext.txt`
- `reference/map_offsets.parquet`: offsets from `base.map` and `gbbq.map`
- `reference/derivatives_meta.parquet`: futures/options metadata from `code2name.ini`
- `reference/source_manifest.parquet`: manifest of all `hq_cache` files + parse coverage

For exact column meanings, read `references/table-dictionary.md`.

## Query examples

### DuckDB

```sql
-- Daily latest 20 rows
select *
from read_parquet('D:/tdx_parquet/daily/**/*.parquet')
where secid = 'sh000001'
order by trade_date desc
limit 20;
```

```sql
-- Min5 for one day
select secid, bar_time, open, high, low, close, volume
from read_parquet('D:/tdx_parquet/min5/**/*.parquet')
where trade_date = 20260414 and secid = 'sz000001'
order by bar_time;
```

```sql
-- Block members (A-share only)
select block_name, secid
from read_parquet('D:/tdx_parquet/reference/block_member.parquet')
where secid is not null
order by block_name, secid
limit 200;
```
