# 输出文件布局

所有产物都落在 `--out-dir` 下，每个 bucket 一组，再加一组跨 bucket 的汇总。

## `{bucket}_trade_log.csv`（逐笔流水）

每一笔交易在表里会出现两行：一行 `BUY`、一行 `SELL`（共享同一个 `trade_id`）。被跳过的买单只出现一行 BUY，`skipped=True`。

| 列 | 说明 |
| --- | --- |
| `event` | `BUY` / `SELL` |
| `trade_id` | 同一笔交易从买到卖共用同一个整数 |
| `date` | 当前事件日 |
| `bucket` | 桶 / 板块 |
| `code` / `name` | 证券代码与简称 |
| `signal_date` | 信号日，仅展示 |
| `buy_date` / `sell_date` | 建仓、平仓日 |
| `buy_price` / `sell_price` | 成交价 |
| `shares` | 成交股数（按 100 股一手向下取整）。跳过单为 0 |
| `alloc_cash` | 买入时分配到的意向资金 = `min(当时可用现金/当日剩余笔数, per_stock_cap)` |
| `cost` | 买入实际成本 = 股数 × buy_price + 买入佣金；SELL 行重复同一数值便于核对 |
| `proceeds` | 卖出净回款 = 股数 × sell_price − 卖出佣金 − 印花税。仅 SELL 行有值 |
| `fee` | 本行产生的费用（买入佣金或卖出佣金+印花税） |
| `pnl` | 本笔已实现盈亏 = proceeds − cost。仅 SELL 行有值 |
| `pnl_pct_on_cost` | (proceeds/cost − 1) × 100。仅 SELL 行有值 |
| `cash_after` | 本行事件记账完成后的账户现金 |
| `equity_after` | 本行事件后的权益（cash + 持仓成本） |
| `skipped` | True 表示被跳过 |
| `skip_reason` | `alloc_below_min_amount` / `insufficient_cash_for_1_lot` / `cash_lt_cost` |

## `{bucket}_monthly.csv`（卖出月汇总）

按**卖出日**所在自然月聚合已平仓交易。

| 列 | 说明 |
| --- | --- |
| `year_month` | `YYYY-MM` 便于阅读 |
| `month` | `YYYYMM` 整型 |
| `closed_trades` | 当月平仓笔数 |
| `wins` / `losses` | 盈亏笔数 |
| `win_rate_pct` | 当月胜率 |
| `realized_pnl` | 当月已实现盈亏合计 |
| `cum_pnl` | 自回测起点到当月末的累计已实现盈亏 |
| `cum_return_pct` | 累计收益率 = cum_pnl / initial_cash × 100 |
| `drawdown_pct` | 当月末相对历史峰值的回撤（基于月度近似权益） |

## `{bucket}_yearly.csv`（年度汇总）

按 `year_month` 的年份聚合：`closed_trades`、`wins`、`losses`、`win_rate_pct`、`realized_pnl`、`return_pct`。

## `{bucket}_equity.csv`（事件日权益）

| 列 | 说明 |
| --- | --- |
| `date` | 事件日 |
| `cash` | 当日事件全部处理完后的现金 |
| `open_cost` | 当日结束时的持仓成本合计 |
| `equity` | cash + open_cost |

## 跨 bucket 汇总

- `summary.csv` — 每个 bucket 一行，含 `initial_cash / final_cash / total_realized_pnl / total_return_pct / executed_buys / skipped_buys / closed_sells`。
- `metrics.csv` — 每个 bucket 一行，含高阶指标（见 `metrics_glossary.md`）。
- `summary.json` — 完整机器可读汇总，适合继续二次加工。
- `report_{label}.xlsx` — 多 sheet 报表，方便外发。
- `README.md` — 人类可读汇总，包含规则摘要 + 各 bucket 表格。

## 组合账户

当 `bucket ≥ 2` 且未传 `--no-combine` 时，会额外出一组 `ALL_*`，代表「所有 bucket 共享一个账户」的视角：

- 同日买入均分资金的分母是**跨 bucket 当日合计买单数**。
- 单股限额依旧生效。
- 它和单 bucket 相加不等：当某 bucket 信号密集时，另一 bucket 闲置的现金也会参与分配。
