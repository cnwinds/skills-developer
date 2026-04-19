# 指标定义

所有高阶指标都在 `scripts/metrics.py` 的 `compute_metrics()` 里算出来，分两类：基于月度净值近似的「时间序列指标」和基于逐笔的「笔级指标」。

## 月度净值近似

我们没有持仓期的逐日盯市价格，所以把月末权益近似为：

```
equity_t = initial_cash + cum_realized_pnl_at_month_end_t
```

这条曲线**只在卖出发生时才会跳一格**（卖出当月的 realized_pnl 进入 cum_pnl）。它低估了持仓期间的浮盈浮亏，但能稳定衡量「真实回到现金里的钱」。

如果以后要逐日 mark-to-market，需要把行情日线接进来 —— 当前 skill 不做。

## 时间序列指标

| 指标 | 计算 |
| --- | --- |
| `total_return_pct` | 总实现收益 / 初始资金 × 100 |
| `cagr_pct` | `(1 + total_return) ** (12 / span_months) - 1`，`span_months` 取首月到末月（含端点） |
| `max_drawdown_pct` | 月末权益相对历史峰值的最大回撤（百分数）|
| `max_drawdown_month` | 出现最大回撤的月（YYYYMM）|
| `monthly_win_rate_pct` | 月度收益 > 0 的月份占比 |
| `best_month` / `best_month_pnl` | 当月已实现 pnl 最高的月 |
| `worst_month` / `worst_month_pnl` | 当月已实现 pnl 最低的月 |
| `sharpe_monthly` | `mean(excess) / std(excess) * sqrt(12)`，`excess = monthly_return - rf_monthly` |
| `sortino_monthly` | 同上，但分母是只取 excess<0 的部分的 std |
| `calmar` | `cagr / |max_drawdown|` |

`risk_free_annual` 默认 0；可以通过 `--risk-free-annual` 调（年化形式，例如 `0.025`）。

## 笔级指标

| 指标 | 计算 |
| --- | --- |
| `trade_win_rate_pct` | 盈利笔数 / 总平仓笔数 × 100 |
| `avg_win_pnl` | 盈利笔的 pnl 均值 |
| `avg_loss_pnl` | 亏损笔的 pnl 均值（保留负号）|
| `profit_factor` | 总盈利金额 / |总亏损金额| |
| `max_consecutive_losses` | 按 `trade_id` 顺序的最长连续亏损笔数 |

## 两种「累计回撤」

需要区分一下：

- `monthly.drawdown_pct`：每月末打点的回撤。
- `metrics.max_drawdown_pct`：上面这条曲线的最小值。

如果要看更细的「事件日回撤」，可以基于 `{bucket}_equity.csv`（事件日权益）自行算 `cummax → drawdown`。
事件日回撤会比月度更深（因为同一月内可能先暴涨再回吐），但口径更接近真实账户体验。

## 默认值的取舍

- 默认 `commission_bps=0`、`stamp_duty_bps=0`：这是为了和 `04_outputs/capital_20w_201908_202604/` 的历史结果对齐，避免无声地破坏旧报表。
- 真实 A 股可参考：佣金双边 2.5 bps（万二点五）、印花税卖出 10 bps（千一）。可以用 `--commission-bps 2.5 --stamp-duty-bps 10` 模拟。
- 单笔最小金额建议设到 1000 ~ 3000，避免极端情况下账户里只剩几百块还要分配一笔。
