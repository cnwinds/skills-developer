# 常见调用例子

例子里的相对路径（如 `04_outputs/...`）假定当前目录是**策略项目根**（含 `04_outputs/` 等）。

`SCRIPT` 指向 `run_backtest.py` 的实路径，二选一即可：

- 本仓库（[skills-developer](https://github.com/cnwinds/skills-developer)）根目录：`skills/capital-backtest/scripts/run_backtest.py`
- 已复制到本机 Claude Code skill 目录：`.claude/skills/capital-backtest/scripts/run_backtest.py`

## 0. 新的统一入口：`run --adapter`

推荐所有新调用都走这个形式：

```bash
# 看有哪些 adapter 可用
python $SCRIPT list-adapters

# 跑本项目 930-00c 的 buy_signals（S1+S2）
python $SCRIPT run --adapter 930_00c \
    --input 04_outputs/930_00c_buy_signals_201511_en.csv \
    --input 04_outputs/930_00c_buy_signals_201512_en.csv \
    --tier-filter S1,S2 \
    --initial-cash 200000 --per-stock-cap 150000 \
    --out-dir 04_outputs/capital_backtest_930_00c_201511_201512
```

下方原有的 `from-qinglong / from-trades` 示例仍然可用（作为兼容壳），不必改。

### 股票池和时间段（任何 adapter 通用）

```bash
# 用项目 pool_j.json 做过滤池；时间窗 2018-2022
python $SCRIPT run --adapter 930_00c \
    --input 04_outputs/930_00c_buy_signals_201511_en.csv \
    --pool 03_inputs/pool_j.json \
    --date-from 20180101 --date-to 20221231 \
    --out-dir 04_outputs/capital_backtest_pool_j_2018_2022
```

- `--pool` 支持 inline 代码列表（逗号分隔）/ `.json` / `.csv` / `.txt`。
- `--date-from` 默认 **20140101**，`--date-to` 默认 **今天**。
- 默认不加 `--pool` 就用策略 adapter 自带池。

### 不写新 adapter：generic + 列映射 + 过滤

输入含中文列 `买入日 / 买入价` + 多窗口 `sell_date_8d` 的任意 CSV：

```bash
python $SCRIPT run --adapter generic \
    --input 04_outputs/my_strategy_trades.csv \
    --col-map sell_date=sell_date_8d \
    --col-map sell_price=sell_price_8d \
    --filter "is_buy == True" \
    --bucket-from-code
```

`买入日 / 买入价 / ts_code` 已在 generic 内置别名里，无需显式映射。

---

## 1. 复刻 20w 分账户回测

和 `04_outputs/capital_20w_201908_202604/` 完全一致：

```powershell
python skills\capital-backtest\scripts\run_backtest.py from-qinglong `
    --keep-csv KCB=04_outputs\930-QINGLONG-R6090_KCB_201908_202604_v3.csv `
    --keep-csv CYB=04_outputs\930-QINGLONG-R6090_CYB_201908_202604_v3.csv `
    --setting-version KCB=v22,CYB=v1 `
    --initial-cash 200000 --per-stock-cap 150000 `
    --package-dir . `
    --out-dir 04_outputs\capital_backtest_KCB_CYB_201908_202604
```

多出来的东西相比老版：`ALL_*` 组合账户、`{bucket}_yearly.csv`、`{bucket}_equity.*`、`*_heatmap.png`、`report_*.xlsx`、`metrics.csv`。

## 2. 同一张 keep CSV、加上 A 股常见成本假设

```powershell
python skills\capital-backtest\scripts\run_backtest.py from-qinglong `
    --keep-csv KCB=04_outputs\930-QINGLONG-R6090_KCB_201908_202604_v3.csv `
    --commission-bps 2.5 --stamp-duty-bps 10 --min-amount 2000 `
    --initial-cash 200000 --per-stock-cap 150000 `
    --package-dir . `
    --out-dir 04_outputs\capital_backtest_KCB_withcost
```

## 3. 新策略：先自己生成一张 trade table

假设你在 Jupyter 里算出一份策略 CSV：

```python
df = pd.DataFrame({
    "code": ...,
    "name": ...,
    "bucket": ...,      # 板块或策略变体名
    "signal_date": ...,
    "buy_date": ...,
    "buy_price": ...,
    "sell_date": ...,
    "sell_price": ...,
    "rank": ...,        # 可选
})
df.to_csv("04_outputs/my_strategy_trades.csv", index=False)
```

然后：

```bash
python skills/capital-backtest/scripts/run_backtest.py from-trades \
    --input 04_outputs/my_strategy_trades.csv \
    --initial-cash 200000 --per-stock-cap 150000 \
    --out-dir 04_outputs/my_strategy_eval
```

如果 CSV 里没有 `bucket` 列，还可以手工指定：

```bash
--input KCB=04_outputs/my_kcb_trades.csv
--input CYB=04_outputs/my_cyb_trades.csv
```

## 4. 只想快速跑一下、不要 xlsx 和图

```bash
python scripts/run_backtest.py from-trades --input my.csv \
    --no-plots --no-xlsx
```

CSV 和 JSON 依然会产出。

## 5. 只要单个 bucket（不需要组合账户）

```bash
python scripts/run_backtest.py from-trades --input KCB=my_kcb.csv --no-combine
```

## 6. 对比两个策略（例如 v22 vs v23）

让两个策略都产 trade table，各自一个 bucket：

```bash
python scripts/run_backtest.py from-trades \
    --input V22=04_outputs/v22_trades.csv \
    --input V23=04_outputs/v23_trades.csv \
    --label v22_vs_v23 \
    --out-dir 04_outputs/compare_v22_vs_v23
```

这样 `summary.csv / metrics.csv / report_*.xlsx` 里会有 V22、V23、ALL 三行，直接对比。

## 7. 把结果和老的 `QINGLONG_KCB_vs_CYB_compare` 放一起看

老的 compare 报告偏向「样本胜率 + 样本均值」的比较（不考虑资金约束）；
capital-backtest 的结果偏向「真账户能跑出多少钱」。两份报告相互印证，不冲突：

- compare 里某年胜率高但资金回测里该年 pnl 反而低 → 提示「高胜率但小盈大亏」。
- compare 里胜率平平但资金回测赚得多 → 提示「命中大涨股的能力」。
