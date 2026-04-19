---
name: capital-backtest
description: 按「资金账户」的视角对股票策略做长区间回测 —— 同一策略在多个板块/子组合上各开一个独立账户、再做一个组合账户，输出逐笔流水、按卖出月汇总、年度汇总、回撤/Sharpe/月胜率、权益曲线图、月度热力图、以及多 sheet 的 xlsx 报表。触发场景：用户提到「回测策略」「评估新策略」「按板块/KCB/CYB 分账户跑」「20 万资金 / 初始资金 N 万」「按月/按年统计收益」「权益曲线」「最大回撤」「月度热力图」「Qinglong keep CSV」「930-00c buy_signals」，或者给到一张含 buy_date/sell_date/buy_price/sell_price 的交易候选表并希望评估其表现时，请使用本 skill。即使用户没有明说「skill」，只要涉及「这个策略表现怎么样 / 把一个新策略按这一套跑一遍」，也优先选它。
---

# capital-backtest

把一套策略（已决定了 buy_date、sell_date、买入价、卖出价）按「资金账户」的角度，
像真人一样用有限现金分配去跑回测，给出可对外发的完整报告包。

## 何时用这个 skill

- 用户给了一个新策略/新 setting 版本，想评估它长期表现。
- 用户说「按 KCB、CYB 各开一个账户」或者「再加一个组合账户」。
- 用户想看「月度已实现盈亏」「按年汇总」「最大回撤」「Sharpe」「月胜率」。
- 用户希望直接拿到权益曲线 PNG、月度热力图，以及可以发给老板的 xlsx。
- 用户想改股票池或时间段在同一策略上做对比。

如果用户只是想看「信号胜率、平均收益」这种样本统计（不用现金约束），这套 skill 就有点重了，
可以直接用 pandas groupby。但只要一旦涉及「有限资金 + 单笔上限 + 跨月累计」，就走这里。

## 架构：一层引擎 + 多个 adapter

```
 用户策略输出 ──► adapter (策略 → 标准 trade table)
                     │
                     ▼
            公共过滤（--pool / --date-from / --date-to）
                     │
                     ▼
                  engine.py ──► 月度/年度/权益/metrics/图/xlsx
```

- **engine / metrics / plots / render_xlsx**：纯函数，和具体策略无关。这一层**不要**为某个策略改动。
- **adapter**：在 `scripts/adapters/` 下，每个 adapter 负责把一种策略输出格式变成标准 trade table。
  新策略接入只需要新增 `adapters/{slug}.py`，其它不用动。契约见 `references/adapter_contract.md`。
- **run_backtest.py**：编排层，暴露 `run --adapter NAME` 入口，以及所有公共 flag（包括
  **股票池** 和 **时间段**）。

## 已注册 adapter

查看最新列表：

```bash
# 在 skills-developer 仓库根目录，或把路径换成你本机 .claude/skills/capital-backtest/...
python skills/capital-backtest/scripts/run_backtest.py list-adapters
```

当前出厂带：

| slug | 适用场景 |
| --- | --- |
| `generic` | 任意 CSV，靠 `--col-map` / `--filter` / `--bucket-from-code` 拼装。80% 新策略够用。 |
| `930_00c` | 本项目 `04_outputs/930_00c_buy_signals_*_en.csv`（中文列 + `sell_*_Nd` + `is_buy` + 板块推断）。 |
| `qinglong` | 原 Qinglong 项目的 `930-QINGLONG-R6090_*.csv`；在非 Qinglong 项目会报清晰错误。 |

## 怎么跑

### 最常用：`run --adapter NAME`

```bash
python skills/capital-backtest/scripts/run_backtest.py run \
    --adapter 930_00c \
    --input 04_outputs/930_00c_buy_signals_201511_en.csv \
    --input 04_outputs/930_00c_buy_signals_201512_en.csv \
    --exit-horizon 8d \
    --tier-filter S1,S2 \
    --initial-cash 200000 --per-stock-cap 150000 \
    --out-dir 04_outputs/capital_backtest_930_00c_201511_201512
```

adapter 专属的 flag（`--input / --exit-horizon / --tier-filter / --bucket-mode` 等）直接
跟在 `run --adapter 930_00c` 后面即可；公共 flag（`--initial-cash / --per-stock-cap /
--pool / --date-from / --date-to / --out-dir`）在哪个位置都认。

### 股票池和时间段（公共 flag，任何 adapter 都能用）

- `--pool`：
  - 逗号分隔的 inline 列表：`--pool 300258.SZ,600519.SH`
  - JSON 文件（数组、`{codes:[...]}` 或像 `pool_j.json` 那样的对象键）：`--pool 03_inputs/pool_j.json`
  - CSV / TXT 文件（第一列，支持带 header）：`--pool my_pool.csv`
  - **默认不传 = 用策略 adapter 自己带的池**。
- `--date-from`：买入日下界，**默认 20140101**。
- `--date-to`：买入日上界，**默认 = 今天**。

过滤作用在 adapter 产出标准 trade table 之后、喂给 engine 之前，每个 adapter 都自动继承。

### 兼容老调用

```bash
# 老写法（保留，等价于 run --adapter generic）
python scripts/run_backtest.py from-trades --input trades.csv --initial-cash 200000 --per-stock-cap 150000

# 老写法（保留，等价于 run --adapter qinglong）
python scripts/run_backtest.py from-qinglong --keep-csv KCB=... --keep-csv CYB=...
```

## 工作流（skill 被触发时 Claude 的行为）

1. **先嗅探输入格式**：看用户给的 CSV / 路径 / 列名，能不能命中已知 adapter。
   - 中文列 `买入日 / 买入价` + `sell_date_{N}d` + `is_buy` → 几乎肯定是 `930_00c`。
   - 文件名含 `QINGLONG-R6090` 且有 `02_core/run_qinglong_predict_zi.py` → `qinglong`。
   - 已经是标准列 `code,bucket,buy_date,buy_price,sell_date,sell_price` → `generic`（零配置）。
2. **命中就直接跑**。跑完在回复里告知"用了 X adapter + 默认时间 20140101 至今 + 默认股票池"。
3. **没命中就最多问 3 个问题**（用 AskUserQuestion）：
   - Q1. 买入/卖出列分别是哪几列？（列名选项从 CSV 头抽）
   - Q2. bucket 怎么定？按 code 推 / 固定一个名 / 用 CSV 里某一列
   - Q3. 要不要过滤行？推荐 `is_buy == True` / 不过滤 / 自定义
4. **选择落地方式**：
   - 能用 `generic + CLI flags` 搞定 → 直接合成命令跑。
   - 用户说"以后都会用这个" → 基于 `930_00c.py` 的骨架生成 `adapters/{slug}.py`，
     告诉用户下次 `--adapter {slug}` 就能复用。

## 输出包含什么

每次跑完都会在 `--out-dir` 里写下：

- `{bucket}_trade_log.csv` — 逐笔事件流水，含 BUY/SELL 两类行、成交/跳过、现金余额、事件后权益。
- `{bucket}_monthly.csv` — 按**卖出月**汇总，含 closed_trades / wins / losses / 月胜率 /
  当月已实现 pnl / 累计 pnl / 累计收益率 / 当月末回撤。
- `{bucket}_yearly.csv` — 按年汇总（笔数、胜率、收益）。
- `{bucket}_equity.csv / _equity.png` — 每个事件日的权益曲线（cash + open_cost）。
- `{bucket}_heatmap.png` — 年 × 月的 PnL 热力图。
- `summary.csv / metrics.csv / summary.json` — 跨 bucket 对比。`summary.json` 会额外
  记录 adapter 名、`pool`、`date_from`、`date_to`，方便复现。
- `report_{label}.xlsx` — 多 sheet 报表：`summary / metrics / {bucket}_monthly /
  {bucket}_yearly / {bucket}_trades`。
- `README.md` — 人类可读的摘要。
- 如果 bucket ≥ 2 且没有 `--no-combine`，额外出 `ALL_*` 作为「组合账户」视角。

## 关键设计 / 口径

- **同日多笔**：当日可用现金按「当日剩余买单数」均分，单笔再按 `per_stock_cap` 截断。
- **成交顺序**：同一事件日「先卖后买」。回款进入当日可分配现金 → **复利滚动**。
- **手数**：`floor(alloc / (buy_price * (1 + 佣金率)) / 100) * 100`。
- **费用**：`--commission-bps` 双边、`--stamp-duty-bps` 仅卖出。单笔最小成交用 `--min-amount` 控制。
  跳过原因写到 `skip_reason`：`alloc_below_min_amount / insufficient_cash_for_1_lot / cash_lt_cost`。
- **权益近似**：没有持仓期逐日盯市价格，用 `initial + 累计已实现盈亏` 作为月末权益近似。
  Sharpe / Sortino / Calmar 都建立在月度这条曲线上。
- **默认兼容老口径**：佣金、印花税、最小金额都默认 0；`from-qinglong`（在 Qinglong 项目）与
  `04_outputs/capital_20w_201908_202604/` 的数据一致。smoke test 校对过。

## 进一步阅读

- `references/adapter_contract.md` — **怎么写一个新 adapter**（4 样必须暴露的东西 + 模板）。
- `references/input_schema.md` — 标准 trade table 的列定义与别名。
- `references/output_layout.md` — 输出文件逐列说明。
- `references/metrics_glossary.md` — 回撤 / Sharpe / Calmar / 月胜率 等指标的计算口径。
- `references/examples.md` — 更多调用示例（新策略接入、成本测试、组合账户切换、股票池对比等）。

## 常见坑

- **sell_date 必须覆盖完**：引擎会把所有未平仓作为错误抛出。trade table 里一定是「已经到期且能卖掉」的笔。
  持仓尚未到期的行请在上游/adapter 过滤掉（如 `930_00c` adapter 的 `is_buy` + `sell_date` 非空检查）。
- **日期必须能排序**：`buy_date/sell_date` 接受 `YYYYMMDD` 字符串、`YYYY-MM-DD`、datetime，
  都会被清洗成 8 位数字。
- **bucket 必须有值**：可以是 KCB/CYB/板块名/策略变体名。如果 CSV 本来就只有一个桶，
  用 `--bucket-default` 或 `BUCKET=path.csv` 形式注入；也可以 `--bucket-from-code`。
- **xlsx 太大会截断**：单个 `_trades` sheet 最多写 20k 行，更多的笔请看 `{bucket}_trade_log.csv`。
- **股票池过滤在 adapter 之后**：如果 adapter 自己已经先按 pool 跑过一轮（例如 Qinglong），
  再 `--pool` 只是收窄；不会让 adapter 去看更多股票。

## 修改这个 skill 时请注意

- 对「回测口径」的任何改动（手数、顺序、先买后卖等）都要更新 smoke test 并在
  `references/metrics_glossary.md` 记录改动原因。
- `engine.py` 是纯函数，不依赖项目。新 adapter 应该产 DataFrame 后再喂进 engine，
  避免把业务逻辑泄漏进来。
- `_contract.py` 里的 `infer_board` 是 vendored 自项目内 `02_core/board_classifier.py` 的版本，
  修改时必须同步，并让 `tests/smoke_930_00c.py` 的一致性校验通过。
