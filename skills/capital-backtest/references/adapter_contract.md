# Adapter 契约：怎么写一个新 adapter

Adapter 的唯一职责：**把一种策略输出格式转成标准 trade table**。
它不做权益计算、不画图、不写 xlsx —— 那些是 engine 层的事。

## 1. 必须暴露的 4 样东西

每个 `adapters/{slug}.py` 都必须有：

```python
NAME = "my_strategy"                 # CLI slug，用在 --adapter 后面
DESCRIPTION = "一行描述，会显示在 list-adapters 里"

def add_arguments(p: argparse.ArgumentParser) -> None:
    # 注册这个 adapter 专属的 CLI 参数。
    # 不要注册 --initial-cash / --pool / --date-from 这种公共 flag —— run_backtest.py 已经有了。
    p.add_argument("--input", action="append", required=True, ...)

def load(args: argparse.Namespace) -> pd.DataFrame:
    # 返回标准 trade table，列见第 2 节。
    ...
```

加载这个 adapter 时 `load_adapter(NAME)` 会校验这 3 个名字是否都有；缺一就报错。

## 2. `load` 必须返回的列

| 列名 | 类型 | 说明 | 必填 |
| --- | --- | --- | --- |
| `code` | str | 证券代码，如 `688006.SH`、`300258.SZ` | ✅ |
| `bucket` | str | 桶/板块/策略变体名 | ✅ |
| `buy_date` | str | `YYYYMMDD` 字符串（或任何能被清洗成 8 位数字的格式） | ✅ |
| `buy_price` | float | > 0 | ✅ |
| `sell_date` | str | 同 buy_date | ✅ |
| `sell_price` | float | > 0 | ✅ |
| `name` | str | 证券简称，没有就填 code | 可选 |
| `signal_date` | str | 信号日，只作展示 | 可选 |
| `rank` | int/float | 当日撮合顺序，rank 小的先走 | 可选 |

adapter 内部完成后**务必**调一次 `finalize_trade_table(df)`（来自
`adapters._contract`），它会补齐可选列、检查必填列、按标准列顺序重排，并把
额外列保留在后面。如果 bucket 列缺失它会报一个可读的错误。

## 3. 能不能只用 generic，别写新 adapter？

优先级：**能 generic 就 generic**。只要以下条件都满足，就用 `generic + CLI flags`：

- 列名变换是无歧义的 1:1 映射（即 `--col-map SRC=DST` 足够）。
- 行过滤能用 `df.query` 写清楚（即 `--filter "is_buy == True"` 足够）。
- bucket 来源：CSV 里已经有一列 / 可以按 `code` 推 / 全表一个桶。
- 不需要调项目内部的 helper 函数。

凡是出现以下情况，就写专用 adapter：

- 需要 fallback（例如 rank 优先 `day_rank`，没有就 `rank_score`，再没有就 `new_def_rank`）。
- 需要 derive 列（例如 `sell_date = buy_date + N 交易日`）。
- 需要调项目 helper（如 `02_core/board_classifier.infer_board`，哪怕 vendored 也最好封装起来）。
- CLI flags 过 4 个还不干净。
- 口径未来可能变（把口径封装在 adapter 里，不在每次调用都靠 CLI 记）。

## 4. 模板

复制 `930_00c.py` 做改动是最快的。骨架：

```python
"""<策略> adapter。"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

from adapters._contract import (
    apply_col_map, apply_query_filters,
    finalize_trade_table, infer_board, load_csv_smart,
)

NAME = "my_strategy"
DESCRIPTION = "<一行描述>"


def add_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--input", action="append", required=True,
                   help="CSV 路径，可重复；也支持 BUCKET=path。")
    # 按需加自己的 flag ——
    p.add_argument("--exit-horizon", default="8d")
    p.add_argument("--encoding", default="utf-8-sig")


def load(args: argparse.Namespace) -> pd.DataFrame:
    frames = []
    for spec in args.input:
        bucket_override, path = _split_bucket_spec(spec)
        df = load_csv_smart(path, encoding=args.encoding)
        # 1) 列名标准化
        df = apply_col_map(df, col_map={
            # 你策略独有的列映射
        })
        # 2) 行过滤
        df = apply_query_filters(df, [
            # "is_buy == True",
        ])
        # 3) 派生列（bucket / rank / name …）
        if bucket_override is not None:
            df["bucket"] = bucket_override
        elif "bucket" not in df.columns:
            df["bucket"] = df["code"].map(infer_board)  # 或其它来源
        frames.append(df)
    return finalize_trade_table(pd.concat(frames, ignore_index=True))


def _split_bucket_spec(spec: str) -> tuple[str | None, str]:
    if "=" in spec:
        bucket, path = spec.split("=", 1)
        return bucket.strip() or None, path.strip()
    return None, spec
```

放进 `scripts/adapters/`，`list-adapters` 就能看到它了。

## 5. 推荐的参数命名

为了让不同 adapter 用起来感觉接近，推荐遵循：

| Flag | 用途 |
| --- | --- |
| `--input` | CSV/Parquet 路径，可重复；`BUCKET=path` 形式强制指定 bucket。 |
| `--encoding` | CSV 编码。 |
| `--bucket-mode {board,flat}` | 没有 bucket 列时，按 code 推 / 全表一个桶。 |
| `--exit-horizon 8d` | 多窗口卖出数据时选哪一个窗口。 |

**不要**在 adapter 里注册 `--initial-cash / --per-stock-cap / --commission-bps /
--pool / --date-from / --date-to`。这些都是公共 flag，`run_backtest.py` 已经替你
注册好了。

## 6. 外部 adapter（不进 `adapters/` 目录）

想在项目其它地方保留 adapter 也行：

```bash
python scripts/run_backtest.py run \
    --adapter /path/to/my_strategy_adapter.py \
    ... adapter-specific flags ...
```

这时 adapter 不能用 `from adapters._contract import ...`（不在包内）。如果确实需要
_contract 里的工具，把 skill 的 `scripts/` 加到 `sys.path`，然后写成
`from adapters._contract import ...`。或者就复制粘贴 `_contract.py` 里的那几个函数。

## 7. 一致性自测

写完新 adapter 后至少跑一次：

1. `python scripts/run_backtest.py list-adapters` — 应该能看到新 adapter，description 不报错。
2. 给一个最小的输入样本，跑 `run --adapter {slug} --input ... --no-plots --no-xlsx`，
   看 `out-dir/summary.json` 的 `buckets / per_bucket` 是否符合预期。
3. 如果 vendored 了项目 helper（例如 `infer_board`），写一个 smoke test
   对着项目版本做逐行一致性校验（参考 `tests/smoke_930_00c.py`）。
