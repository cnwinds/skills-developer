"""930-00c 项目专用 adapter。

目标输入：``04_outputs/930_00c_buy_signals_{YYYYMM}_en.csv``。

口径要点：

- 关键列用中文：``买入日 / 买入价`` → 重命名成 ``buy_date / buy_price``。
- 卖出端多窗口并存（``sell_date_8d``、``sell_date_25d`` …），由 ``--exit-horizon``
  决定用哪一个，默认 ``8d``。
- 只保留 ``is_buy == True`` 且 ``sell_date`` 非空的行。
- bucket 默认按 code 前缀推（``infer_board``）；``--bucket-mode flat`` 则所有行一个桶。
- rank 的 fallback：``day_rank`` → ``rank_score`` → ``new_def_rank`` → 999。
- 可选 ``--tier-filter S1,S2``：只保留 ``new_def_tier`` 命中的行（和
  ``02_core/report_930_00c_board_backtest.py`` 的 ALL_S 口径一致）。

该 adapter vendor 了一份 ``infer_board``（见 ``_contract.py``），smoke
``tests/smoke_930_00c.py`` 会对着项目内版本做一致性校验。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from adapters._contract import (
    apply_col_map,
    apply_query_filters,
    finalize_trade_table,
    infer_board,
    load_csv_smart,
)


NAME = "930_00c"
DESCRIPTION = (
    "930-00c buy_signals_*_en.csv（中文列 + sell_*_Nd + is_buy + 板块推断）"
)


def add_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--input", action="append", required=True,
        help="930_00c_buy_signals_*_en.csv 路径。可重复；也支持 BUCKET=path 强制指定 bucket（会跳过板块推断）。",
    )
    p.add_argument(
        "--exit-horizon", default="8d", metavar="Nd",
        help="卖出窗口后缀，默认 8d。会把 sell_date_{Nd}/sell_price_{Nd} 映射成 sell_date/sell_price。",
    )
    p.add_argument(
        "--bucket-mode", choices=["board", "flat"], default="board",
        help="board=按 code 前缀推 KCB/CYB/…；flat=所有行一个桶（桶名由 --flat-bucket-name 决定）。",
    )
    p.add_argument(
        "--flat-bucket-name", default="ALL",
        help="当 --bucket-mode flat 时使用的桶名，默认 ALL。",
    )
    p.add_argument(
        "--tier-filter", default=None, metavar="S1,S2",
        help="可选：只保留 new_def_tier 在这个列表里的行（逗号分隔）。",
    )
    p.add_argument(
        "--keep-a-signals", action="store_true",
        help="默认：A 类只作信号不买入，会被过滤掉。加这个 flag 则把 A 也保留。",
    )
    p.add_argument(
        "--encoding", default="utf-8-sig",
        help="CSV 编码，默认 utf-8-sig。",
    )


def load(args: argparse.Namespace) -> pd.DataFrame:
    horizon = _normalize_horizon(args.exit_horizon)
    tier_set = _parse_tier_filter(args.tier_filter)

    col_map = {
        f"sell_date_{horizon}": "sell_date",
        f"sell_price_{horizon}": "sell_price",
    }

    frames: list[pd.DataFrame] = []
    for spec in args.input:
        bucket_override: str | None = None
        path = spec
        if "=" in spec:
            bucket_override, path = spec.split("=", 1)
            bucket_override = bucket_override.strip() or None
            path = path.strip()

        df = load_csv_smart(path, encoding=args.encoding)
        df = apply_col_map(df, col_map=col_map)  # 中文列别名在 _contract.COMMON_ALIASES 里

        df = _filter_rows(df, tier_set, keep_a=args.keep_a_signals)
        df = _add_bucket(df, bucket_override, args.bucket_mode, args.flat_bucket_name)
        df = _add_rank(df)
        df = _coerce_types(df)
        frames.append(df)

    if not frames:
        raise SystemExit("没有解析出任何输入文件")

    out = pd.concat(frames, ignore_index=True)
    return finalize_trade_table(out)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _normalize_horizon(raw: str) -> str:
    """接受 ``8``、``"8d"``、``"8D"``；统一成小写 ``"8d"``。"""
    text = str(raw).strip().lower()
    if text.endswith("d"):
        text = text[:-1]
    if not text.isdigit():
        raise SystemExit(f"--exit-horizon 应形如 8 或 8d，收到: {raw!r}")
    return f"{int(text)}d"


def _parse_tier_filter(raw: str | None) -> set[str] | None:
    if not raw:
        return None
    tiers = {t.strip() for t in raw.split(",") if t.strip()}
    return tiers or None


def _filter_rows(df: pd.DataFrame, tier_set: Iterable[str] | None,
                 keep_a: bool) -> pd.DataFrame:
    """核心过滤：is_buy + sell_date 非空 + 可选 tier。"""
    out = df
    if "is_buy" in out.columns:
        out = out[_to_bool(out["is_buy"])]
    if "sell_date" in out.columns:
        out = out[out["sell_date"].astype(str).str.strip().ne("")]
        out = out[out["sell_date"].notna()]
    if "sell_price" in out.columns:
        out = out[pd.to_numeric(out["sell_price"], errors="coerce") > 0]

    if tier_set and "new_def_tier" in out.columns:
        out = out[out["new_def_tier"].astype(str).isin(tier_set)]

    if not keep_a:
        # 默认把 A 类（signal-only）剔掉：abc_label == 'ABC-A' 且未进 S1/S2 的行
        if "abc_label" in out.columns and "new_def_tier" in out.columns:
            is_a_only = (out["abc_label"].astype(str) == "ABC-A") & \
                        (~out["new_def_tier"].astype(str).isin({"S1", "S2"}))
            out = out[~is_a_only]

    return out.reset_index(drop=True)


def _to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y", "t"})


def _add_bucket(df: pd.DataFrame, override: str | None, mode: str,
                flat_name: str) -> pd.DataFrame:
    out = df.copy()
    if override is not None:
        out["bucket"] = override
        return out
    if mode == "flat":
        out["bucket"] = flat_name
        return out
    if "code" not in out.columns:
        raise SystemExit("930_00c adapter: 找不到 code 列，无法推断 bucket")
    out["bucket"] = out["code"].map(infer_board)
    return out


def _add_rank(df: pd.DataFrame) -> pd.DataFrame:
    """优先级：day_rank → rank_score → new_def_rank → 999。"""
    out = df.copy()
    rank = pd.Series(999, index=out.index, dtype="float64")
    for col in ("day_rank", "rank_score", "new_def_rank"):
        if col in out.columns:
            candidate = pd.to_numeric(out[col], errors="coerce")
            rank = rank.where(rank.ne(999), candidate.fillna(999))
            # day_rank 是最优先的，一旦拿到就 break
            if col == "day_rank":
                break
    out["rank"] = rank.astype("float64")
    return out


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ("buy_price", "sell_price"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in ("buy_date", "sell_date", "signal_date"):
        if c in out.columns:
            out[c] = out[c].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    if "code" in out.columns:
        out["code"] = out["code"].astype(str).str.strip()
    if "name" not in out.columns and "code" in out.columns:
        out["name"] = out["code"]
    return out
