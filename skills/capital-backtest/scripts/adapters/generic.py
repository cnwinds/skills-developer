"""通用 adapter：读一张 CSV/Parquet，做列名映射 + 行过滤后产出标准 trade table。

这个 adapter 尽量"不做业务逻辑"，只在 CLI 上给足够多的拼装积木，覆盖 80% 的
新策略接入。剩下 20% 需要代码层干活的（板块推断、rank fallback 等）走专用
adapter（见 ``930_00c.py`` 作参考）。

必填列（重命名后）：``code, buy_date, buy_price, sell_date, sell_price``。
bucket 有三种来源（优先级从高到低）：
1. CSV 本身有 ``bucket`` 列（或别名 pool/board/sector）。
2. ``--bucket-from-code``：用 board_classifier 按代码前缀推。
3. ``--bucket-default NAME`` 或 ``--input BUCKET=path.csv``：整表打同一个桶。

向后兼容：``load_generic(path, bucket=...)`` 这个旧函数还保留着。
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from adapters._contract import (
    apply_col_map,
    apply_query_filters,
    finalize_trade_table,
    infer_board,
    load_csv_smart,
)


# ---------------------------------------------------------------------------
# 新契约：NAME / DESCRIPTION / add_arguments / load
# ---------------------------------------------------------------------------

NAME = "generic"
DESCRIPTION = "通用 CSV 输入：支持 --col-map/--filter/--bucket-from-code，够用不够再写专用 adapter"


def add_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--input", action="append", required=True,
        help="CSV / Parquet 路径。可重复。支持 BUCKET=path 形式（整表打同一个桶）。",
    )
    p.add_argument(
        "--col-map", action="append", default=None, metavar="SRC=DST",
        help="在内置别名之前做一次重命名，例如 --col-map sell_date=sell_date_8d。可重复。",
    )
    p.add_argument(
        "--filter", dest="filters", action="append", default=None, metavar="EXPR",
        help="DataFrame.query 表达式，例如 --filter \"is_buy == True\"。可重复，按顺序生效。",
    )
    p.add_argument(
        "--bucket-from-code", action="store_true",
        help="没有 bucket 列时，按 code 前缀推（KCB/CYB/BSE/SH_MAIN/SZ_MAIN/SZ_SME/OTHER）。",
    )
    p.add_argument(
        "--bucket-default", default=None, metavar="NAME",
        help="没有 bucket 列时，把所有行打同一个桶。和 --bucket-from-code 互斥（后者优先）。",
    )
    p.add_argument(
        "--encoding", default="utf-8-sig",
        help="CSV 编码，默认 utf-8-sig（兼容带 BOM 的 Excel 导出）。",
    )


def load(args: argparse.Namespace) -> pd.DataFrame:
    col_map = _parse_col_map(getattr(args, "col_map", None))
    frames: list[pd.DataFrame] = []
    for spec in args.input:
        bucket_override: str | None = None
        path = spec
        if "=" in spec:
            bucket_override, path = spec.split("=", 1)
            bucket_override = bucket_override.strip() or None
            path = path.strip()
        df = _load_one(path, encoding=args.encoding)
        df = apply_col_map(df, col_map=col_map)
        df = apply_query_filters(df, getattr(args, "filters", None))

        # bucket 派生
        if bucket_override is not None:
            df["bucket"] = bucket_override
        elif "bucket" not in df.columns:
            if args.bucket_from_code and "code" in df.columns:
                df["bucket"] = df["code"].map(infer_board)
            elif args.bucket_default:
                df["bucket"] = args.bucket_default
            else:
                raise SystemExit(
                    f"{path}: 没有 bucket 列。用 BUCKET=path、--bucket-default、"
                    f"或 --bucket-from-code 任选其一。"
                )
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    return finalize_trade_table(out)


def _parse_col_map(items: list[str] | None) -> dict[str, str] | None:
    if not items:
        return None
    out: dict[str, str] = {}
    for it in items:
        if "=" not in it:
            raise SystemExit(f"--col-map 格式应为 SRC=DST: {it!r}")
        src, dst = it.split("=", 1)
        out[src.strip()] = dst.strip()
    return out


def _load_one(path: str | Path, encoding: str) -> pd.DataFrame:
    return load_csv_smart(path, encoding=encoding)


# ---------------------------------------------------------------------------
# 向后兼容：老函数 load_generic(path, bucket=None)
# ---------------------------------------------------------------------------

def load_generic(path: str | Path, bucket: str | None = None) -> pd.DataFrame:
    """旧签名：读一张 CSV/Parquet，做内置别名映射。调用方自己补 bucket。

    新代码请走 ``NAME / add_arguments / load`` 契约；这个函数只为
    ``run_backtest.py`` 的 ``from-trades`` 兼容壳保留。
    """
    df = load_csv_smart(path)
    df = apply_col_map(df)
    if "bucket" not in df.columns:
        if bucket is None:
            raise ValueError(f"{path} 没有 bucket 列，且未通过 bucket 参数指定")
        df["bucket"] = bucket
    required = ["code", "bucket", "buy_date", "buy_price", "sell_date", "sell_price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"{path} 缺少必需列: {missing}")
    return df
