"""Qinglong adapter：把 930-QINGLONG-R6090_{pool}_{start}_{end}_v3.csv 转成通用 trade table。

逻辑和 02_core/qinglong_capital_20w_backtest.py 的 dedupe_for_live + trades_for_sim
保持一致（去重键、setting_is_buy 过滤、has_full_25d 过滤），只是把
bucket 字段固定成 pool 的名字（KCB / CYB / ...）。

依赖原 Qinglong 项目内 02_core/run_qinglong_predict_zi.py 的 enrich_live_sheet；
在没有该文件的项目里，此 adapter 会在 ``load`` 时抛出清晰的错误（而非 import 时崩）。
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd


NAME = "qinglong"
DESCRIPTION = "Qinglong 项目的 keep CSV（setting_is_buy + has_full_25d）；需要项目内 02_core/run_qinglong_predict_zi.py"


def add_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--keep-csv", action="append", required=True,
        help="BUCKET=keep_csv_path 形式，例如 KCB=04_outputs/930-QINGLONG-R6090_KCB_201908_202604_v3.csv。可重复。",
    )
    p.add_argument(
        "--setting-version", default=None,
        help="默认按 bucket 推断：KCB->v22, 其它->v1。也可 KCB=v22,CYB=v1。",
    )
    p.add_argument(
        "--package-dir", default=None,
        help="Qinglong 项目根目录（含 02_core/）。默认从 --keep-csv 向上一级推断。",
    )


def load(args: argparse.Namespace) -> pd.DataFrame:
    kc = _parse_kv(args.keep_csv)
    version_map = _parse_setting_versions(args.setting_version)
    frames = []
    for bucket, path in kc.items():
        sv = version_map.get(bucket) or _default_setting_version(bucket)
        frames.append(load_qinglong(path, bucket=bucket, setting_version=sv, package_dir=args.package_dir))
    return pd.concat(frames, ignore_index=True)


def _parse_kv(items: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for it in items:
        if "=" not in it:
            raise SystemExit(f"--keep-csv 应为 BUCKET=path: {it!r}")
        k, v = it.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _parse_setting_versions(spec: str | None) -> dict[str, str]:
    if not spec:
        return {}
    if "=" not in spec:
        return {"_default": spec}
    return _parse_kv([s for s in spec.split(",") if s])


def _default_setting_version(bucket: str) -> str:
    return "v22" if bucket.upper() == "KCB" else "v1"


def _ensure_core_on_path(package_dir: Path) -> None:
    core = package_dir / "02_core"
    if not core.exists():
        raise FileNotFoundError(
            f"找不到 02_core 目录: {core}。qinglong adapter 需要原 Qinglong 项目结构；"
            f"如果这是其它项目，请改用 --adapter generic 或写专用 adapter。"
        )
    if str(core) not in sys.path:
        sys.path.insert(0, str(core))


def _dedupe_for_live(df: pd.DataFrame) -> pd.DataFrame:
    if "cold_no10_days" not in df.columns:
        df = df.copy()
        df["cold_no10_days"] = 0
    df["cold_no10_days"] = pd.to_numeric(df["cold_no10_days"], errors="coerce").fillna(0)
    return (
        df.sort_values(
            ["code", "signal_date", "cold_no10_days", "tier_rank", "candidate_tier_l2_l4", "r_ratio", "signal_pct"],
            ascending=[True, True, False, False, False, False, False],
        )
        .drop_duplicates(["code", "signal_date"])
        .reset_index(drop=True)
    )


def load_qinglong(
    keep_csv: str | Path,
    bucket: str,
    setting_version: str = "v1",
    package_dir: str | Path | None = None,
) -> pd.DataFrame:
    """读 keep CSV，enrich，再抽出 trade-ready 的通用 trade table。

    Args:
        keep_csv: 930-QINGLONG-R6090_*.csv 路径
        bucket: 标记用的桶名，通常就是 KCB / CYB
        setting_version: enrich_live_sheet 的 setting_version，默认 v1
        package_dir: 95-00c 项目根目录；默认从 keep_csv 向上推断
    """
    keep_csv = Path(keep_csv)
    if package_dir is None:
        # 默认 04_outputs/xxx.csv 在 package 内
        package_dir = keep_csv.resolve().parents[1]
    package_dir = Path(package_dir)

    _ensure_core_on_path(package_dir)
    os.environ.setdefault("STOCK_ROOT", str(package_dir))

    try:
        from run_qinglong_predict_zi import enrich_live_sheet  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            f"qinglong adapter 无法找到 run_qinglong_predict_zi.enrich_live_sheet：{exc}。"
            f" 这个 adapter 仅用于原 Qinglong 项目；在其它项目里请改用 --adapter generic 或写专用 adapter。"
        ) from exc

    raw = pd.read_csv(keep_csv)
    pool_arg = bucket if bucket in {"KCB"} else None  # CYB 在原脚本里传 None
    enriched = enrich_live_sheet(_dedupe_for_live(raw), setting_version=setting_version, pool=pool_arg)

    mask = (enriched["setting_is_buy"] == True) & enriched["has_full_25d"].fillna(False)  # noqa: E712
    df = enriched.loc[mask].copy()
    need = [
        "code", "name", "signal_date",
        "buy_date_25d", "buy_price_25d",
        "sell_date_25d", "sell_price_25d",
        "setting_rank",
    ]
    for c in need:
        if c not in df.columns:
            raise KeyError(f"enrich_live_sheet 输出缺少列 {c}")

    out = pd.DataFrame(
        {
            "code": df["code"].astype(str),
            "name": df["name"].astype(str),
            "bucket": bucket,
            "signal_date": df["signal_date"].astype(str),
            "buy_date": df["buy_date_25d"].astype(str),
            "buy_price": pd.to_numeric(df["buy_price_25d"], errors="coerce"),
            "sell_date": df["sell_date_25d"].astype(str),
            "sell_price": pd.to_numeric(df["sell_price_25d"], errors="coerce"),
            "rank": pd.to_numeric(df["setting_rank"], errors="coerce").fillna(999).astype(int),
        }
    )
    return out.dropna(subset=["buy_price", "sell_price"]).reset_index(drop=True)
