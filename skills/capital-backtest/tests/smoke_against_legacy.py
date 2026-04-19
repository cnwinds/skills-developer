"""回归 smoke 测试。

把 04_outputs/capital_20w_201908_202604/ 里 KCB+CYB 的 BUY 事件抽出来，
重建成通用 trade table，跑 from-trades，然后逐月对比新旧 monthly.csv，
任何超过 0.02 的差异都算失败。

跑法（在含 ``04_outputs/capital_20w_201908_202604/`` 的对照数据目录所在的项目根下）：

    python skills/capital-backtest/tests/smoke_against_legacy.py

若上层无该项目数据，脚本会 ``[SKIP]`` 并退出 0。

这是一个对齐用例，确保 engine 的回测口径和老脚本
qinglong_capital_20w_backtest 完全一致。任何引擎变更后都跑一遍。
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
SKILL_DIR = HERE.parents[1]
SCRIPT = SKILL_DIR / "scripts" / "run_backtest.py"


def _find_workspace_root(skill_dir: Path) -> Path | None:
    cur = skill_dir.parent
    for _ in range(16):
        if (cur / "04_outputs").is_dir():
            return cur
        if cur.parent == cur:
            return None
        cur = cur.parent
    return None


PACKAGE_DIR: Path | None = _find_workspace_root(SKILL_DIR)
LEGACY = (
    (PACKAGE_DIR / "04_outputs" / "capital_20w_201908_202604")
    if PACKAGE_DIR is not None
    else Path("/__no_legacy__")
)


def build_trade_table() -> pd.DataFrame:
    if not LEGACY.exists():
        # 不是 Qinglong 项目时直接跳过，而不是整个 smoke 崩掉
        print(f"[SKIP] 找不到对照目录 {LEGACY}；此 smoke 只在原 Qinglong 项目里生效")
        raise SystemExit(0)
    frames = []
    for bucket in ("KCB", "CYB"):
        log = pd.read_csv(LEGACY / f"{bucket}_trade_log.csv")
        buys = log[log["event"] == "BUY"].copy()
        frames.append(
            pd.DataFrame(
                {
                    "code": buys["code"].astype(str),
                    "name": buys.get("name", buys["code"]).astype(str),
                    "bucket": bucket,
                    "signal_date": buys["signal_date"].astype(str),
                    "buy_date": buys["buy_date"].astype(str),
                    "buy_price": pd.to_numeric(buys["buy_price"], errors="coerce"),
                    "sell_date": buys["sell_date"].astype(str),
                    "sell_price": pd.to_numeric(buys["sell_price"], errors="coerce"),
                    "rank": buys["trade_id"].astype(int),  # 保持同日顺序
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    if PACKAGE_DIR is None:
        print("[SKIP] 未找到含 04_outputs/ 的项目根；此 smoke 仅在完整 Qinglong/对照数据工作区内运行。")
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        stage = tmp / "trades.csv"
        out = tmp / "out"
        build_trade_table().to_csv(stage, index=False)

        cmd = [
            sys.executable, str(SCRIPT), "from-trades",
            "--input", str(stage),
            "--initial-cash", "200000", "--per-stock-cap", "150000",
            "--no-plots", "--no-xlsx",
            "--out-dir", str(out),
        ]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout); print(r.stderr, file=sys.stderr)
            return r.returncode

        failed = False
        for bucket in ("KCB", "CYB"):
            old = pd.read_csv(LEGACY / f"{bucket}_monthly.csv")
            new = pd.read_csv(out / f"{bucket}_monthly.csv")
            merged = old.merge(
                new[["month", "closed_trades", "realized_pnl", "cum_pnl", "cum_return_pct"]],
                on="month", suffixes=("_old", "_new"),
            )
            diff = merged[
                (merged["closed_trades_old"] != merged["closed_trades_new"])
                | ((merged["realized_pnl_old"] - merged["realized_pnl_new"]).abs() > 0.02)
                | ((merged["cum_pnl_old"] - merged["cum_pnl_new"]).abs() > 0.02)
            ]
            tag = "OK " if diff.empty and len(old) == len(new) else "FAIL"
            print(f"[{tag}] {bucket}: {len(old)} months, diff={len(diff)}")
            if not diff.empty or len(old) != len(new):
                failed = True
                print(diff)

        # 对照 summary.json
        old_sj = json.loads((LEGACY / "summary.json").read_text())
        new_sj = json.loads((out / "summary.json").read_text())
        for bucket in ("kcb", "cyb"):
            o = old_sj[bucket]
            n = new_sj["per_bucket"][bucket.upper()]["summary"]
            for k in ("final_cash", "total_realized_pnl", "total_return_pct",
                      "executed_buys", "skipped_buys", "closed_sells"):
                if o[k] != n[k]:
                    failed = True
                    print(f"[FAIL] {bucket.upper()}.{k}: old={o[k]} new={n[k]}")
            else:
                print(f"[OK ] {bucket.upper()} summary fields all match")

    print("ALL PASSED" if not failed else "SMOKE FAILED")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
