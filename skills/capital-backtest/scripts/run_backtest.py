"""capital-backtest 主入口。

子命令：

    run              通用入口，按 --adapter 加载策略适配器
    list-adapters    列出已注册的 adapter
    from-trades      兼容壳：相当于 run --adapter generic ...
    from-qinglong    兼容壳：相当于 run --adapter qinglong ...

公共 flag（所有入口都支持）：
    --initial-cash 200000 --per-stock-cap 150000
    --commission-bps 0 --stamp-duty-bps 0 --min-amount 0
    --pool POOL_SPEC       股票池过滤：inline 逗号分隔 / .json / .csv（见 _contract.load_pool）
    --date-from YYYYMMDD   默认 20140101
    --date-to   YYYYMMDD   默认今天
    --no-combine           关掉「组合账户」
    --no-plots             不画图
    --no-xlsx              不产 xlsx
    --label LABEL          输出目录名后缀
    --out-dir DIR          输出根目录
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from adapters import list_adapters, load_adapter, load_generic, load_qinglong  # noqa: E402
from adapters._contract import (  # noqa: E402
    apply_date_range,
    apply_pool_filter,
    finalize_trade_table,
    load_pool,
)
from engine import (  # noqa: E402
    AccountSummary,
    CostConfig,
    build_monthly,
    build_yearly,
    simulate_bucket,
    simulate_combined,
)
from metrics import compute_metrics  # noqa: E402


DEFAULT_DATE_FROM = "20140101"


# ---------------------------------------------------------------------------
# 公共 flag
# ---------------------------------------------------------------------------

def _add_common_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("--initial-cash", type=float, default=200_000)
    p.add_argument("--per-stock-cap", type=float, default=150_000)
    p.add_argument("--commission-bps", type=float, default=0.0)
    p.add_argument("--stamp-duty-bps", type=float, default=0.0)
    p.add_argument("--min-amount", type=float, default=0.0)
    p.add_argument("--risk-free-annual", type=float, default=0.0)
    p.add_argument(
        "--pool", default=None,
        help="股票池过滤：代码逗号列表 / .json / .csv / .txt。默认不过滤（用 adapter 自带池）。",
    )
    p.add_argument(
        "--date-from", default=DEFAULT_DATE_FROM,
        help=f"买入日下界，默认 {DEFAULT_DATE_FROM}。",
    )
    p.add_argument(
        "--date-to", default=None,
        help="买入日上界，默认 = 今天。",
    )
    p.add_argument("--no-combine", action="store_true", help="不算组合账户")
    p.add_argument("--no-plots", action="store_true")
    p.add_argument("--no-xlsx", action="store_true")
    p.add_argument("--label", default=None)
    p.add_argument("--out-dir", default=None)


# ---------------------------------------------------------------------------
# CLI 解析（两阶段：先拆出 adapter，再用 adapter 自己的 parser 解剩下的）
# ---------------------------------------------------------------------------

def parse_args_and_load_trades(argv: list[str]) -> tuple[argparse.Namespace, pd.DataFrame]:
    # 第一阶段：只认子命令和公共 flag
    root = argparse.ArgumentParser("capital-backtest")
    sub = root.add_subparsers(dest="cmd", required=True)

    # ── run ──
    run_p = sub.add_parser("run", help="通用入口（推荐）", add_help=True)
    run_p.add_argument("--adapter", required=True,
                       help="adapter slug（见 list-adapters）或 .py 文件路径")
    _add_common_flags(run_p)

    # ── from-trades（兼容壳）──
    t_p = sub.add_parser("from-trades", help="兼容壳：等价于 run --adapter generic")
    t_p.add_argument("--input", action="append", required=True,
                     help="CSV 路径，可重复。支持 BUCKET=path 形式。")
    t_p.add_argument("--col-map", action="append", default=None, metavar="SRC=DST")
    t_p.add_argument("--filter", dest="filters", action="append", default=None, metavar="EXPR")
    t_p.add_argument("--bucket-from-code", action="store_true")
    t_p.add_argument("--bucket-default", default=None)
    t_p.add_argument("--encoding", default="utf-8-sig")
    _add_common_flags(t_p)

    # ── from-qinglong（兼容壳）──
    q_p = sub.add_parser("from-qinglong", help="兼容壳：等价于 run --adapter qinglong")
    q_p.add_argument("--keep-csv", action="append", required=True)
    q_p.add_argument("--setting-version", default=None)
    q_p.add_argument("--package-dir", default=None)
    _add_common_flags(q_p)

    # ── list-adapters ──
    sub.add_parser("list-adapters", help="列出所有已注册 adapter")

    # 如果是 run，要先解出 --adapter 再交给 adapter 自己解其余参数。
    # 策略：把已知的 run 主 flag 用 parse_known_args 解出来，其余透传给 adapter。
    if argv and argv[0] == "run":
        args, rest = root.parse_known_args(argv)
        adapter = load_adapter(args.adapter)
        adapter_p = argparse.ArgumentParser(f"capital-backtest --adapter={adapter.NAME}")
        adapter.add_arguments(adapter_p)
        adapter_args = adapter_p.parse_args(rest)
        trades = adapter.load(adapter_args)
        return args, trades

    args = root.parse_args(argv)

    if args.cmd == "list-adapters":
        _print_adapters()
        raise SystemExit(0)

    if args.cmd == "from-trades":
        trades = _compat_from_trades(args)
        return args, trades

    if args.cmd == "from-qinglong":
        trades = _compat_from_qinglong(args)
        return args, trades

    raise SystemExit(f"未知子命令: {args.cmd}")


def _compat_from_trades(args: argparse.Namespace) -> pd.DataFrame:
    """把 from-trades 的旧参数转译给 generic adapter。"""
    from adapters import generic as gen
    fake = argparse.Namespace(
        input=args.input,
        col_map=args.col_map,
        filters=args.filters,
        bucket_from_code=args.bucket_from_code,
        bucket_default=args.bucket_default,
        encoding=args.encoding,
    )
    return gen.load(fake)


def _compat_from_qinglong(args: argparse.Namespace) -> pd.DataFrame:
    """把 from-qinglong 的旧参数转译给 qinglong adapter。"""
    from adapters import qinglong as ql
    fake = argparse.Namespace(
        keep_csv=args.keep_csv,
        setting_version=args.setting_version,
        package_dir=args.package_dir,
    )
    return ql.load(fake)


def _print_adapters() -> None:
    print("已注册 adapter：")
    for info in list_adapters():
        print(f"  - {info['name']:12s} {info['description']}")


# ---------------------------------------------------------------------------
# 公共过滤层：adapter 产出之后、engine 之前
# ---------------------------------------------------------------------------

def _apply_common_filters(trades: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    # 股票池
    pool = load_pool(args.pool)
    trades = apply_pool_filter(trades, pool)
    # 时间段（按 buy_date 下限 / 上限）
    date_to = args.date_to or _today_str()
    trades = apply_date_range(trades, args.date_from, date_to, column="buy_date")
    return trades.reset_index(drop=True)


def _today_str() -> str:
    return date.today().strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    args, trades = parse_args_and_load_trades(argv)

    # finalize_trade_table 是 idempotent 的；adapter 内已经调过，这里兜底
    trades = finalize_trade_table(trades)
    trades = _apply_common_filters(trades, args)
    if trades.empty:
        raise SystemExit(
            "过滤后 trade table 为空。检查 --pool / --date-from / --date-to / adapter 过滤条件。"
        )

    cost = CostConfig(
        commission_bps=args.commission_bps,
        stamp_duty_bps=args.stamp_duty_bps,
        min_amount=args.min_amount,
    )

    buckets = sorted(trades["bucket"].dropna().unique().tolist())
    label = args.label or "_".join(buckets)
    spans = trades["buy_date"].astype(str).str[:6]
    label_full = f"{label}_{spans.min()}_{spans.max()}"

    out_dir = Path(args.out_dir) if args.out_dir else Path.cwd() / f"capital_backtest_{label_full}"
    out_dir.mkdir(parents=True, exist_ok=True)

    per_bucket: dict[str, dict] = {}
    for bucket in buckets:
        sub_df = trades[trades["bucket"] == bucket].copy()
        log_df, eq_df, summary = simulate_bucket(
            sub_df, bucket, args.initial_cash, args.per_stock_cap, cost
        )
        monthly = build_monthly(log_df, args.initial_cash)
        yearly = build_yearly(monthly, args.initial_cash)
        metrics = compute_metrics(monthly, log_df, args.initial_cash, args.risk_free_annual)
        per_bucket[bucket] = {
            "log": log_df, "equity": eq_df, "monthly": monthly, "yearly": yearly,
            "summary": summary, "metrics": metrics,
        }
        log_df.to_csv(out_dir / f"{bucket}_trade_log.csv", index=False, encoding="utf-8-sig")
        eq_df.to_csv(out_dir / f"{bucket}_equity.csv", index=False, encoding="utf-8-sig")
        monthly.to_csv(out_dir / f"{bucket}_monthly.csv", index=False, encoding="utf-8-sig")
        yearly.to_csv(out_dir / f"{bucket}_yearly.csv", index=False, encoding="utf-8-sig")

    combined: dict | None = None
    if not args.no_combine and len(buckets) >= 2:
        log_df, eq_df, summary = simulate_combined(
            trades, args.initial_cash, args.per_stock_cap, cost
        )
        monthly = build_monthly(log_df, args.initial_cash)
        yearly = build_yearly(monthly, args.initial_cash)
        metrics = compute_metrics(monthly, log_df, args.initial_cash, args.risk_free_annual)
        combined = {
            "log": log_df, "equity": eq_df, "monthly": monthly, "yearly": yearly,
            "summary": summary, "metrics": metrics,
        }
        log_df.to_csv(out_dir / "ALL_trade_log.csv", index=False, encoding="utf-8-sig")
        eq_df.to_csv(out_dir / "ALL_equity.csv", index=False, encoding="utf-8-sig")
        monthly.to_csv(out_dir / "ALL_monthly.csv", index=False, encoding="utf-8-sig")
        yearly.to_csv(out_dir / "ALL_yearly.csv", index=False, encoding="utf-8-sig")

    if not args.no_plots:
        try:
            from plots import plot_equity_curve, plot_monthly_heatmap
            for bucket, data in per_bucket.items():
                plot_equity_curve(data["monthly"], args.initial_cash,
                                  out_dir / f"{bucket}_equity.png", title=f"{bucket} equity")
                plot_monthly_heatmap(data["monthly"], out_dir / f"{bucket}_heatmap.png",
                                     title=f"{bucket} monthly realized PnL")
            if combined:
                plot_equity_curve(combined["monthly"], args.initial_cash,
                                  out_dir / "ALL_equity.png", title="Combined equity")
                plot_monthly_heatmap(combined["monthly"], out_dir / "ALL_heatmap.png",
                                     title="Combined monthly realized PnL")
        except ImportError as e:
            print(f"[warn] 跳过画图：{e}", file=sys.stderr)

    summary_rows = pd.DataFrame([_summary_row(b, d["summary"]) for b, d in per_bucket.items()])
    metrics_rows = pd.DataFrame([_metrics_row(b, d["metrics"], d["summary"]) for b, d in per_bucket.items()])
    if combined:
        summary_rows = pd.concat(
            [summary_rows, pd.DataFrame([_summary_row("ALL", combined["summary"])])],
            ignore_index=True,
        )
        metrics_rows = pd.concat(
            [metrics_rows, pd.DataFrame([_metrics_row("ALL", combined["metrics"], combined["summary"])])],
            ignore_index=True,
        )

    summary_rows.to_csv(out_dir / "summary.csv", index=False, encoding="utf-8-sig")
    metrics_rows.to_csv(out_dir / "metrics.csv", index=False, encoding="utf-8-sig")

    summary_json = {
        "label": label_full,
        "buckets": buckets,
        "adapter": getattr(args, "adapter", _compat_adapter_name(args)),
        "initial_cash": args.initial_cash,
        "per_stock_cap": args.per_stock_cap,
        "cost": {
            "commission_bps": args.commission_bps,
            "stamp_duty_bps": args.stamp_duty_bps,
            "min_amount": args.min_amount,
        },
        "pool": args.pool,
        "date_from": args.date_from,
        "date_to": args.date_to or _today_str(),
        "per_bucket": {
            b: {"summary": d["summary"].to_dict(), "metrics": d["metrics"]} for b, d in per_bucket.items()
        },
        "combined": (
            {"summary": combined["summary"].to_dict(), "metrics": combined["metrics"]} if combined else None
        ),
        "output_dir": str(out_dir),
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary_json, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    (out_dir / "README.md").write_text(
        _render_readme(args, summary_json, per_bucket, combined), encoding="utf-8",
    )

    if not args.no_xlsx:
        try:
            from render_xlsx import write_report
            monthlies = {b: d["monthly"] for b, d in per_bucket.items()}
            yearlies = {b: d["yearly"] for b, d in per_bucket.items()}
            trade_logs = {b: d["log"] for b, d in per_bucket.items()}
            if combined:
                monthlies["ALL"] = combined["monthly"]
                yearlies["ALL"] = combined["yearly"]
                trade_logs["ALL"] = combined["log"]
            write_report(
                out_dir / f"report_{label_full}.xlsx",
                summary_rows, metrics_rows,
                monthlies, yearlies, trade_logs,
            )
        except ImportError as e:
            print(f"[warn] 跳过 xlsx：{e}", file=sys.stderr)

    print(f"[done] 输出: {out_dir}")
    return 0


def _compat_adapter_name(args: argparse.Namespace) -> str:
    return {"from-trades": "generic", "from-qinglong": "qinglong"}.get(args.cmd, args.cmd)


def _summary_row(bucket: str, s: AccountSummary) -> dict:
    d = s.to_dict()
    d["bucket"] = bucket
    return d


def _metrics_row(bucket: str, m: dict, s: AccountSummary) -> dict:
    row = {"bucket": bucket}
    row.update({"realized_pnl": s.total_realized_pnl, "return_pct": s.total_return_pct})
    row.update(m)
    return row


def _render_readme(args, sj: dict, per_bucket: dict, combined: dict | None) -> str:
    lines = [
        f"# capital-backtest 报告 ({sj['label']})",
        "",
        f"- adapter: **{sj['adapter']}**",
        f"- 时间窗：`{sj['date_from']}` ~ `{sj['date_to']}`"
        + (f"（股票池：{sj['pool']}）" if sj['pool'] else "（股票池：策略自带）"),
        "",
        "## 规则摘要",
        "",
        f"- 初始资金：**{args.initial_cash:,.0f} 元/账户**",
        f"- 单笔上限：**{args.per_stock_cap:,.0f} 元**；同一买入日多笔均分当日可用现金",
        f"- 佣金：双边 {args.commission_bps} bps；印花税：卖出 {args.stamp_duty_bps} bps；最小成交金额：{args.min_amount:.0f}",
        f"- 卖出口径：sell_date 当日按 sell_price 一次性平仓",
        "",
        "## 各 bucket 汇总",
        "",
        "| bucket | 期末现金 | 累计盈亏 | 收益% | 成交 | 跳过 | 平仓 | 最大回撤% | Sharpe | 月胜率% |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    rows = list(per_bucket.items())
    if combined:
        rows = rows + [("ALL", combined)]
    for b, d in rows:
        s = d["summary"]; m = d["metrics"]
        lines.append(
            f"| {b} | {s.final_cash:,.2f} | {s.total_realized_pnl:,.2f} | {s.total_return_pct:.4f} | "
            f"{s.executed_buys} | {s.skipped_buys} | {s.closed_sells} | "
            f"{m.get('max_drawdown_pct', '')} | {m.get('sharpe_monthly', '')} | {m.get('monthly_win_rate_pct', '')} |"
        )
    lines += [
        "",
        "## 输出文件",
        "",
        "- `{bucket}_trade_log.csv` 逐笔事件（含 BUY/SELL，跳过单也在）",
        "- `{bucket}_equity.csv` 事件日权益曲线（cash + open_cost）",
        "- `{bucket}_monthly.csv` 卖出月汇总（含月度胜率与回撤）",
        "- `{bucket}_yearly.csv` 年度汇总",
        "- `summary.csv / summary.json / metrics.csv` 跨 bucket 汇总",
        "- `{bucket}_equity.png / {bucket}_heatmap.png` 可视化（如启用）",
        "- `report_{label}.xlsx` 多 sheet 报表（如启用）",
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
