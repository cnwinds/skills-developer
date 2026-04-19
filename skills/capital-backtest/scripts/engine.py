"""事件驱动的资金回测引擎（通用版）。

输入是一张「通用 trade table」DataFrame，列包括：
- code (str)                必须
- name (str)                可选，没有则用 code 补齐
- bucket (str)              必须，标记板块/子组合，例如 KCB / CYB / ALL
- signal_date (YYYYMMDD)    可选，没有就置空
- buy_date (YYYYMMDD)       必须
- buy_price (float)         必须 (>0)
- sell_date (YYYYMMDD)      必须
- sell_price (float)        必须 (>0)
- rank (int)                可选，当日撮合顺序；缺省按 code 字典序

simulate_bucket 返回单一账户的回测结果；simulate_combined 把所有 bucket 合成一个大账户跑。

规则（对齐老脚本 qinglong_capital_20w_backtest 的行为，另加可选成本）：

- 同一买入日的多笔：把当日可用现金均分，单笔再按 per_stock_cap 截断。
- 手数 = floor(alloc / (buy_price * (1 + commission_rate)) / LOT) * LOT。
- 佣金/印花税：commission_bps 双向收，stamp_duty_bps 只在卖出收。万 = bps/10。
- 最小成交约束：如果分配到的金额（alloc）小于 min_amount，或计算出的 shares < LOT，或
  cost + buy_commission > cash，均跳过，并记录 skip_reason。
- 同一事件日：先卖后买（回款先进现金，再参与分配）。
- 持仓到期一次性按 sell_price 平仓，不做中途止盈/止损（策略层已在 buy/sell_date 里表达）。
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional

import pandas as pd

LOT = 100


@dataclass(frozen=True)
class CostConfig:
    """交易成本配置。bps = 万分之几 / 10，也就是万分之 N 填 N*10。

    更直观：千分之 1 = 10 bps，万分之 2.5 = 2.5 bps。
    默认全部为 0，和老回测口径一致，不破坏历史数据可复现。
    """

    commission_bps: float = 0.0  # 双边，按成交金额
    stamp_duty_bps: float = 0.0  # 仅卖出，按成交金额
    min_amount: float = 0.0      # 单笔最小分配金额，低于此值视为跳过


@dataclass
class AccountSummary:
    bucket: str
    initial_cash: float
    final_cash: float
    total_realized_pnl: float
    total_return_pct: float
    executed_buys: int
    skipped_buys: int
    closed_sells: int

    def to_dict(self) -> dict:
        return asdict(self)


def _normalize(trades: pd.DataFrame) -> pd.DataFrame:
    """统一列名 / 类型 / 排序。"""
    df = trades.copy()
    required = ["code", "bucket", "buy_date", "buy_price", "sell_date", "sell_price"]
    for c in required:
        if c not in df.columns:
            raise KeyError(f"trade table 缺少必需列: {c}")

    if "name" not in df.columns:
        df["name"] = df["code"]
    if "signal_date" not in df.columns:
        df["signal_date"] = ""
    if "rank" not in df.columns:
        df["rank"] = 999

    # 数值化
    for c in ["buy_price", "sell_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce").fillna(999)

    # 日期统一成 8 位字符串
    for c in ["buy_date", "sell_date", "signal_date"]:
        df[c] = df[c].astype(str).str.replace(r"\D", "", regex=True).str[:8]

    df = df.dropna(subset=["buy_price", "sell_price"])
    df = df[(df["buy_price"] > 0) & (df["sell_price"] > 0)].copy()
    df = df.sort_values(["buy_date", "rank", "code"]).reset_index(drop=True)
    return df


def simulate_bucket(
    trades: pd.DataFrame,
    bucket: str,
    initial_cash: float,
    per_stock_cap: float,
    cost: Optional[CostConfig] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, AccountSummary]:
    """单一 bucket 账户回测。

    返回:
        trade_log: 逐笔事件流水（含 BUY/SELL/跳过），包含 cash_after / equity_after
        equity_curve: 按事件日的权益曲线（cash + 持仓成本）
        summary: 汇总统计
    """
    cost = cost or CostConfig()
    comm_rate = cost.commission_bps / 10_000.0
    stamp_rate = cost.stamp_duty_bps / 10_000.0
    df = _normalize(trades)

    cash = float(initial_cash)
    open_pos: list[dict] = []
    logs: list[dict] = []
    trade_id = 0

    by_buy = {d: g for d, g in df.groupby("buy_date", sort=True)}
    event_days = sorted(set(df["sell_date"].unique()) | set(df["buy_date"].unique()))
    equity_rows: list[dict] = []

    def open_cost_sum() -> float:
        return sum(p["cost"] for p in open_pos)

    def push_equity(day: str) -> None:
        equity_rows.append(
            {
                "date": day,
                "cash": round(cash, 2),
                "open_cost": round(open_cost_sum(), 2),
                "equity": round(cash + open_cost_sum(), 2),
            }
        )

    for day in event_days:
        # —— 先卖 ——
        to_close = [p for p in open_pos if p["sell_date"] == day]
        open_pos = [p for p in open_pos if p["sell_date"] != day]
        for p in to_close:
            gross = p["shares"] * p["sell_price"]
            fee = gross * (comm_rate + stamp_rate)
            proceeds = gross - fee
            cash += proceeds
            pnl = proceeds - p["cost"]  # cost 已含买入佣金
            logs.append(
                {
                    "event": "SELL",
                    "trade_id": p["trade_id"],
                    "date": day,
                    "bucket": bucket,
                    "code": p["code"],
                    "name": p["name"],
                    "signal_date": p["signal_date"],
                    "buy_date": p["buy_date"],
                    "sell_date": day,
                    "buy_price": p["buy_price"],
                    "sell_price": p["sell_price"],
                    "shares": p["shares"],
                    "alloc_cash": p["alloc_cash"],
                    "cost": p["cost"],
                    "proceeds": round(proceeds, 2),
                    "fee": round(fee, 2),
                    "pnl": round(pnl, 2),
                    "pnl_pct_on_cost": round((proceeds / p["cost"] - 1.0) * 100, 4) if p["cost"] > 0 else None,
                    "cash_after": round(cash, 2),
                    "equity_after": round(cash + open_cost_sum(), 2),
                    "skipped": False,
                    "skip_reason": "",
                }
            )

        # —— 再买 ——
        if day in by_buy:
            day_df = by_buy[day].sort_values(["rank", "code"])
            k_left = len(day_df)
            for _, row in day_df.iterrows():
                trade_id += 1
                per_cap = min(cash / k_left, per_stock_cap) if k_left > 0 else 0.0
                bp = float(row["buy_price"])
                sp = float(row["sell_price"])
                alloc = per_cap

                def buy_log(skipped: bool, reason: str, shares: int = 0, cost_amt: float = 0.0, fee: float = 0.0) -> dict:
                    return {
                        "event": "BUY",
                        "trade_id": trade_id,
                        "date": day,
                        "bucket": bucket,
                        "code": row["code"],
                        "name": row["name"],
                        "signal_date": str(row["signal_date"]),
                        "buy_date": day,
                        "sell_date": str(row["sell_date"]),
                        "buy_price": bp,
                        "sell_price": sp,
                        "shares": shares,
                        "alloc_cash": round(alloc, 2),
                        "cost": round(cost_amt, 2),
                        "proceeds": None,
                        "fee": round(fee, 2),
                        "pnl": None,
                        "pnl_pct_on_cost": None,
                        "cash_after": round(cash, 2),
                        "equity_after": round(cash + open_cost_sum(), 2),
                        "skipped": skipped,
                        "skip_reason": reason,
                    }

                if alloc < cost.min_amount:
                    logs.append(buy_log(True, "alloc_below_min_amount"))
                    k_left -= 1
                    continue

                raw_sh = int(alloc / (bp * (1.0 + comm_rate)) / LOT) * LOT
                if raw_sh < LOT:
                    logs.append(buy_log(True, "insufficient_cash_for_1_lot"))
                    k_left -= 1
                    continue

                gross = raw_sh * bp
                buy_fee = gross * comm_rate
                total_cost = gross + buy_fee
                if total_cost > cash + 1e-6:
                    logs.append(buy_log(True, "cash_lt_cost"))
                    k_left -= 1
                    continue

                cash -= total_cost
                open_pos.append(
                    {
                        "trade_id": trade_id,
                        "code": str(row["code"]),
                        "name": row["name"],
                        "signal_date": str(row["signal_date"]),
                        "buy_date": day,
                        "sell_date": str(row["sell_date"]),
                        "buy_price": bp,
                        "sell_price": sp,
                        "shares": raw_sh,
                        "alloc_cash": round(alloc, 2),
                        "cost": round(total_cost, 2),  # 含买入佣金
                    }
                )
                logs.append(buy_log(False, "", raw_sh, total_cost, buy_fee))
                k_left -= 1

        push_equity(day)

    if open_pos:
        raise RuntimeError(f"[{bucket}] 仍有未平仓 {len(open_pos)} 笔 — 检查 sell_date 是否覆盖到")

    log_df = pd.DataFrame(logs)
    eq_df = pd.DataFrame(equity_rows)

    sells = log_df[log_df["event"] == "SELL"]
    total_pnl = float(sells["pnl"].sum()) if not sells.empty else 0.0
    summary = AccountSummary(
        bucket=bucket,
        initial_cash=float(initial_cash),
        final_cash=round(cash, 2),
        total_realized_pnl=round(total_pnl, 2),
        total_return_pct=round(total_pnl / initial_cash * 100, 4),
        executed_buys=int(log_df[(log_df["event"] == "BUY") & (~log_df["skipped"])].shape[0]),
        skipped_buys=int(log_df[(log_df["event"] == "BUY") & (log_df["skipped"])].shape[0]),
        closed_sells=int(len(sells)),
    )
    return log_df, eq_df, summary


def build_monthly(log_df: pd.DataFrame, initial_cash: float) -> pd.DataFrame:
    """按卖出月汇总。列：year_month, month, closed_trades, wins, losses,
    win_rate_pct, realized_pnl, cum_pnl, cum_return_pct, drawdown_pct"""
    sells = log_df[log_df["event"] == "SELL"].copy()
    if sells.empty:
        return pd.DataFrame(
            columns=[
                "year_month", "month", "closed_trades", "wins", "losses",
                "win_rate_pct", "realized_pnl", "cum_pnl", "cum_return_pct", "drawdown_pct",
            ]
        )
    sells["month"] = sells["sell_date"].astype(str).str[:6]
    sells["is_win"] = sells["pnl"] > 0

    monthly = (
        sells.groupby("month", sort=True)
        .agg(
            closed_trades=("trade_id", "count"),
            wins=("is_win", "sum"),
            realized_pnl=("pnl", "sum"),
        )
        .reset_index()
    )
    monthly["losses"] = monthly["closed_trades"] - monthly["wins"]
    monthly["win_rate_pct"] = (monthly["wins"] / monthly["closed_trades"] * 100).round(2)
    monthly["realized_pnl"] = monthly["realized_pnl"].round(2)
    monthly["cum_pnl"] = monthly["realized_pnl"].cumsum().round(2)
    monthly["cum_return_pct"] = (monthly["cum_pnl"] / initial_cash * 100).round(4)

    # 回撤（以 initial_cash + cum_pnl 为净值近似）
    equity = initial_cash + monthly["cum_pnl"]
    peak = equity.cummax()
    monthly["drawdown_pct"] = ((equity - peak) / peak * 100).round(4)

    monthly.insert(
        0,
        "year_month",
        monthly["month"].astype(str).str.replace(r"(\d{4})(\d{2})", r"\1-\2", regex=True),
    )
    return monthly[
        [
            "year_month", "month", "closed_trades", "wins", "losses",
            "win_rate_pct", "realized_pnl", "cum_pnl", "cum_return_pct", "drawdown_pct",
        ]
    ]


def build_yearly(monthly_df: pd.DataFrame, initial_cash: float) -> pd.DataFrame:
    """按年汇总。"""
    if monthly_df.empty:
        return pd.DataFrame(
            columns=["year", "closed_trades", "wins", "losses", "win_rate_pct", "realized_pnl", "return_pct"]
        )
    df = monthly_df.copy()
    df["year"] = df["month"].astype(str).str[:4]
    yearly = (
        df.groupby("year", sort=True)
        .agg(
            closed_trades=("closed_trades", "sum"),
            wins=("wins", "sum"),
            losses=("losses", "sum"),
            realized_pnl=("realized_pnl", "sum"),
        )
        .reset_index()
    )
    yearly["win_rate_pct"] = (yearly["wins"] / yearly["closed_trades"].replace(0, pd.NA) * 100).round(2)
    yearly["return_pct"] = (yearly["realized_pnl"] / initial_cash * 100).round(4)
    return yearly[["year", "closed_trades", "wins", "losses", "win_rate_pct", "realized_pnl", "return_pct"]]


def simulate_combined(
    trades: pd.DataFrame,
    initial_cash: float,
    per_stock_cap: float,
    cost: Optional[CostConfig] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, AccountSummary]:
    """把所有 bucket 混在同一个账户里跑 —— 当日买入按所有 bucket 的信号一起均分现金。

    这代表「整体资金池」视角，和分 bucket 独立账户相比：
    - 在某一侧密集出信号时，另一侧空着的资金也会被用上；
    - 单股限额 per_stock_cap 仍然生效，避免集中度爆掉。
    """
    merged = trades.copy()
    merged["bucket"] = "ALL"
    return simulate_bucket(merged, "ALL", initial_cash, per_stock_cap, cost)
