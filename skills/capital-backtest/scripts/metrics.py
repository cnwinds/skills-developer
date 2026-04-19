"""高阶指标：基于月度已实现盈亏序列计算回撤、夏普、Calmar、月胜率等。

注意（口径）：
- 我们的回测是事件驱动 + 已实现盈亏，没有持仓期间的逐日盯市。
- 月度净值近似 = initial_cash + cum_realized_pnl_at_month_end。
- 因此 Sharpe/Sortino 是「月度已实现收益」的近似指标，不等于
  逐日 mark-to-market 的 Sharpe，更适合做不同策略之间的横向比较。
"""

from __future__ import annotations

import math
from typing import Optional

import pandas as pd


def _monthly_returns(monthly: pd.DataFrame, initial_cash: float) -> pd.Series:
    """将月度已实现盈亏转成月度收益率（以期初权益为分母）。"""
    if monthly.empty:
        return pd.Series(dtype=float)
    equity = initial_cash + monthly["cum_pnl"]
    prev_equity = equity.shift(1).fillna(initial_cash)
    return (monthly["realized_pnl"] / prev_equity).astype(float)


def compute_metrics(
    monthly: pd.DataFrame,
    log_df: pd.DataFrame,
    initial_cash: float,
    risk_free_annual: float = 0.0,
) -> dict:
    """汇总常用指标，返回 dict（已四舍五入到便于阅读的位数）。"""
    if monthly.empty:
        return {
            "months_with_trades": 0,
            "first_month": None,
            "last_month": None,
            "total_return_pct": 0.0,
            "cagr_pct": None,
            "max_drawdown_pct": 0.0,
            "max_drawdown_month": None,
            "monthly_win_rate_pct": None,
            "best_month": None,
            "best_month_pnl": None,
            "worst_month": None,
            "worst_month_pnl": None,
            "sharpe_monthly": None,
            "sortino_monthly": None,
            "calmar": None,
            "trade_win_rate_pct": None,
            "avg_win_pnl": None,
            "avg_loss_pnl": None,
            "profit_factor": None,
            "max_consecutive_losses": 0,
        }

    months = monthly["month"].astype(str).tolist()
    first_month, last_month = months[0], months[-1]
    n_months = len(months)
    total_pnl = float(monthly["realized_pnl"].sum())
    total_return = total_pnl / initial_cash

    # CAGR：按自然月跨度估算年化（首月到末月，含端点）
    span_months = _month_diff(first_month, last_month) + 1
    years = span_months / 12.0
    cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 and (1 + total_return) > 0 else None

    # 最大回撤
    equity = initial_cash + monthly["cum_pnl"]
    peak = equity.cummax()
    dd = (equity - peak) / peak
    max_dd = float(dd.min()) if not dd.empty else 0.0
    max_dd_month = months[int(dd.idxmin())] if not dd.empty and max_dd < 0 else None

    # 月度胜率（盈利月份比例）
    monthly_returns = _monthly_returns(monthly, initial_cash)
    monthly_win_rate = float((monthly_returns > 0).mean() * 100) if not monthly_returns.empty else None

    # 最佳/最差月
    idx_best = int(monthly["realized_pnl"].idxmax())
    idx_worst = int(monthly["realized_pnl"].idxmin())
    best_month = months[idx_best]
    worst_month = months[idx_worst]
    best_pnl = float(monthly.loc[idx_best, "realized_pnl"])
    worst_pnl = float(monthly.loc[idx_worst, "realized_pnl"])

    # Sharpe / Sortino（月度，年化系数 sqrt(12)）
    rf_monthly = (1 + risk_free_annual) ** (1 / 12) - 1 if risk_free_annual else 0.0
    excess = monthly_returns - rf_monthly
    sharpe = _annualized_ratio(excess)
    downside = excess[excess < 0]
    sortino = _annualized_ratio(excess, downside_std=downside.std(ddof=1)) if not downside.empty else None

    # Calmar
    calmar = (cagr / abs(max_dd)) if (cagr is not None and max_dd < 0) else None

    # 笔级指标
    sells = log_df[log_df["event"] == "SELL"] if not log_df.empty else log_df
    if sells.empty:
        trade_win_rate = avg_win = avg_loss = profit_factor = max_consec_loss = None
    else:
        wins = sells[sells["pnl"] > 0]["pnl"]
        losses = sells[sells["pnl"] < 0]["pnl"]
        n = len(sells)
        trade_win_rate = float(len(wins) / n * 100) if n else None
        avg_win = float(wins.mean()) if not wins.empty else None
        avg_loss = float(losses.mean()) if not losses.empty else None
        profit_factor = float(wins.sum() / abs(losses.sum())) if not losses.empty and losses.sum() != 0 else None
        max_consec_loss = _max_consecutive(sells["pnl"].tolist(), lambda x: x < 0)

    return {
        "months_with_trades": int(n_months),
        "first_month": first_month,
        "last_month": last_month,
        "total_return_pct": round(total_return * 100, 4),
        "cagr_pct": round(cagr * 100, 4) if cagr is not None else None,
        "max_drawdown_pct": round(max_dd * 100, 4),
        "max_drawdown_month": max_dd_month,
        "monthly_win_rate_pct": round(monthly_win_rate, 2) if monthly_win_rate is not None else None,
        "best_month": best_month,
        "best_month_pnl": round(best_pnl, 2),
        "worst_month": worst_month,
        "worst_month_pnl": round(worst_pnl, 2),
        "sharpe_monthly": round(sharpe, 4) if sharpe is not None else None,
        "sortino_monthly": round(sortino, 4) if sortino is not None else None,
        "calmar": round(calmar, 4) if calmar is not None else None,
        "trade_win_rate_pct": round(trade_win_rate, 2) if trade_win_rate is not None else None,
        "avg_win_pnl": round(avg_win, 2) if avg_win is not None else None,
        "avg_loss_pnl": round(avg_loss, 2) if avg_loss is not None else None,
        "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "max_consecutive_losses": int(max_consec_loss) if max_consec_loss is not None else 0,
    }


def _annualized_ratio(excess: pd.Series, downside_std: Optional[float] = None) -> Optional[float]:
    if excess.empty or len(excess) < 2:
        return None
    std = downside_std if downside_std is not None else excess.std(ddof=1)
    if std is None or std == 0 or (isinstance(std, float) and math.isnan(std)):
        return None
    return float(excess.mean() / std * math.sqrt(12))


def _month_diff(a: str, b: str) -> int:
    """两个 YYYYMM 字符串的整月差（b - a）。"""
    ay, am = int(a[:4]), int(a[4:6])
    by, bm = int(b[:4]), int(b[4:6])
    return (by - ay) * 12 + (bm - am)


def _max_consecutive(seq, predicate) -> int:
    best = cur = 0
    for x in seq:
        if predicate(x):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best
