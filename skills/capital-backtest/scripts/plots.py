"""权益曲线 + 月度热力图。所有图都用 matplotlib，无需额外依赖。"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # 服务端无显示设备
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# 中文字体尽量通用：用 DejaVu 做底，显式提到 CJK 字体时由系统决定
plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Microsoft YaHei", "PingFang SC", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


def plot_equity_curve(
    monthly: pd.DataFrame,
    initial_cash: float,
    out_path: str | Path,
    title: str = "Equity curve",
) -> Path:
    """以月末净值近似画曲线，另画回撤填色。"""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if monthly.empty:
        # 空图，但产文件，保证下游打包不报错
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "no trades", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        fig.savefig(out_path, dpi=140, bbox_inches="tight")
        plt.close(fig)
        return out_path

    ym = monthly["year_month"].tolist()
    equity = initial_cash + monthly["cum_pnl"].values
    peak = np.maximum.accumulate(equity)
    dd = (equity - peak) / peak * 100

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 5.5), sharex=True,
        gridspec_kw={"height_ratios": [3, 1], "hspace": 0.08},
    )

    ax1.plot(ym, equity, color="#1f77b4", linewidth=1.6)
    ax1.fill_between(ym, initial_cash, equity, where=(equity >= initial_cash), alpha=0.08, color="#1f77b4")
    ax1.axhline(initial_cash, color="#888", linewidth=0.8, linestyle="--")
    ax1.set_ylabel("Equity")
    ax1.set_title(title)
    ax1.grid(alpha=0.3)

    ax2.fill_between(ym, 0, dd, color="#d62728", alpha=0.35)
    ax2.plot(ym, dd, color="#d62728", linewidth=0.8)
    ax2.set_ylabel("Drawdown %")
    ax2.grid(alpha=0.3)

    step = max(1, len(ym) // 14)
    ax2.set_xticks(ym[::step])
    for label in ax2.get_xticklabels():
        label.set_rotation(45)
        label.set_ha("right")

    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_monthly_heatmap(
    monthly: pd.DataFrame,
    out_path: str | Path,
    title: str = "Monthly realized PnL heatmap",
    value_col: str = "realized_pnl",
) -> Path:
    """画 年 x 月 的热力图，负红正绿，0 居中。"""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if monthly.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "no trades", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        fig.savefig(out_path, dpi=140, bbox_inches="tight")
        plt.close(fig)
        return out_path

    df = monthly.copy()
    df["year"] = df["month"].astype(str).str[:4].astype(int)
    df["m"] = df["month"].astype(str).str[4:6].astype(int)
    pivot = df.pivot_table(index="year", columns="m", values=value_col, aggfunc="sum")
    pivot = pivot.reindex(columns=range(1, 13))

    vmax = float(np.nanmax(np.abs(pivot.values))) if pivot.notna().any().any() else 1.0
    vmax = max(vmax, 1.0)

    fig, ax = plt.subplots(figsize=(11, max(2.5, 0.45 * len(pivot.index) + 1.5)))
    im = ax.imshow(pivot.values, cmap="RdYlGn", vmin=-vmax, vmax=vmax, aspect="auto")
    ax.set_xticks(range(12))
    ax.set_xticklabels([f"{m:02d}" for m in range(1, 13)])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index.tolist())
    ax.set_xlabel("Month")
    ax.set_ylabel("Year")
    ax.set_title(title)

    for yi, year in enumerate(pivot.index):
        for xi, m in enumerate(pivot.columns):
            v = pivot.iloc[yi, xi]
            if pd.isna(v):
                continue
            txt = f"{v:,.0f}"
            ax.text(xi, yi, txt, ha="center", va="center", fontsize=8,
                    color="#222" if abs(v) < vmax * 0.6 else "white")

    cbar = fig.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label(value_col)

    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    return out_path
