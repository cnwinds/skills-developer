"""Adapter 契约 & 共用工具。

每个 adapter 文件（adapters/<slug>.py）必须暴露：

- ``NAME``: str —— CLI 上用的 slug，例如 ``"930_00c"``。
- ``DESCRIPTION``: str —— 一行人类可读描述。
- ``add_arguments(parser)`` —— 往 parser 注册 adapter 专属参数。
- ``load(args) -> pandas.DataFrame`` —— 返回标准 trade table。

标准 trade table 必须至少含这些列（类型/范围由 ``engine._normalize`` 再校验一遍）：

    code        str
    bucket      str
    buy_date    str / datetime，能被清洗成 8 位数字
    buy_price   float (>0)
    sell_date   str / datetime
    sell_price  float (>0)

可选列：``name``、``signal_date``、``rank``。

本模块把所有 adapter 都会用到的 I/O、列名映射、过滤、板块推断等工具
集中在一个地方，新增 adapter 可以直接 import 过去用。
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_csv_smart(path: str | Path, encoding: str = "utf-8-sig") -> pd.DataFrame:
    """读一张 CSV / Parquet，保留 code / signal_date 等列的字符串语义。

    - 自动按后缀切 CSV vs Parquet。
    - CSV 里 ``code``、``signal_date``、``buy_date``、``sell_date`` 默认以字符串读入，
      避免 ``300258.SZ`` 这种被 pandas 猜成 float。
    - 未知列不强求 dtype。
    """
    p = Path(path)
    if p.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(p)
    string_cols = ["code", "ts_code", "symbol", "signal_date",
                   "buy_date", "sell_date", "买入日", "卖出日"]
    return pd.read_csv(p, dtype={c: str for c in string_cols}, encoding=encoding)


# ---------------------------------------------------------------------------
# 列名映射
# ---------------------------------------------------------------------------

# 各 adapter 共用的"无歧义"别名。含歧义（如 sell_date_8d vs sell_date_25d）
# 的列不放这里，由调用方显式指定 --col-map 或专用 adapter 处理。
COMMON_ALIASES: dict[str, str] = {
    # Qinglong 的 _25d 后缀（老项目只产一种，属于历史遗留无歧义）
    "buy_date_25d": "buy_date",
    "sell_date_25d": "sell_date",
    "buy_price_25d": "buy_price",
    "sell_price_25d": "sell_price",
    "setting_rank": "rank",
    "tier_rank": "rank",
    "pool": "bucket",
    "board": "bucket",
    "sector": "bucket",
    # 中文列（930-00c 等项目常见）
    "买入日": "buy_date",
    "买入价": "buy_price",
    "卖出日": "sell_date",
    "卖出价": "sell_price",
    # 代码列别名
    "ts_code": "code",
    "symbol": "code",
}


def apply_col_map(df: pd.DataFrame, col_map: dict[str, str] | None = None,
                  extra_aliases: dict[str, str] | None = None) -> pd.DataFrame:
    """按顺序做两轮重命名：先 ``col_map`` 用户指定，再 ``extra_aliases`` + COMMON。

    col_map 优先，这样用户可以覆盖内置别名。
    """
    df = df.copy()
    if col_map:
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    merged = dict(COMMON_ALIASES)
    if extra_aliases:
        merged.update(extra_aliases)
    df = df.rename(columns={k: v for k, v in merged.items() if k in df.columns})
    return df


# ---------------------------------------------------------------------------
# 行过滤
# ---------------------------------------------------------------------------

def apply_query_filters(df: pd.DataFrame, expressions: Sequence[str] | None) -> pd.DataFrame:
    """用 ``DataFrame.query`` 依次过滤行。

    每条表达式都会被尝试：能跑就跑，跑不了（列不存在等）会抛出 ValueError
    让调用方看到完整上下文。

    例子：``["is_buy == True", "signal_date >= '20200101'"]``
    """
    if not expressions:
        return df
    out = df
    for expr in expressions:
        expr = expr.strip()
        if not expr:
            continue
        try:
            out = out.query(expr)
        except Exception as exc:
            raise ValueError(f"--filter 表达式无法求值：{expr!r} ({exc})") from exc
    return out.reset_index(drop=True)


def apply_date_range(df: pd.DataFrame, date_from: str | None, date_to: str | None,
                     column: str = "buy_date") -> pd.DataFrame:
    """按 ``column`` 取 [date_from, date_to] 闭区间。

    date_from/date_to 接受 ``YYYYMMDD`` 或 ``YYYY-MM-DD``；空值 / ``None`` 不过滤。
    """
    if column not in df.columns:
        return df
    if date_from is None and date_to is None:
        return df
    s = df[column].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    lo = _canon_date(date_from) if date_from else None
    hi = _canon_date(date_to) if date_to else None
    mask = pd.Series(True, index=df.index)
    if lo is not None:
        mask &= (s >= lo)
    if hi is not None:
        mask &= (s <= hi)
    return df.loc[mask].reset_index(drop=True)


def _canon_date(raw: str) -> str:
    cleaned = "".join(ch for ch in str(raw) if ch.isdigit())[:8]
    if len(cleaned) != 8:
        raise ValueError(f"无法解析日期: {raw!r}，期望 YYYYMMDD 或 YYYY-MM-DD")
    return cleaned


def apply_pool_filter(df: pd.DataFrame, pool: Iterable[str] | None,
                      column: str = "code") -> pd.DataFrame:
    """保留 ``df[column]`` 在 pool 集合内的行。

    pool 为 None 或空集合时不过滤。code 会做一次规整（去首尾空格、统一大写）。
    """
    if not pool:
        return df
    wanted = {str(c).strip().upper() for c in pool if str(c).strip()}
    if not wanted:
        return df
    key = df[column].astype(str).str.strip().str.upper()
    # 允许 pool 只给 6 位数字（带不带 .SH/.SZ 后缀都匹配）
    pure = key.str.split(".", n=1).str[0]
    matches = key.isin(wanted) | pure.isin(wanted)
    return df.loc[matches].reset_index(drop=True)


def load_pool(pool_spec: str | None) -> list[str] | None:
    """把 ``--pool`` 的值解析成代码列表。支持：

    - ``None``：不过滤。
    - 以 ``.json`` 结尾的路径：JSON 数组；或 JSON 对象、取 ``"codes"``/``"pool"`` 字段。
    - 以 ``.csv`` / ``.txt`` 结尾：按行或第一列读。
    - 其他：逗号分隔的 inline 列表。
    """
    if not pool_spec:
        return None
    p = Path(pool_spec)
    if p.exists() and p.suffix.lower() == ".json":
        import json
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [str(x) for x in data]
        if isinstance(data, dict):
            for key in ("codes", "pool", "stocks"):
                if key in data and isinstance(data[key], list):
                    return [str(x) for x in data[key]]
            # 930-00c 的 pool_j.json：{ "600000.SH": {...}, ... }
            if data and all(isinstance(k, str) for k in data):
                return list(data.keys())
        raise ValueError(f"无法识别 pool JSON 结构: {p}")
    if p.exists() and p.suffix.lower() in {".csv", ".txt", ".tsv"}:
        # 首列当 code；跳过 header 如果看起来像 header
        lines = [line.strip().split(",")[0].split("\t")[0].strip()
                 for line in p.read_text(encoding="utf-8-sig").splitlines()
                 if line.strip()]
        if lines and lines[0].lower() in {"code", "ts_code", "symbol"}:
            lines = lines[1:]
        return [x for x in lines if x]
    # inline，逗号分隔
    return [x.strip() for x in pool_spec.split(",") if x.strip()]


# ---------------------------------------------------------------------------
# 板块推断 —— vendored 自 02_core/board_classifier.py（保持逐字一致）
# ---------------------------------------------------------------------------
# 来源：930-00c 项目 02_core/board_classifier.py
# 同步原则：如改动这里的逻辑，必须同步改项目内版本，并让 smoke_930_00c.py 做一致性校验。


def normalize_ts_code(raw: object) -> str:
    text = str(raw).strip().upper()
    if text.endswith(".SZ") or text.endswith(".SH") or text.endswith(".BJ"):
        return text
    if len(text) == 6 and text.isdigit():
        if text.startswith("8") or text.startswith("4"):
            return f"{text}.BJ"
        return f"{text}.SZ" if text.startswith(("0", "3")) else f"{text}.SH"
    return text


def infer_board(ts_code: object) -> str:
    """A-share board labels for cross-section backtest comparison.

    Returns one of: KCB, CYB, BSE, SH_MAIN, SZ_MAIN, SZ_SME, OTHER.
    """
    code = normalize_ts_code(ts_code)
    pure = code.split(".", 1)[0] if "." in code else code

    if code.endswith(".BJ") or pure.startswith(("8", "4")):
        return "BSE"
    if code.endswith(".SH"):
        if pure.startswith("688"):
            return "KCB"
        if pure.startswith(("600", "601", "603", "605")):
            return "SH_MAIN"
        return "OTHER"
    if code.endswith(".SZ"):
        if pure.startswith("300"):
            return "CYB"
        if pure.startswith(("000", "001")):
            return "SZ_MAIN"
        if pure.startswith("002"):
            return "SZ_SME"
        return "OTHER"
    return "OTHER"


BOARD_ORDER = ["KCB", "CYB", "BSE", "SH_MAIN", "SZ_MAIN", "SZ_SME", "OTHER"]


# ---------------------------------------------------------------------------
# 收尾
# ---------------------------------------------------------------------------

STANDARD_COLUMNS: tuple[str, ...] = (
    "code", "name", "bucket", "signal_date",
    "buy_date", "buy_price", "sell_date", "sell_price", "rank",
)


def finalize_trade_table(df: pd.DataFrame, default_bucket: str | None = None) -> pd.DataFrame:
    """统一列名/补齐可选列/扔掉无效行。

    这是 adapter 结束前最后的 "标准化出口"。它不 replace ``engine._normalize``
    的严格校验，但能在更早一步给出可读的错误。
    """
    df = df.copy()
    if "bucket" not in df.columns:
        if default_bucket is None:
            raise KeyError("trade table 缺少 bucket 列，请用 --bucket-default 或 --bucket-from-code")
        df["bucket"] = default_bucket
    if "name" not in df.columns:
        df["name"] = df["code"].astype(str)
    if "signal_date" not in df.columns:
        df["signal_date"] = ""
    if "rank" not in df.columns:
        df["rank"] = 999

    required = ["code", "bucket", "buy_date", "buy_price", "sell_date", "sell_price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"trade table 缺少必需列: {missing}（现有列: {list(df.columns)}）")

    # 保留标准列 + 任何额外列（方便后续查，但不会被 engine 用到）
    ordered = list(STANDARD_COLUMNS) + [c for c in df.columns if c not in STANDARD_COLUMNS]
    return df.loc[:, [c for c in ordered if c in df.columns]].reset_index(drop=True)
