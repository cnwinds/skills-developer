#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import struct
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

try:
    import duckdb
    import pyarrow as pa
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: duckdb and/or pyarrow. Install with: python -m pip install duckdb pyarrow"
    ) from exc


RECORD_SIZE = 32
DAY_RECORD = struct.Struct("<IIIIIfII")
LC5_RECORD = struct.Struct("<HHfffffii")
TNF_HEADER_SIZE = 50
TNF_RECORD_SIZE = 360

MARKET_BY_DIGIT = {"0": "sz", "1": "sh", "2": "bj"}
MARKET_BY_CODE_LEAD = {"0": "sz", "3": "sz", "6": "sh", "4": "bj", "8": "bj"}


DAILY_SCHEMA = pa.schema(
    [
        ("market", pa.string()),
        ("symbol", pa.string()),
        ("secid", pa.string()),
        ("trade_date", pa.int32()),
        ("open", pa.float32()),
        ("high", pa.float32()),
        ("low", pa.float32()),
        ("close", pa.float32()),
        ("amount", pa.float64()),
        ("volume", pa.int64()),
    ]
)

MIN5_SCHEMA = pa.schema(
    [
        ("market", pa.string()),
        ("symbol", pa.string()),
        ("secid", pa.string()),
        ("trade_date", pa.int32()),
        ("bar_time", pa.int64()),
        ("open", pa.float32()),
        ("high", pa.float32()),
        ("low", pa.float32()),
        ("close", pa.float32()),
        ("amount", pa.float64()),
        ("volume", pa.int64()),
    ]
)

DUCKDB_FILENAME = "tdx.duckdb"

BAR_TABLE_DDL = {
    "daily": """
        CREATE TABLE IF NOT EXISTS daily (
            market VARCHAR,
            symbol VARCHAR,
            secid VARCHAR,
            trade_date INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            amount DOUBLE,
            volume BIGINT
        )
    """,
    "min5": """
        CREATE TABLE IF NOT EXISTS min5 (
            market VARCHAR,
            symbol VARCHAR,
            secid VARCHAR,
            trade_date INTEGER,
            bar_time BIGINT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            amount DOUBLE,
            volume BIGINT
        )
    """,
}


REFERENCE_SOURCE_FILES = [
    "T0002/hq_cache/shs.tnf",
    "T0002/hq_cache/szs.tnf",
    "T0002/hq_cache/bjs.tnf",
    "T0002/hq_cache/base.dbf",
    "T0002/hq_cache/base.map",
    "T0002/hq_cache/gbbq",
    "T0002/hq_cache/gbbq.map",
    "T0002/hq_cache/tdxhy.cfg",
    "T0002/hq_cache/tdxzs.cfg",
    "T0002/hq_cache/tdxzs3.cfg",
    "T0002/hq_cache/tdxzsbase.cfg",
    "T0002/hq_cache/infoharbor_block.dat",
    "T0002/hq_cache/csiblock.dat",
    "T0002/hq_cache/hkblock.dat",
    "T0002/hq_cache/jjblock.dat",
    "T0002/hq_cache/mgblock.dat",
    "T0002/hq_cache/sbblock.dat",
    "T0002/hq_cache/spblock.dat",
    "T0002/hq_cache/ukblock.dat",
    "T0002/hq_cache/sgxblock.dat",
    "T0002/hq_cache/specgpext.txt",
    "T0002/hq_cache/specetfdata.txt",
    "T0002/hq_cache/speclofdata.txt",
    "T0002/hq_cache/specjjdata.txt",
    "T0002/hq_cache/code2name.ini",
]


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def validate_yyyymmdd(yyyymmdd: int) -> bool:
    if yyyymmdd < 19900101 or yyyymmdd > 21001231:
        return False
    year = yyyymmdd // 10000
    month = (yyyymmdd // 100) % 100
    day = yyyymmdd % 100
    try:
        date(year, month, day)
    except ValueError:
        return False
    return True


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if not re.fullmatch(r"-?\d+", text):
        return None
    return int(text)


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def clean_ascii_token(raw: bytes) -> str:
    text = raw.decode("ascii", "ignore").replace("\x00", "")
    return "".join(ch for ch in text if ch.isalnum() or ch in "._-").strip()


def market_from_digit(value: str | int | None) -> str | None:
    if value is None:
        return None
    return MARKET_BY_DIGIT.get(str(value))


def market_from_symbol(symbol: str | None) -> str | None:
    if not symbol:
        return None
    if len(symbol) >= 1:
        return MARKET_BY_CODE_LEAD.get(symbol[0])
    return None


def make_secid(market: str | None, symbol: str | None) -> str | None:
    if not market or not symbol:
        return None
    return f"{market}{symbol}"


def decode_lc5_trade_date(raw_date: int) -> int | None:
    year = (raw_date >> 11) + 2004
    month_day = raw_date & 0x7FF
    month = month_day // 100
    day = month_day % 100
    try:
        date(year, month, day)
    except ValueError:
        return None
    return year * 10000 + month * 100 + day


def decode_bar_time(trade_date: int, minute_of_day: int) -> int | None:
    if minute_of_day < 0 or minute_of_day >= 24 * 60:
        return None
    hour = minute_of_day // 60
    minute = minute_of_day % 60
    if minute > 59:
        return None
    return trade_date * 10000 + hour * 100 + minute


def default_state() -> dict:
    return {
        "version": 2,
        "updated_at": None,
        "sources": {"daily": {}, "min5": {}, "reference": {}},
    }


def load_state(path: Path) -> dict:
    if not path.exists():
        return default_state()
    with path.open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    if not isinstance(loaded, dict):
        return default_state()
    if "sources" not in loaded or not isinstance(loaded["sources"], dict):
        loaded["sources"] = {}
    loaded["sources"].setdefault("daily", {})
    loaded["sources"].setdefault("min5", {})
    loaded["sources"].setdefault("reference", {})
    loaded.setdefault("version", 2)
    loaded.setdefault("updated_at", None)
    return loaded


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    tmp.replace(path)


class Buffer:
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns
        self.data: dict[str, list] = {name: [] for name in columns}

    def __len__(self) -> int:
        if not self.columns:
            return 0
        return len(self.data[self.columns[0]])

    def clear(self) -> None:
        for column in self.columns:
            self.data[column].clear()

    def add(self, row: tuple) -> None:
        for idx, column in enumerate(self.columns):
            self.data[column].append(row[idx])


def database_path(output_root: Path) -> Path:
    return output_root / DUCKDB_FILENAME


def connect_database(path: Path) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    thread_count = max(1, os.cpu_count() or 1)
    con.execute(f"PRAGMA threads={thread_count}")
    return con


def ensure_bar_table(con: duckdb.DuckDBPyConnection, spec: "DatasetSpec") -> None:
    con.execute(BAR_TABLE_DDL[spec.output_name])


def ensure_indexes(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_daily_secid_trade_date ON daily (secid, trade_date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON daily (trade_date)",
        "CREATE INDEX IF NOT EXISTS idx_min5_secid_bar_time ON min5 (secid, bar_time)",
        "CREATE INDEX IF NOT EXISTS idx_min5_trade_date ON min5 (trade_date)",
        "CREATE INDEX IF NOT EXISTS idx_security_master_secid ON security_master (secid)",
        "CREATE INDEX IF NOT EXISTS idx_security_profile_secid ON security_profile (secid)",
        "CREATE INDEX IF NOT EXISTS idx_security_industry_map_secid ON security_industry_map (secid)",
        "CREATE INDEX IF NOT EXISTS idx_block_member_secid ON block_member (secid)",
        "CREATE INDEX IF NOT EXISTS idx_block_member_block ON block_member (block_name, secid)",
        "CREATE INDEX IF NOT EXISTS idx_index_snapshot_secid_date ON index_snapshot (secid, trade_date)",
        "CREATE INDEX IF NOT EXISTS idx_security_business_secid ON security_business (secid)",
        "CREATE INDEX IF NOT EXISTS idx_etf_meta_secid ON etf_meta (secid)",
        "CREATE INDEX IF NOT EXISTS idx_lof_meta_secid ON lof_meta (secid)",
        "CREATE INDEX IF NOT EXISTS idx_fund_nav_snapshot_secid_date ON fund_nav_snapshot (secid, trade_date)",
        "CREATE INDEX IF NOT EXISTS idx_map_offsets_secid ON map_offsets (secid)",
        "CREATE INDEX IF NOT EXISTS idx_corporate_action_secid ON corporate_action (secid, ex_date)",
        "CREATE INDEX IF NOT EXISTS idx_corporate_action_ex_date ON corporate_action (ex_date)",
    ]:
        try:
            con.execute(stmt)
        except duckdb.Error:
            continue
    con.execute("ANALYZE")


def flush_buffer(
    buffer: Buffer,
    schema: pa.Schema,
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> int:
    rows = len(buffer)
    if rows == 0:
        return 0
    arrow_table = pa.Table.from_pydict(buffer.data, schema=schema)
    relation_name = f"_buffer_{uuid4().hex[:10]}"
    con.register(relation_name, arrow_table)
    try:
        con.execute(f"INSERT INTO {table_name} SELECT * FROM {relation_name}")
    finally:
        con.unregister(relation_name)
    buffer.clear()
    return rows


def parse_day_records(data: bytes, start_index: int):
    valid_length = (len(data) // RECORD_SIZE) * RECORD_SIZE
    if valid_length <= start_index * RECORD_SIZE:
        return
    payload = memoryview(data)[start_index * RECORD_SIZE : valid_length]
    for trade_date, op, hi, lo, cl, amount, volume, _ in DAY_RECORD.iter_unpack(payload):
        if not validate_yyyymmdd(trade_date):
            continue
        yield (
            int(trade_date),
            float(op) / 100.0,
            float(hi) / 100.0,
            float(lo) / 100.0,
            float(cl) / 100.0,
            float(amount),
            int(volume),
        )


def parse_lc5_records(data: bytes, start_index: int):
    valid_length = (len(data) // RECORD_SIZE) * RECORD_SIZE
    if valid_length <= start_index * RECORD_SIZE:
        return
    payload = memoryview(data)[start_index * RECORD_SIZE : valid_length]
    for raw_date, minute_of_day, op, hi, lo, cl, amount, volume, _ in LC5_RECORD.iter_unpack(
        payload
    ):
        trade_date = decode_lc5_trade_date(int(raw_date))
        if trade_date is None:
            continue
        bar_time = decode_bar_time(trade_date, int(minute_of_day))
        if bar_time is None:
            continue
        yield (
            int(trade_date),
            int(bar_time),
            float(op),
            float(hi),
            float(lo),
            float(cl),
            float(amount),
            int(volume),
        )


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    subdir: str
    extension: str
    schema: pa.Schema
    output_name: str
    parser: Callable[[bytes, int], object]
    columns: list[str]


DATASETS: dict[str, DatasetSpec] = {
    "daily": DatasetSpec(
        name="daily",
        subdir="lday",
        extension=".day",
        schema=DAILY_SCHEMA,
        output_name="daily",
        parser=parse_day_records,
        columns=[
            "market",
            "symbol",
            "secid",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "amount",
            "volume",
        ],
    ),
    "min5": DatasetSpec(
        name="min5",
        subdir="fzline",
        extension=".lc5",
        schema=MIN5_SCHEMA,
        output_name="min5",
        parser=parse_lc5_records,
        columns=[
            "market",
            "symbol",
            "secid",
            "trade_date",
            "bar_time",
            "open",
            "high",
            "low",
            "close",
            "amount",
            "volume",
        ],
    ),
}


def list_source_files(
    tdx_root: Path,
    markets: list[str],
    spec: DatasetSpec,
    max_files: int | None,
) -> list[tuple[str, Path]]:
    results: list[tuple[str, Path]] = []
    for market in markets:
        folder = tdx_root / "vipdoc" / market / spec.subdir
        if not folder.exists():
            continue
        files = sorted(folder.glob(f"*{spec.extension}"))
        for file_path in files:
            results.append((market, file_path))
            if max_files is not None and len(results) >= max_files:
                return results
    return results


def process_bar_dataset(
    spec: DatasetSpec,
    con: duckdb.DuckDBPyConnection,
    tdx_root: Path,
    state_section: dict[str, dict],
    markets: list[str],
    batch_size: int,
    full_rebuild: bool,
    fail_on_reset: bool,
    max_files: int | None,
    prune_state: bool,
) -> tuple[dict, dict]:
    files = list_source_files(tdx_root, markets, spec, max_files)
    buffer = Buffer(spec.columns)
    ensure_bar_table(con, spec)
    next_state = {} if full_rebuild else dict(state_section)
    seen_keys: set[str] = set()
    touched_dates: set[int] = set()
    touched_secids: set[str] = set()
    written_rows = 0
    written_files = 0
    skipped_files = 0
    replaced_files = 0
    parse_errors: list[str] = []

    if full_rebuild:
        market_list = ",".join("?" for _ in markets)
        con.execute(f"DELETE FROM {spec.output_name} WHERE market IN ({market_list})", markets)

    for file_index, (market, file_path) in enumerate(files, start=1):
        stat = file_path.stat()
        if stat.st_size < RECORD_SIZE:
            skipped_files += 1
            continue
        total_records = stat.st_size // RECORD_SIZE
        key = f"{market}/{file_path.name.lower()}"
        seen_keys.add(key)
        old = next_state.get(key, {})

        same_file = (
            (not full_rebuild)
            and old.get("size") == stat.st_size
            and old.get("mtime_ns") == stat.st_mtime_ns
            and old.get("records") == total_records
        )
        if same_file:
            skipped_files += 1
            continue

        start_index = 0
        replace_existing = full_rebuild
        if not full_rebuild and old:
            old_records = int(old.get("records", 0))
            if total_records < old_records:
                message = (
                    f"{spec.name}: file shrank, key={key}, old_records={old_records}, "
                    f"new_records={total_records}"
                )
                if fail_on_reset:
                    raise RuntimeError(message)
                parse_errors.append(message)
                start_index = 0
                replace_existing = True
            elif total_records == old_records:
                start_index = 0
                replace_existing = True
            else:
                start_index = old_records

        if start_index >= total_records:
            next_state[key] = {
                "records": total_records,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
            skipped_files += 1
            continue

        raw = file_path.read_bytes()
        stem = file_path.stem.lower()
        symbol = stem[len(market) :] if stem.startswith(market) else stem
        secid = f"{market}{symbol}"
        file_rows: list[tuple] = []

        try:
            if spec.name == "daily":
                for trade_date, op, hi, lo, cl, amount, volume in spec.parser(raw, start_index):
                    touched_dates.add(trade_date)
                    file_rows.append(
                        (
                            market,
                            symbol,
                            secid,
                            trade_date,
                            op,
                            hi,
                            lo,
                            cl,
                            amount,
                            volume,
                        )
                    )
            else:
                for trade_date, bar_time, op, hi, lo, cl, amount, volume in spec.parser(
                    raw, start_index
                ):
                    touched_dates.add(trade_date)
                    file_rows.append(
                        (
                            market,
                            symbol,
                            secid,
                            trade_date,
                            bar_time,
                            op,
                            hi,
                            lo,
                            cl,
                            amount,
                            volume,
                        )
                    )
        except Exception as exc:  # noqa: BLE001
            parse_errors.append(f"{spec.name}: parse failed for {file_path}: {exc}")
            continue

        if replace_existing:
            con.execute(f"DELETE FROM {spec.output_name} WHERE secid = ?", [secid])
            replaced_files += 1
        touched_secids.add(secid)
        for row in file_rows:
            buffer.add(row)
            if len(buffer) >= batch_size:
                written_rows += flush_buffer(buffer, spec.schema, con, spec.output_name)

        next_state[key] = {
            "records": total_records,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
        written_files += 1

        if file_index % 500 == 0:
            print(
                f"[{spec.name}] scanned={file_index}/{len(files)} "
                f"written_files={written_files} skipped={skipped_files}",
                flush=True,
            )

    if len(buffer) > 0:
        written_rows += flush_buffer(buffer, spec.schema, con, spec.output_name)

    if prune_state:
        next_state = {k: v for k, v in next_state.items() if k in seen_keys}

    summary = {
        "dataset": spec.name,
        "source_files": len(files),
        "written_files": written_files,
        "skipped_files": skipped_files,
        "replaced_files": replaced_files,
        "rows_written": written_rows,
        "touched_secids": sorted(touched_secids),
        "touched_trade_dates": sorted(touched_dates),
        "errors": parse_errors,
    }
    return next_state, summary


def text_lines(path: Path) -> list[str]:
    return path.read_bytes().decode("gb18030", "ignore").splitlines()


def parse_tnf(path: Path, market: str) -> list[dict]:
    rows: list[dict] = []
    raw = path.read_bytes()
    if len(raw) <= TNF_HEADER_SIZE:
        return rows
    body = raw[TNF_HEADER_SIZE:]
    records = len(body) // TNF_RECORD_SIZE
    for idx in range(records):
        rec = body[idx * TNF_RECORD_SIZE : (idx + 1) * TNF_RECORD_SIZE]
        symbol = rec[0:6].decode("ascii", "ignore").strip("\x00").strip()
        if not symbol:
            continue
        name = rec[23:39].decode("gb18030", "ignore").replace("\x00", "").strip()
        if not name:
            name = rec[20:52].decode("gb18030", "ignore").replace("\x00", "").strip()
        pinyin = clean_ascii_token(rec[320:360]).upper()
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "secid": make_secid(market, symbol),
                "name": name,
                "pinyin_hint": pinyin or None,
                "tnf_flag_u32_272": int.from_bytes(rec[272:276], "little"),
                "tnf_flag_u32_276": int.from_bytes(rec[276:280], "little"),
                "tnf_type_u16": int.from_bytes(rec[280:282], "little"),
                "tnf_market_u16": int.from_bytes(rec[282:284], "little"),
                "source_file": path.name,
                "row_index": idx,
            }
        )
    return rows


def parse_base_dbf(path: Path) -> list[dict]:
    try:
        from dbfread import DBF
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency: dbfread. Install with: python -m pip install dbfread"
        ) from exc

    rows: list[dict] = []
    table = DBF(str(path), encoding="gb18030", char_decode_errors="ignore")
    for row in table:
        item = {str(k): row[k] for k in row.keys()}
        market = market_from_digit(item.get("SC"))
        symbol = str(item.get("GPDM", "")).strip()
        item["market"] = market
        item["symbol"] = symbol
        item["secid"] = make_secid(market, symbol)
        item["source_file"] = path.name
        rows.append(item)
    return rows


def parse_tdxhy(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split("|")
        if len(parts) < 6:
            continue
        market_digit = parts[0].strip()
        symbol = parts[1].strip()
        if not symbol:
            continue
        market = market_from_digit(market_digit)
        rows.append(
            {
                "market_digit": parse_int(market_digit),
                "market": market,
                "symbol": symbol,
                "secid": make_secid(market, symbol),
                "tdx_industry_code": parts[2].strip() or None,
                "reserved_1": parts[3].strip() or None,
                "reserved_2": parts[4].strip() or None,
                "csi_industry_code": parts[5].strip() or None,
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def parse_tdxzs(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split("|")
        if len(parts) < 6:
            continue
        rows.append(
            {
                "block_source": "tdxzs",
                "block_kind": "TDX",
                "block_name": parts[0].strip() or None,
                "block_code": parts[1].strip() or None,
                "field_03": parts[2].strip() or None,
                "field_04": parts[3].strip() or None,
                "field_05": parts[4].strip() or None,
                "field_06": parts[5].strip() or None,
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def parse_tdxzsbase(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split("|")
        if len(parts) != 26:
            continue
        market_digit = parts[0].strip()
        market = market_from_digit(market_digit)
        symbol = parts[1].strip()
        row = {
            "market_digit": parse_int(market_digit),
            "market": market,
            "symbol": symbol,
            "secid": make_secid(market, symbol),
            "trade_date": parse_int(parts[7]),
            "direction_flag": parse_int(parts[8]),
            "source_file": path.name,
            "line_no": line_no,
        }
        metric_index = 1
        for idx, value in enumerate(parts):
            if idx in (0, 1, 7, 8):
                continue
            row[f"metric_{metric_index:02d}"] = parse_float(value)
            metric_index += 1
        rows.append(row)
    return rows


def parse_infoharbor_block(path: Path) -> tuple[list[dict], list[dict]]:
    def_rows: list[dict] = []
    mem_rows: list[dict] = []
    current_code: str | None = None
    current_name: str | None = None
    current_block_id: str | None = None
    current_kind: str | None = None

    for line_no, line in enumerate(text_lines(path), start=1):
        text = line.strip()
        if not text:
            continue
        if text.startswith("#"):
            header = text[1:]
            parts = header.split(",")
            while len(parts) < 7:
                parts.append("")
            first = parts[0].strip()
            if "_" in first:
                current_kind, current_name = first.split("_", 1)
            else:
                current_kind, current_name = None, first
            current_code = parts[2].strip() or None
            current_block_id = f"infoharbor:{current_code or current_name or line_no}"
            def_rows.append(
                {
                    "block_source": "infoharbor",
                    "block_kind": current_kind,
                    "block_name": current_name,
                    "block_code": current_code,
                    "member_count_hint": parse_int(parts[1]),
                    "create_date": parse_int(parts[3]),
                    "update_date": parse_int(parts[4]),
                    "extra_1": parts[5].strip() or None,
                    "extra_2": parts[6].strip() or None,
                    "source_file": path.name,
                    "line_no": line_no,
                    "block_id": current_block_id,
                }
            )
            continue

        if not current_block_id:
            continue
        for token in [x.strip() for x in text.split(",") if x.strip()]:
            market_digit = None
            symbol = token
            if "#" in token:
                market_digit, symbol = token.split("#", 1)
            market = market_from_digit(market_digit) or market_from_symbol(symbol)
            secid = make_secid(market, symbol if symbol and symbol.isdigit() and len(symbol) == 6 else None)
            mem_rows.append(
                {
                    "block_source": "infoharbor",
                    "block_id": current_block_id,
                    "block_code": current_code,
                    "block_name": current_name,
                    "member_market_digit": parse_int(market_digit),
                    "market": market,
                    "symbol": symbol or None,
                    "secid": secid,
                    "raw_member": token,
                    "source_file": path.name,
                    "line_no": line_no,
                }
            )
    return def_rows, mem_rows


def parse_simple_block(path: Path) -> tuple[list[dict], list[dict]]:
    def_rows: list[dict] = []
    mem_rows: list[dict] = []
    current_name: str | None = None
    current_id: str | None = None
    section = -1
    stem = path.stem.lower()

    for line_no, line in enumerate(text_lines(path), start=1):
        text = line.strip()
        if not text:
            continue
        if text.startswith("#"):
            section += 1
            current_name = text[1:].strip() or f"{stem}_{section:04d}"
            current_id = f"{stem}:{section:04d}"
            def_rows.append(
                {
                    "block_source": stem,
                    "block_kind": stem.upper(),
                    "block_name": current_name,
                    "block_code": current_id,
                    "member_count_hint": None,
                    "create_date": None,
                    "update_date": None,
                    "extra_1": None,
                    "extra_2": None,
                    "source_file": path.name,
                    "line_no": line_no,
                    "block_id": current_id,
                }
            )
            continue

        if not current_id:
            continue
        prefix = None
        code = text
        if "," in text:
            prefix, code = [x.strip() for x in text.split(",", 1)]
        symbol = code.strip()
        market = market_from_digit(prefix) if prefix else None
        if market is None and symbol.isdigit() and len(symbol) == 7 and symbol[0] in MARKET_BY_DIGIT:
            market = market_from_digit(symbol[0])
            symbol = symbol[1:]
        if market is None and symbol.isdigit() and len(symbol) == 6:
            market = market_from_symbol(symbol)
        secid = make_secid(market, symbol if symbol.isdigit() and len(symbol) == 6 else None)
        mem_rows.append(
            {
                "block_source": stem,
                "block_id": current_id,
                "block_code": current_id,
                "block_name": current_name,
                "member_market_digit": parse_int(prefix),
                "market": market,
                "symbol": symbol or None,
                "secid": secid,
                "raw_member": text,
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return def_rows, mem_rows


def parse_map_file(path: Path, map_name: str) -> list[dict]:
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        raw = line.strip()
        if len(raw) < 7:
            continue
        symbol = raw[:6]
        offset = parse_int(raw[6:].strip())
        market = market_from_symbol(symbol)
        rows.append(
            {
                "map_name": map_name,
                "symbol": symbol,
                "market": market,
                "secid": make_secid(market, symbol),
                "offset": offset,
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def parse_specgpext(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split("|")
        while len(parts) < 9:
            parts.append("")
        market_digit = parts[0].strip()
        symbol = parts[1].strip()
        if not symbol:
            continue
        market = market_from_digit(market_digit)
        rows.append(
            {
                "market_digit": parse_int(market_digit),
                "market": market,
                "symbol": symbol,
                "secid": make_secid(market, symbol),
                "business_summary": parts[2].strip() or None,
                "field_04": parts[3].strip() or None,
                "field_05": parts[4].strip() or None,
                "field_06": parts[5].strip() or None,
                "related_etf_code": parts[6].strip() or None,
                "related_weight": parse_float(parts[7]),
                "reserved": parts[8].strip() or None,
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def parse_specetfdata(path: Path) -> list[dict]:
    """解析 ETF 扩展元数据（specetfdata.txt，固定 8 列，逗号分隔）。"""
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split(",")
        if len(parts) != 8:
            continue
        market_digit = parts[0].strip()
        symbol = parts[1].strip()
        if not symbol:
            continue
        market = market_from_digit(market_digit) or market_from_symbol(symbol)
        list_date = parse_int(parts[6])
        first_trade_date = parse_int(parts[7])
        rows.append(
            {
                "market_digit": parse_int(market_digit),
                "market": market,
                "symbol": symbol,
                "secid": make_secid(market, symbol if symbol.isdigit() and len(symbol) == 6 else None),
                "tracking_code": parts[2].strip() or None,
                "tracking_market_digit": parse_int(parts[3]),
                "manager_code": parts[4].strip() or None,
                "reserved": parts[5].strip() or None,
                "list_date": list_date if list_date and validate_yyyymmdd(list_date) else None,
                "first_trade_date": (
                    first_trade_date
                    if first_trade_date and validate_yyyymmdd(first_trade_date)
                    else None
                ),
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def parse_speclofdata(path: Path) -> list[dict]:
    """解析 LOF 扩展元数据（speclofdata.txt，固定 6 列，逗号分隔）。"""
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split(",")
        if len(parts) != 6:
            continue
        market_digit = parts[0].strip()
        symbol = parts[1].strip()
        if not symbol:
            continue
        market = market_from_digit(market_digit) or market_from_symbol(symbol)
        rows.append(
            {
                "market_digit": parse_int(market_digit),
                "market": market,
                "symbol": symbol,
                "secid": make_secid(market, symbol if symbol.isdigit() and len(symbol) == 6 else None),
                "tracking_code": parts[2].strip() or None,
                "tracking_market_digit": parse_int(parts[3]),
                "manager_code": parts[4].strip() or None,
                "reserved": parts[5].strip() or None,
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def parse_specjjdata(path: Path) -> list[dict]:
    """解析基金快照数据（specjjdata.txt，固定 6 列，逗号分隔）。"""
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split(",")
        if len(parts) != 6:
            continue
        symbol = parts[0].strip()
        if not symbol:
            continue
        market_digit = parts[1].strip()
        market = market_from_digit(market_digit) or market_from_symbol(symbol)
        trade_date = parse_int(parts[3])
        rows.append(
            {
                "symbol": symbol,
                "market_digit": parse_int(market_digit),
                "market": market,
                "secid": make_secid(market, symbol if symbol.isdigit() and len(symbol) == 6 else None),
                "tracking_code": parts[2].strip() or None,
                "trade_date": trade_date if trade_date and validate_yyyymmdd(trade_date) else None,
                "metric_01": parse_float(parts[4]),
                "metric_02": parse_float(parts[5]),
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def parse_gbbq(path: Path) -> list[dict]:
    """解析通达信股本变迁(除权除息)二进制文件 T0002/hq_cache/gbbq。"""
    try:
        from pytdx.reader.gbbq_reader import GbbqReader
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency: pytdx. Install with: python -m pip install pytdx"
        ) from exc

    df = GbbqReader().get_df(str(path))
    rows: list[dict] = []
    if df is None or len(df) == 0:
        return rows

    for idx, record in enumerate(df.itertuples(index=False), start=1):
        market_digit = int(record.market)
        symbol = str(record.code).strip()
        ex_date = int(record.datetime)
        if not symbol or not validate_yyyymmdd(ex_date):
            continue
        market = market_from_digit(market_digit)
        rows.append(
            {
                "market_digit": market_digit,
                "market": market,
                "symbol": symbol,
                "secid": make_secid(market, symbol),
                "ex_date": ex_date,
                "category": int(record.category),
                "field_01": float(record.hongli_panqianliutong),
                "field_02": float(record.peigujia_qianzongguben),
                "field_03": float(record.songgu_qianzongguben),
                "field_04": float(record.peigu_houzongguben),
                "source_file": path.name,
                "line_no": idx,
            }
        )
    return rows


def parse_code2name(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line_no, line in enumerate(text_lines(path), start=1):
        parts = line.split(",")
        if len(parts) < 17:
            continue
        extra_desc = ""
        if len(parts) > 17:
            extra_desc = ",".join(parts[17:])
        rows.append(
            {
                "instrument_prefix": parts[0].strip() or None,
                "instrument_name": parts[1].strip() or None,
                "exchange_code": parts[2].strip() or None,
                "contract_type": parts[3].strip() or None,
                "contract_month": parts[4].strip() or None,
                "expire_date": parts[5].strip() or None,
                "multiplier": parse_float(parts[6]),
                "price_tick": parse_float(parts[7]),
                "fee_open": parse_float(parts[8]),
                "fee_close": parse_float(parts[9]),
                "margin_ratio": parse_float(parts[10]),
                "fee_unit": parts[11].strip() or None,
                "quote_unit": parts[12].strip() or None,
                "session_type": parts[13].strip() or None,
                "price_decimals": parse_int(parts[14]),
                "reserved_flag": parse_int(parts[15]),
                "delivery_rule": (parts[16] + ("," + extra_desc if extra_desc else "")).strip() or None,
                "source_file": path.name,
                "line_no": line_no,
            }
        )
    return rows


def replace_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    rows: list[dict],
) -> int:
    if not rows:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        return 0
    arrow_table = pa.Table.from_pylist(rows)
    relation_name = f"_table_{uuid4().hex[:10]}"
    con.register(relation_name, arrow_table)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {relation_name}")
    finally:
        con.unregister(relation_name)
    return arrow_table.num_rows


def file_signature(path: Path) -> dict[str, int]:
    st = path.stat()
    return {"size": int(st.st_size), "mtime_ns": int(st.st_mtime_ns)}


def reference_sources(tdx_root: Path) -> tuple[dict[str, Path], dict]:
    mapping: dict[str, Path] = {}
    sigs: dict[str, dict] = {}
    for rel in REFERENCE_SOURCE_FILES:
        abs_path = tdx_root / rel
        if not abs_path.exists():
            continue
        key = rel.replace("\\", "/")
        mapping[key] = abs_path
        sigs[key] = file_signature(abs_path)
    return mapping, sigs


def parse_source_manifest(tdx_root: Path, parsed_keys: set[str]) -> list[dict]:
    rows: list[dict] = []
    hq_root = tdx_root / "T0002" / "hq_cache"
    if not hq_root.exists():
        return rows
    for path in sorted(hq_root.iterdir()):
        if not path.is_file():
            continue
        rel = str(path.relative_to(tdx_root)).replace("\\", "/")
        st = path.stat()
        with path.open("rb") as handle:
            digest = hashlib.sha256(handle.read(4096)).hexdigest()
        rows.append(
            {
                "source_file": rel,
                "size": int(st.st_size),
                "mtime_ns": int(st.st_mtime_ns),
                "parsed": rel in parsed_keys,
                "sha256_head4096": digest,
            }
        )
    return rows


def process_reference_dataset(
    con: duckdb.DuckDBPyConnection,
    tdx_root: Path,
    state_section: dict[str, dict],
    full_rebuild: bool,
) -> tuple[dict, dict]:
    sources, current = reference_sources(tdx_root)
    parse_errors: list[str] = []

    unchanged = (not full_rebuild) and state_section == current
    if unchanged:
        return state_section, {
            "dataset": "reference",
            "source_files": len(current),
            "written_files": 0,
            "skipped_files": len(current),
            "rows_written": 0,
            "tables_written": {},
            "errors": [],
        }

    tables: dict[str, list[dict]] = {
        "security_master": [],
        "security_profile": [],
        "security_industry_map": [],
        "block_definition": [],
        "block_member": [],
        "index_snapshot": [],
        "security_business": [],
        "etf_meta": [],
        "lof_meta": [],
        "fund_nav_snapshot": [],
        "map_offsets": [],
        "derivatives_meta": [],
        "corporate_action": [],
    }

    parsed_keys: set[str] = set()

    try:
        tnf_inputs = [
            ("sh", "T0002/hq_cache/shs.tnf"),
            ("sz", "T0002/hq_cache/szs.tnf"),
            ("bj", "T0002/hq_cache/bjs.tnf"),
        ]
        for market, key in tnf_inputs:
            path = sources.get(key)
            if not path:
                continue
            parsed_keys.add(key)
            tables["security_master"].extend(parse_tnf(path, market))

        key = "T0002/hq_cache/base.dbf"
        if key in sources:
            parsed_keys.add(key)
            tables["security_profile"].extend(parse_base_dbf(sources[key]))

        key = "T0002/hq_cache/tdxhy.cfg"
        if key in sources:
            parsed_keys.add(key)
            tables["security_industry_map"].extend(parse_tdxhy(sources[key]))

        for key in ["T0002/hq_cache/tdxzs.cfg", "T0002/hq_cache/tdxzs3.cfg"]:
            if key in sources:
                parsed_keys.add(key)
                tables["block_definition"].extend(parse_tdxzs(sources[key]))

        key = "T0002/hq_cache/tdxzsbase.cfg"
        if key in sources:
            parsed_keys.add(key)
            tables["index_snapshot"].extend(parse_tdxzsbase(sources[key]))

        key = "T0002/hq_cache/infoharbor_block.dat"
        if key in sources:
            parsed_keys.add(key)
            defs, members = parse_infoharbor_block(sources[key])
            tables["block_definition"].extend(defs)
            tables["block_member"].extend(members)

        for key in [
            "T0002/hq_cache/csiblock.dat",
            "T0002/hq_cache/hkblock.dat",
            "T0002/hq_cache/jjblock.dat",
            "T0002/hq_cache/mgblock.dat",
            "T0002/hq_cache/sbblock.dat",
            "T0002/hq_cache/spblock.dat",
            "T0002/hq_cache/ukblock.dat",
            "T0002/hq_cache/sgxblock.dat",
        ]:
            if key not in sources:
                continue
            parsed_keys.add(key)
            defs, members = parse_simple_block(sources[key])
            tables["block_definition"].extend(defs)
            tables["block_member"].extend(members)

        for map_name, key in [
            ("base", "T0002/hq_cache/base.map"),
            ("gbbq", "T0002/hq_cache/gbbq.map"),
        ]:
            if key in sources:
                parsed_keys.add(key)
                tables["map_offsets"].extend(parse_map_file(sources[key], map_name))

        key = "T0002/hq_cache/specgpext.txt"
        if key in sources:
            parsed_keys.add(key)
            tables["security_business"].extend(parse_specgpext(sources[key]))

        key = "T0002/hq_cache/specetfdata.txt"
        if key in sources:
            parsed_keys.add(key)
            tables["etf_meta"].extend(parse_specetfdata(sources[key]))

        key = "T0002/hq_cache/speclofdata.txt"
        if key in sources:
            parsed_keys.add(key)
            tables["lof_meta"].extend(parse_speclofdata(sources[key]))

        key = "T0002/hq_cache/specjjdata.txt"
        if key in sources:
            parsed_keys.add(key)
            tables["fund_nav_snapshot"].extend(parse_specjjdata(sources[key]))

        key = "T0002/hq_cache/code2name.ini"
        if key in sources:
            parsed_keys.add(key)
            tables["derivatives_meta"].extend(parse_code2name(sources[key]))

        key = "T0002/hq_cache/gbbq"
        if key in sources:
            parsed_keys.add(key)
            tables["corporate_action"].extend(parse_gbbq(sources[key]))
    except Exception as exc:  # noqa: BLE001
        parse_errors.append(f"reference parse failed: {exc}")

    table_rows: dict[str, int] = {}
    rows_written = 0
    written_files = 0

    if not parse_errors:
        for table_name, rows in tables.items():
            count = replace_table(con, table_name, rows)
            table_rows[table_name] = count
            rows_written += count
            written_files += 1
        manifest_rows = parse_source_manifest(tdx_root, parsed_keys)
        manifest_count = replace_table(con, "source_manifest", manifest_rows)
        table_rows["source_manifest"] = manifest_count
        rows_written += manifest_count
        written_files += 1

    summary = {
        "dataset": "reference",
        "source_files": len(current),
        "written_files": written_files,
        "skipped_files": 0 if written_files else len(current),
        "rows_written": rows_written,
        "tables_written": table_rows,
        "errors": parse_errors,
    }
    return current, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Incrementally sync TongDaXin bars and reference data into a single DuckDB database file. "
            "Bars: vipdoc/*/lday/*.day and vipdoc/*/fzline/*.lc5; "
            "Reference: T0002/hq_cache."
        )
    )
    parser.add_argument(
        "--tdx-root",
        required=True,
        help="TongDaXin root directory, e.g. C:\\new_tdx64",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Output directory for the DuckDB database, state, runner, and summary files.",
    )
    parser.add_argument(
        "--datasets",
        default="daily,min5,reference",
        help="Comma-separated datasets: daily,min5,reference",
    )
    parser.add_argument(
        "--markets",
        default="sh,sz,bj",
        help="Comma-separated markets for bar datasets: sh,sz,bj",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=300_000,
        help="Rows per bar write batch.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Ignore state and rebuild selected datasets from source files.",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help="Optional state file path. Default: <output-root>/_state/tdx_sync_state.json",
    )
    parser.add_argument(
        "--fail-on-reset",
        action="store_true",
        help="Fail when bar source file record count shrinks compared with saved state.",
    )
    parser.add_argument(
        "--keep-stale-state",
        action="store_true",
        help="Do not prune deleted/missing bar source files from state.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="For debug only: process at most N files per bar dataset.",
    )
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional path to write machine-readable run summary JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tdx_root = Path(args.tdx_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    db_path = database_path(output_root)
    con = connect_database(db_path)

    valid_datasets = set(DATASETS.keys()) | {"reference"}
    requested_datasets = [x.strip().lower() for x in args.datasets.split(",") if x.strip()]
    datasets = [name for name in requested_datasets if name in valid_datasets]
    invalid = [name for name in requested_datasets if name not in valid_datasets]
    if invalid:
        raise SystemExit(f"Unsupported datasets: {','.join(invalid)}")
    if not datasets:
        raise SystemExit("No datasets selected.")

    markets = [x.strip().lower() for x in args.markets.split(",") if x.strip()]
    if not markets:
        raise SystemExit("No markets selected.")

    state_path = (
        Path(args.state_file).expanduser().resolve()
        if args.state_file
        else output_root / "_state" / "tdx_sync_state.json"
    )

    state = default_state() if args.full_rebuild else load_state(state_path)
    run_id = uuid4().hex[:12]
    run_started = utc_now_iso()

    print(
        f"Start sync run_id={run_id} datasets={','.join(datasets)} markets={','.join(markets)}",
        flush=True,
    )

    overall_summaries: list[dict] = []
    total_rows = 0
    total_files = 0
    total_errors = 0

    try:
        for dataset_name in datasets:
            if dataset_name in DATASETS:
                spec = DATASETS[dataset_name]
                section = state["sources"].get(dataset_name, {})
                next_section, summary = process_bar_dataset(
                    spec=spec,
                    con=con,
                    tdx_root=tdx_root,
                    state_section=section,
                    markets=markets,
                    batch_size=args.batch_size,
                    full_rebuild=args.full_rebuild,
                    fail_on_reset=args.fail_on_reset,
                    max_files=args.max_files,
                    prune_state=not args.keep_stale_state,
                )
            else:
                section = state["sources"].get("reference", {})
                next_section, summary = process_reference_dataset(
                    con=con,
                    tdx_root=tdx_root,
                    state_section=section,
                    full_rebuild=args.full_rebuild,
                )
            state["sources"][dataset_name] = next_section
            overall_summaries.append(summary)
            total_rows += int(summary.get("rows_written", 0))
            total_files += int(summary.get("written_files", 0))
            total_errors += len(summary.get("errors", []))
            print(
                f"[{dataset_name}] source_files={summary.get('source_files', 0)} "
                f"written_files={summary.get('written_files', 0)} "
                f"rows_written={summary.get('rows_written', 0)} "
                f"errors={len(summary.get('errors', []))}",
                flush=True,
            )

        ensure_indexes(con)
        con.close()
    except Exception:
        con.close()
        raise

    state["updated_at"] = utc_now_iso()
    save_state(state_path, state)

    result = {
        "run_id": run_id,
        "started_at": run_started,
        "finished_at": utc_now_iso(),
        "tdx_root": str(tdx_root),
        "output_root": str(output_root),
        "database_path": str(db_path),
        "state_file": str(state_path),
        "datasets": overall_summaries,
        "totals": {
            "files_written": total_files,
            "rows_written": total_rows,
            "error_count": total_errors,
        },
    }
    if args.summary_json:
        summary_path = Path(args.summary_json).expanduser().resolve()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with summary_path.open("w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=True, indent=2, sort_keys=True)

    print(
        f"Done run_id={run_id} files_written={total_files} rows_written={total_rows} "
        f"errors={total_errors}",
        flush=True,
    )
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
