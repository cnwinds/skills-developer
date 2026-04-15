#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import struct
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

try:
    import pyarrow as pa
    import pyarrow.dataset as ds
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: pyarrow. Install with: python -m pip install pyarrow"
    ) from exc


RECORD_SIZE = 32
DAY_RECORD = struct.Struct("<IIIIIfII")
LC5_RECORD = struct.Struct("<HHfffffii")


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
    return {"version": 1, "updated_at": None, "sources": {"daily": {}, "min5": {}}}


def load_state(path: Path) -> dict:
    if not path.exists():
        return default_state()
    with path.open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    if not isinstance(loaded, dict):
        return default_state()
    if "sources" not in loaded or not isinstance(loaded["sources"], dict):
        loaded["sources"] = {"daily": {}, "min5": {}}
    loaded["sources"].setdefault("daily", {})
    loaded["sources"].setdefault("min5", {})
    loaded.setdefault("version", 1)
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


def flush_buffer(
    buffer: Buffer,
    schema: pa.Schema,
    output_dir: Path,
    run_id: str,
    flush_index: int,
) -> int:
    rows = len(buffer)
    if rows == 0:
        return 0
    output_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(buffer.data, schema=schema)
    ds.write_dataset(
        data=table,
        base_dir=str(output_dir),
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema([("market", pa.string()), ("trade_date", pa.int32())]),
            flavor="hive",
        ),
        max_partitions=20000,
        existing_data_behavior="overwrite_or_ignore",
        basename_template=f"part-{run_id}-{flush_index}-{{i}}.parquet",
    )
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


def process_dataset(
    spec: DatasetSpec,
    tdx_root: Path,
    output_root: Path,
    state_section: dict[str, dict],
    run_id: str,
    markets: list[str],
    batch_size: int,
    full_rebuild: bool,
    fail_on_reset: bool,
    max_files: int | None,
    prune_state: bool,
) -> tuple[dict, dict]:
    files = list_source_files(tdx_root, markets, spec, max_files)
    output_dir = output_root / spec.output_name
    buffer = Buffer(spec.columns)
    next_state = dict(state_section)
    seen_keys: set[str] = set()
    touched_dates: set[int] = set()
    flush_index = 0
    written_rows = 0
    written_files = 0
    skipped_files = 0
    parse_errors: list[str] = []

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

        try:
            if spec.name == "daily":
                for trade_date, op, hi, lo, cl, amount, volume in spec.parser(raw, start_index):
                    touched_dates.add(trade_date)
                    buffer.add(
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
                    if len(buffer) >= batch_size:
                        flush_index += 1
                        written_rows += flush_buffer(
                            buffer, spec.schema, output_dir, run_id, flush_index
                        )
            else:
                for trade_date, bar_time, op, hi, lo, cl, amount, volume in spec.parser(
                    raw, start_index
                ):
                    touched_dates.add(trade_date)
                    buffer.add(
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
                    if len(buffer) >= batch_size:
                        flush_index += 1
                        written_rows += flush_buffer(
                            buffer, spec.schema, output_dir, run_id, flush_index
                        )
        except Exception as exc:  # noqa: BLE001
            parse_errors.append(f"{spec.name}: parse failed for {file_path}: {exc}")
            continue

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
        flush_index += 1
        written_rows += flush_buffer(buffer, spec.schema, output_dir, run_id, flush_index)

    if prune_state:
        next_state = {k: v for k, v in next_state.items() if k in seen_keys}

    summary = {
        "dataset": spec.name,
        "source_files": len(files),
        "written_files": written_files,
        "skipped_files": skipped_files,
        "rows_written": written_rows,
        "touched_trade_dates": sorted(touched_dates),
        "errors": parse_errors,
    }
    return next_state, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incrementally sync TongDaXin .day/.lc5 bars to Parquet datasets."
    )
    parser.add_argument(
        "--tdx-root",
        required=True,
        help="TongDaXin root directory, e.g. C:\\new_tdx64",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Output directory for parquet datasets and state.",
    )
    parser.add_argument(
        "--datasets",
        default="daily,min5",
        help="Comma-separated datasets: daily,min5",
    )
    parser.add_argument(
        "--markets",
        default="sh,sz,bj",
        help="Comma-separated markets: sh,sz,bj",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=300_000,
        help="Rows per parquet write batch.",
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
        help="Fail when source file record count shrinks compared with saved state.",
    )
    parser.add_argument(
        "--keep-stale-state",
        action="store_true",
        help="Do not prune deleted/missing source files from state.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="For debug only: process at most N files per dataset.",
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

    requested_datasets = [x.strip().lower() for x in args.datasets.split(",") if x.strip()]
    datasets = [name for name in requested_datasets if name in DATASETS]
    invalid = [name for name in requested_datasets if name not in DATASETS]
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

    for dataset_name in datasets:
        spec = DATASETS[dataset_name]
        section = state["sources"].get(dataset_name, {})
        next_section, summary = process_dataset(
            spec=spec,
            tdx_root=tdx_root,
            output_root=output_root,
            state_section=section,
            run_id=run_id,
            markets=markets,
            batch_size=args.batch_size,
            full_rebuild=args.full_rebuild,
            fail_on_reset=args.fail_on_reset,
            max_files=args.max_files,
            prune_state=not args.keep_stale_state,
        )
        state["sources"][dataset_name] = next_section
        overall_summaries.append(summary)
        total_rows += int(summary["rows_written"])
        total_files += int(summary["written_files"])
        total_errors += len(summary["errors"])
        print(
            f"[{dataset_name}] source_files={summary['source_files']} "
            f"written_files={summary['written_files']} "
            f"rows_written={summary['rows_written']} "
            f"errors={len(summary['errors'])}",
            flush=True,
        )

    state["updated_at"] = utc_now_iso()
    save_state(state_path, state)

    result = {
        "run_id": run_id,
        "started_at": run_started,
        "finished_at": utc_now_iso(),
        "tdx_root": str(tdx_root),
        "output_root": str(output_root),
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
