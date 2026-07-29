#!/usr/bin/env python3
"""Analyze an offline Kafka value export without connecting to Kafka or a database.

Supports pipe-delimited text (the default) and JSON Lines. It reports message-size,
throughput, token, key-cardinality, duplicate-event, fixed-window, and application
batch statistics. The script is intentionally dependency-free.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def number(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(str(value).strip())
        return int(parsed) if parsed.is_integer() else parsed
    except (TypeError, ValueError):
        return None


def get_path(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def parse_spec(spec: str | None) -> list[str]:
    return [item.strip() for item in (spec or "").split(",") if item.strip()]


def parse_timestamp(value: Any, unit: str) -> float | None:
    parsed = number(value)
    if parsed is None:
        return None
    if unit == "ms":
        return float(parsed) / 1000
    if unit == "us":
        return float(parsed) / 1_000_000
    return float(parsed)


def open_input(path: str) -> TextIO:
    if path == "-":
        return sys.stdin
    file_path = Path(path)
    if str(file_path).endswith(".gz"):
        return gzip.open(file_path, "rt", encoding="utf-8", errors="replace")
    return file_path.open("r", encoding="utf-8", errors="replace")


def parse_line(
    line: str,
    fmt: str,
    delimiter: str,
) -> tuple[Any, bytes] | tuple[None, bytes]:
    raw = line.rstrip("\r\n").encode("utf-8")
    if fmt == "pipe":
        return line.rstrip("\r\n").split(delimiter), raw
    try:
        return json.loads(line), raw
    except json.JSONDecodeError:
        return None, raw


def read_records(args: argparse.Namespace) -> Iterable[tuple[Any, bytes]]:
    with open_input(args.input) as stream:
        for line in stream:
            if line.strip():
                yield parse_line(line, args.format, args.delimiter)


def value(record: Any, spec: str | None) -> Any:
    return get_path(record, spec) if spec else None


def window_start(timestamp: float, window_seconds: int) -> int:
    return int(timestamp // window_seconds) * window_seconds


def iso(timestamp: float | int | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(float(timestamp), tz=timezone.utc).isoformat()


def stats(values: list[float]) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values) if values else None,
        "mean": statistics.fmean(values) if values else None,
    }


def analyze(args: argparse.Namespace) -> dict[str, Any]:
    timestamp_values: list[float] = []
    message_sizes: list[float] = []
    input_tokens: list[float] = []
    output_tokens: list[float] = []
    event_ids: Counter[str] = Counter()
    keys: set[str] = set()
    window_keys: defaultdict[int, set[str]] = defaultdict(set)
    batches: list[dict[str, Any]] = []
    current_batch: list[tuple[str, int, float | None]] = []
    invalid = 0
    total = 0
    total_bytes = 0
    delimiter = args.delimiter

    def flush_batch() -> None:
        if not current_batch:
            return
        batches.append(
            {
                "messages": len(current_batch),
                "bytes": sum(item[1] for item in current_batch),
                "unique_keys": len({item[0] for item in current_batch}),
                "input_tokens": sum(item[2] or 0 for item in current_batch),
            }
        )
        current_batch.clear()

    for record, raw in read_records(args):
        total += 1
        total_bytes += len(raw)
        message_sizes.append(len(raw))
        if record is None:
            invalid += 1
            continue

        timestamp = parse_timestamp(value(record, args.timestamp), args.timestamp_unit)
        if timestamp is not None:
            timestamp_values.append(timestamp)

        fields = [str(value(record, item) or "") for item in args.key_fields]
        key = args.key_separator.join(fields)
        if not key.strip(args.key_separator):
            key = "<missing-key>"
        keys.add(key)
        if timestamp is not None:
            window_keys[window_start(timestamp, args.window_seconds)].add(key)

        event_id_value = value(record, args.event_id)
        if event_id_value not in (None, ""):
            event_ids[str(event_id_value)] += 1

        input_value = number(value(record, args.input_tokens))
        output_value = number(value(record, args.output_tokens))
        if input_value is not None:
            input_tokens.append(float(input_value))
        if output_value is not None:
            output_tokens.append(float(output_value))

        current_batch.append((key, len(raw), float(input_value) if input_value is not None else None))
        if len(current_batch) >= args.batch_size:
            flush_batch()
    flush_batch()

    duration = max(timestamp_values) - min(timestamp_values) if len(timestamp_values) >= 2 else 0
    duplicate_events = sum(count - 1 for count in event_ids.values() if count > 1)
    windows = [
        {
            "window_start": iso(start),
            "window_end": iso(start + args.window_seconds),
            "unique_keys": len(window_key_set),
        }
        for start, window_key_set in sorted(window_keys.items())
    ]
    observed_qps = (total / duration) if duration > 0 else None
    report = {
        "input": args.input,
        "format": args.format,
        "delimiter": delimiter if args.format == "pipe" else None,
        "total_messages": total,
        "invalid_records": invalid,
        "total_payload_bytes": total_bytes,
        "message_size_bytes": stats(message_sizes),
        "event_time": {
            "first": iso(min(timestamp_values)) if timestamp_values else None,
            "last": iso(max(timestamp_values)) if timestamp_values else None,
            "duration_seconds": duration,
            "observed_qps": observed_qps,
        },
        "tokens": {
            "input": {"sum": sum(input_tokens), **stats(input_tokens)},
            "output": {"sum": sum(output_tokens), **stats(output_tokens)},
        },
        "keys": {
            "unique_over_file": len(keys),
            "unique_per_fixed_window": windows,
        },
        "duplicates": {
            "event_id_field": args.event_id,
            "event_ids_seen": len(event_ids),
            "duplicate_records": duplicate_events,
        },
        "application_batches": {
            "batch_size": args.batch_size,
            "count": len(batches),
            "messages": stats([float(item["messages"]) for item in batches]),
            "bytes": stats([float(item["bytes"]) for item in batches]),
            "unique_keys": stats([float(item["unique_keys"]) for item in batches]),
            "samples": batches[: args.max_batch_samples],
        },
        "fetch_estimates": {
            "configured_max_partition_fetch_bytes": args.max_partition_fetch_bytes,
            "estimated_records_per_fetch_at_p50": (
                args.max_partition_fetch_bytes / percentile(message_sizes, 0.50)
                if message_sizes and percentile(message_sizes, 0.50)
                else None
            ),
            "estimated_records_per_fetch_at_p95": (
                args.max_partition_fetch_bytes / percentile(message_sizes, 0.95)
                if message_sizes and percentile(message_sizes, 0.95)
                else None
            ),
            "estimated_bytes_for_application_batch": args.batch_size * (statistics.fmean(message_sizes) if message_sizes else 0),
        },
    }
    return report


def print_summary(report: dict[str, Any]) -> None:
    event_time = report["event_time"]
    sizes = report["message_size_bytes"]
    batches = report["application_batches"]
    print(f"messages={report['total_messages']:,} invalid={report['invalid_records']:,}")
    print(f"payload_bytes={report['total_payload_bytes']:,} observed_qps={event_time['observed_qps']}")
    print(f"message_bytes p50/p95/p99/max={sizes['p50']}/{sizes['p95']}/{sizes['p99']}/{sizes['max']}")
    print(f"unique_keys={report['keys']['unique_over_file']:,} duplicate_event_records={report['duplicates']['duplicate_records']:,}")
    print(f"batches={batches['count']:,} batch_unique_keys_p50/p95={batches['unique_keys']['p50']}/{batches['unique_keys']['p95']}")
    fetch = report["fetch_estimates"]
    print(f"estimated_records_per_{fetch['configured_max_partition_fetch_bytes']}B_fetch_p50/p95={fetch['estimated_records_per_fetch_at_p50']}/{fetch['estimated_records_per_fetch_at_p95']}")
    print(f"estimated_bytes_for_{batches['batch_size']}_records={fetch['estimated_bytes_for_application_batch']:.0f}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="消息导出文件、.gz文件，或-表示stdin")
    parser.add_argument("--format", choices=("pipe", "jsonl"), default="pipe")
    parser.add_argument("--delimiter", default="|", help="pipe格式分隔符，默认|")
    parser.add_argument("--timestamp", default="0", help="时间戳字段索引或JSON路径，默认0")
    parser.add_argument("--timestamp-unit", choices=("s", "ms", "us"), default="ms")
    parser.add_argument("--key-fields", default="1,2,3,4,5,6,7,8,9", help="唯一key字段索引/路径，逗号分隔")
    parser.add_argument("--key-separator", default="|")
    parser.add_argument("--event-id", default=None, help="事件ID字段索引/JSON路径；不传则不统计事件ID重复")
    parser.add_argument("--input-tokens", default="10", help="输入Token字段索引/JSON路径，默认10")
    parser.add_argument("--output-tokens", default="11", help="输出Token字段索引/JSON路径，默认11")
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--max-partition-fetch-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--max-batch-samples", type=int, default=20)
    parser.add_argument("--json-output", help="将完整报告写入JSON文件")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.key_fields = parse_spec(args.key_fields)
    report = analyze(args)
    print_summary(report)
    if args.json_output:
        output = Path(args.json_output)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"json_report={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
