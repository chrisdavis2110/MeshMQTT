"""Shared CLI flags and helpers for MQTT data_log*.jsonl tools."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple


def parse_date_string(s: str) -> date:
    """
    Parse a calendar date: 1-1-2026, 3-30-2026, 01/01/2026, 2026-01-01 (use / or -).
    If the first part is a 4-digit year, order is Y-M-D; otherwise M-D-Y (US).
    """
    s = s.strip()
    sep = "/" if "/" in s else "-"
    if sep not in s:
        raise ValueError(f"Use - or / in date: {s!r}")
    parts = [p.strip() for p in s.split(sep) if p.strip()]
    if len(parts) != 3:
        raise ValueError(f"Expected 3 date parts in {s!r}")
    a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
    if len(parts[0]) == 4:
        y, mo, d = a, b, c
    else:
        mo, d, y = a, b, c
    return date(y, mo, d)


def parse_log_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        s = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


def entry_in_date_range(
    entry_timestamp: Optional[str],
    start: Optional[date],
    end: Optional[date],
) -> bool:
    """If start/end are None, that side is unbounded. Missing timestamp fails when any bound is set."""
    if start is None and end is None:
        return True
    ts = parse_log_timestamp(entry_timestamp)
    if ts is None:
        return False
    d = ts.date()
    if start is not None and d < start:
        return False
    if end is not None and d > end:
        return False
    return True


def validate_date_range(start: Optional[date], end: Optional[date]) -> Optional[str]:
    if start and end and start > end:
        return "--start-date must be on or before --end-date"
    return None


def add_standard_log_args(
    parser: argparse.ArgumentParser,
    *,
    include_log_files: bool = True,
    legacy_repeatable_log: bool = False,
) -> None:
    parser.add_argument(
        "--log-dir",
        default="mqtt_logs",
        help="Directory containing data_log*.jsonl (default: mqtt_logs)",
    )
    parser.add_argument(
        "--region",
        default="",
        metavar="NAME",
        help="Regional logs: data_log_{region}.jsonl and data_log_{region}_*.jsonl",
    )
    parser.add_argument(
        "--start-date",
        default="",
        metavar="DATE",
        help="Only include entries on or after this date (e.g. 1-1-2026 or 2026-01-01)",
    )
    parser.add_argument(
        "--end-date",
        default="",
        metavar="DATE",
        help="Only include entries on or before this date",
    )
    if include_log_files:
        parser.add_argument(
            "--log-files",
            nargs="*",
            default=None,
            metavar="PATH",
            help="Explicit log file paths (overrides --log-dir / --region)",
        )
    if legacy_repeatable_log:
        parser.add_argument(
            "--log",
            action="append",
            dest="_log_repeat",
            default=None,
            help="Log file (repeatable); same as listing paths after --log-files",
        )


def resolve_data_log_files(args: argparse.Namespace) -> List[Path]:
    """Use explicit paths if given; else glob under --log-dir with optional --region."""
    extra: List[str] = []
    if getattr(args, "log_files", None):
        extra.extend(args.log_files)
    rep = getattr(args, "_log_repeat", None)
    if rep:
        extra.extend(rep)
    if extra:
        return [Path(p) for p in extra]
    log_dir = Path(args.log_dir)
    region = (getattr(args, "region", None) or "").strip().lower()
    if region:
        log_files = sorted(log_dir.glob(f"data_log_{region}.jsonl"))
        log_files += sorted(log_dir.glob(f"data_log_{region}_*.jsonl"))
        return sorted(set(log_files))
    return sorted(log_dir.glob("data_log*.jsonl"))


def resolve_log_paths_with_optional_single(
    args: argparse.Namespace,
    *,
    single_log_attr: str = "log",
) -> List[Path]:
    """If `single_log_attr` is set on args, use that one file; else `resolve_data_log_files`."""
    single = getattr(args, single_log_attr, None)
    if single:
        return [Path(single)]
    return resolve_data_log_files(args)


def parse_cli_date_range(args: argparse.Namespace) -> Tuple[Optional[date], Optional[date], Optional[str]]:
    """Returns (start, end, error_message)."""
    start: Optional[date] = None
    end: Optional[date] = None
    sd = (getattr(args, "start_date", None) or "").strip()
    ed = (getattr(args, "end_date", None) or "").strip()
    if sd:
        try:
            start = parse_date_string(sd)
        except ValueError as e:
            return None, None, f"Invalid --start-date: {e}"
    if ed:
        try:
            end = parse_date_string(ed)
        except ValueError as e:
            return None, None, f"Invalid --end-date: {e}"
    err = validate_date_range(start, end)
    if err:
        return None, None, err
    return start, end, None
