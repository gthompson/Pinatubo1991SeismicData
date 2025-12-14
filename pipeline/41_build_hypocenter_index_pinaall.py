#!/usr/bin/env python3
"""
41_build_hypocenter_index_pinaall.py

STEP 41 of the Pinatubo FAIR pipeline

Build a tolerant hypocenter index from PINAALL.DAT,
emitting the SAME core columns as STEP 05 so exact
row-by-row comparison is possible.

Core output columns (matched to STEP 05):
-----------------------------------------
origin_time   (ISO-8601, UTC, no fractional seconds unless present)
latitude
longitude
depth_km
magnitude

Additional provenance columns are preserved.

This step:
  • parses origin time, location, depth, magnitude
  • tolerates spacing variation
  • handles minute / second rollover
  • preserves provenance and raw text
  • logs unparsed lines (with reason)
  • DOES NOT create QuakeML
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from obspy import UTCDateTime

PARSER_ID = "pinaall_dat_v3"


# =============================================================================
# Helpers: tolerant date/time parsing
# =============================================================================

@dataclass
class DTParseResult:
    dt: datetime
    next_index: int


def _split_mmdd(tok: str) -> Optional[Tuple[int, int]]:
    if not tok.isdigit():
        return None
    if len(tok) == 3:
        m = int(tok[0])
        d = int(tok[1:])
    elif len(tok) == 4:
        m = int(tok[:2])
        d = int(tok[2:])
    else:
        return None
    if 1 <= m <= 12 and 1 <= d <= 31:
        return m, d
    return None


def _parse_hhmm(tok: str) -> Optional[Tuple[int, int]]:
    if not tok.isdigit():
        return None
    v = int(tok)
    if len(tok) in (3, 4):
        h = v // 100
        mi = v % 100
        if 0 <= h <= 23 and 0 <= mi <= 99:
            return h, mi
    return None


def _normalize_minute_overflow(h: int, mi: int, sec: float, base: datetime) -> datetime:
    extra_h, mi2 = divmod(mi, 60)
    dt0 = base.replace(hour=h, minute=0, second=0, microsecond=0)
    return dt0 + timedelta(hours=extra_h, minutes=mi2, seconds=sec)


def _looks_like_number(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False


def parse_datetime_tokens(tokens: List[str], i0: int = 0) -> DTParseResult:
    i = i0
    if i + 3 >= len(tokens):
        raise ValueError("Not enough tokens for datetime")

    yy = int(tokens[i]); i += 1
    year = 1900 + yy if yy < 100 else yy

    md = _split_mmdd(tokens[i])
    if md is not None:
        month, day = md
        i += 1
    else:
        month = int(tokens[i])
        day = int(tokens[i + 1])
        i += 2

    if (
        i + 2 < len(tokens)
        and tokens[i].isdigit()
        and tokens[i + 1].isdigit()
        and _looks_like_number(tokens[i + 2])
    ):
        h = int(tokens[i])
        mi = int(tokens[i + 1])
        sec = float(tokens[i + 2])
        i += 3
    else:
        hm = _parse_hhmm(tokens[i])
        if hm is None:
            raise ValueError(f"Bad HHMM token: {tokens[i]!r}")
        h, mi = hm
        sec = float(tokens[i + 1])
        i += 2

    base = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
    dt = _normalize_minute_overflow(h, mi, sec, base)
    return DTParseResult(dt=dt, next_index=i)


# =============================================================================
# Helpers: tolerant lat/lon parsing
# =============================================================================

LATLON_ONE_TOKEN_RE = re.compile(r"^(?P<deg>\d+)(?P<hem>[NnSsEeWw])(?P<min>\d+(\.\d+)?)$")
DEG_HEM_RE = re.compile(r"^(?P<deg>\d+)(?P<hem>[NnSsEeWw])$")
HEM_ONLY_RE = re.compile(r"^(?P<hem>[NnSsEeWw])$")


def _hem_sign(hem: str) -> int:
    return -1 if hem.upper() in ("S", "W") else 1


def parse_latlon_from_tokens(tokens: List[str], i: int) -> Tuple[float, int, str]:
    t0 = tokens[i]

    m = LATLON_ONE_TOKEN_RE.match(t0)
    if m:
        deg = float(m.group("deg"))
        mins = float(m.group("min"))
        hem = m.group("hem")
        val = _hem_sign(hem) * (deg + mins / 60.0)
        return val, i + 1, t0

    m = DEG_HEM_RE.match(t0)
    if m:
        mins = float(tokens[i + 1])
        deg = float(m.group("deg"))
        hem = m.group("hem")
        val = _hem_sign(hem) * (deg + mins / 60.0)
        return val, i + 2, f"{t0} {tokens[i+1]}"

    if t0.isdigit():
        hem = tokens[i + 1]
        mins = float(tokens[i + 2])
        val = _hem_sign(hem) * (float(t0) + mins / 60.0)
        return val, i + 3, f"{t0} {hem} {mins}"

    raise ValueError(f"Cannot parse lat/lon at token {i}")


def parse_depth_mag_from_tokens(tokens: List[str], i: int) -> Tuple[Optional[float], Optional[float], int]:
    nums: List[float] = []
    j = i
    while j < len(tokens) and len(nums) < 2:
        if _looks_like_number(tokens[j]):
            nums.append(float(tokens[j]))
        j += 1

    depth = nums[0] if len(nums) >= 1 else None
    mag = nums[1] if len(nums) >= 2 else None
    return depth, mag, j


# =============================================================================
# Line parser
# =============================================================================

def parse_pinaall_line(line: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
    tokens = line.strip().split()
    if len(tokens) < 6:
        return None, "too_few_tokens"

    try:
        dt_res = parse_datetime_tokens(tokens)
        dt = dt_res.dt
        i = dt_res.next_index
    except Exception as e:
        return None, f"datetime_failed: {e}"

    try:
        lat, i, _ = parse_latlon_from_tokens(tokens, i)
        lon, i, _ = parse_latlon_from_tokens(tokens, i)
    except Exception as e:
        return None, f"latlon_failed: {e}"

    depth_km, mag, _ = parse_depth_mag_from_tokens(tokens, i)

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None, "latlon_out_of_range"

    origin_time = UTCDateTime(dt).isoformat()

    return {
        "origin_time": origin_time,
        "latitude": lat,
        "longitude": lon,
        "depth_km": depth_km,
        "magnitude": mag,
    }, None


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="STEP 41: Build hypocenter index from PINAALL.DAT"
    )
    ap.add_argument("--pinaall-file", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--error-log", required=True)
    args = ap.parse_args()

    src = Path(args.pinaall_file)
    rows: List[Dict[str, object]] = []
    unparsed: List[str] = []

    with src.open("r", errors="ignore") as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.rstrip("\n")
            if not line.strip():
                continue

            parsed, err = parse_pinaall_line(line)
            if parsed is None:
                unparsed.append(f"{lineno}: {err}: {raw}")
                continue

            parsed.update({
                "source_file": src.name,
                "source_line": lineno,
                "parser": PARSER_ID,
                "raw_line": line,
            })
            rows.append(parsed)

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    err_path = Path(args.error_log)
    err_path.parent.mkdir(parents=True, exist_ok=True)
    err_path.write_text("\n".join(unparsed))

    print("\nSTEP 41 — PINAALL.DAT HYPOCENTER INDEX BUILT")
    print("-------------------------------------------")
    print(f"Source file:     {src}")
    print(f"Parsed rows:     {len(rows)}")
    print(f"Unparsed lines:  {len(unparsed)}")
    print(f"Index CSV:       {out_csv}")
    print(f"Error log:       {err_path}")


if __name__ == "__main__":
    main()