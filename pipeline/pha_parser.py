#!/usr/bin/env python3
"""
pha_parser.py

Shared parsing utilities for PHA files (monthly and individual formats).

Public API (expected by pipeline):
  - parse_phase_line(line) -> list[dict] | None
  - parse_pha_file(path, errors=None) -> list[{"origin_time": UTCDateTime, "picks": list[dict]}]
  - parse_individual_pha_file(path) -> list[dict]

Additional utilities used by pipeline steps:
  - filter_pick_outliers(picks, max_span_seconds=60.0) -> list[dict]
  - filter_pick_group(picks, max_span_seconds=60.0, ...) -> list[dict]
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np
from obspy import UTCDateTime


# =============================================================================
# Helpers
# =============================================================================

def _utc_median(times: Sequence[UTCDateTime]) -> Optional[UTCDateTime]:
    if not times:
        return None
    vals = [t.timestamp for t in times]
    return UTCDateTime(float(np.median(vals)))


def _safe_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except Exception:
        return None


def _parse_yyMMddHHMMSS_frac(ts: str) -> Optional[UTCDateTime]:
    """
    Parse YYMMDDHHMMSS(.ffffff) or YYMMDDHHMMSSffffff-ish strings.
    """
    ts = ts.strip()
    if not ts:
        return None

    # common: YYMMDDHHMMSS(.ff...)
    if "." in ts:
        base, frac = ts.split(".", 1)
        try:
            dt = datetime.strptime(base[:12], "%y%m%d%H%M%S")
            frac_val = float("0." + "".join([c for c in frac if c.isdigit()]))
            return UTCDateTime(dt) + frac_val
        except Exception:
            return None

    # no dot: just YYMMDDHHMMSS
    try:
        dt = datetime.strptime(ts[:12], "%y%m%d%H%M%S")
        return UTCDateTime(dt)
    except Exception:
        return None


def _dedupe_picks(picks: List[dict], time_eps_s: float = 1e-3) -> List[dict]:
    """
    Remove duplicates by (seed_id, phase, time within eps).
    Keeps first occurrence.
    """
    if not picks:
        return []

    # sort for stable behavior
    picks_sorted = sorted(picks, key=lambda p: (p.get("seed_id", ""), p.get("phase", ""), float(p["time"])))
    out: List[dict] = []
    last_by_key: dict[Tuple[str, str], UTCDateTime] = {}

    for p in picks_sorted:
        sid = p.get("seed_id") or ""
        ph = p.get("phase") or ""
        key = (sid, ph)
        t = p["time"]
        if key in last_by_key and abs(t - last_by_key[key]) <= time_eps_s:
            continue
        out.append(p)
        last_by_key[key] = t

    return out


# =============================================================================
# Filtering / sanity (used by steps 02 and 03)
# =============================================================================

def filter_pick_outliers(picks: List[dict], max_span_seconds: float = 60.0) -> List[dict]:
    """
    Median-based outlier filter for a *single event/group* of picks.
    Keeps picks within ±max_span_seconds of the median pick time.

    If <3 picks, returns unchanged (not enough support to call outliers).
    """
    if not picks or len(picks) < 3:
        return picks

    times = [p["time"] for p in picks if p.get("time") is not None]
    if len(times) < 3:
        return picks

    tmed = _utc_median(times)
    if tmed is None:
        return picks

    return [p for p in picks if abs(p["time"] - tmed) <= max_span_seconds]


def filter_pick_group(
    picks: List[dict],
    *,
    max_span_seconds: float = 60.0,
    max_gap_seconds: float = 4.0,
    min_picks: int = 1,
    drop_singletons_far_from_median: bool = False,
) -> List[dict]:
    """
    A more general "group sanity" function (Step 03 uses this).

    What it does:
      1) dedupe picks
      2) optional median outlier removal
      3) optional: drop singleton groups far from median (if enabled)
      4) enforce internal span constraint

    NOTE: This function returns a *filtered list* of picks for the group.
    Splitting into multiple events is handled elsewhere (monthly parsing).
    """
    if not picks:
        return []

    picks = _dedupe_picks(picks)

    # median-based outliers (always)
    picks = filter_pick_outliers(picks, max_span_seconds=max_span_seconds)

    if not picks:
        return []

    # internal span sanity
    times = sorted([p["time"] for p in picks])
    span = float(times[-1] - times[0])
    if span > max_span_seconds:
        # too wide -> keep only those within median window
        picks = filter_pick_outliers(picks, max_span_seconds=max_span_seconds)

    if not picks or len(picks) < min_picks:
        return []

    if drop_singletons_far_from_median and len(picks) == 1:
        # (rarely used; provided for completeness)
        # if you enable this, it can drop isolated garbage groups
        return []

    return picks


# =============================================================================
# Parsing
# =============================================================================

def parse_phase_line(line: str):
    """
    Parse a single PHA line (fixed-width monthly style, or tokenized style).

    Returns:
      - list of pick dicts (0, 1, or 2 picks) OR
      - None if not a pick line
    """
    if not line:
        return None
    s = line.rstrip("\n\r")
    if not s.strip():
        return None

    # -------------------------------------------------------------------------
    # Fixed-width / monthly style
    # -------------------------------------------------------------------------
    try:
        station = s[0:3].strip()
        if station and station.lower() != "xxx" and len(station) >= 2:
            orientation = s[3:4].strip() if len(s) > 3 else ""
            p_arrival_code = s[4:8].replace(" ", "?") if len(s) > 4 else ""

            timestamp_str = None
            if len(s) > 8:
                if s[8] == " ":
                    timestamp_str = s[9:24].strip().replace(" ", "0") if len(s) >= 24 else None
                else:
                    timestamp_str = s[8:23].strip().replace(" ", "0") if len(s) >= 23 else None

            if timestamp_str:
                # normalize occasional 60s weirdness
                add_secs = 0
                if timestamp_str.endswith("60.00"):
                    timestamp_str = timestamp_str.replace("60.00", "00.00")
                    add_secs += 60

                # support fraction if present
                t0 = _parse_yyMMddHHMMSS_frac(timestamp_str)
                if t0 is None:
                    # last resort: try first 12 chars
                    try:
                        dt = datetime.strptime(timestamp_str[:12], "%y%m%d%H%M%S")
                        t0 = UTCDateTime(dt)
                    except Exception:
                        t0 = None

                if t0 is None:
                    return None
                t0 = t0 + add_secs

                if orientation in "ZNE":
                    channel = f"EH{orientation}"
                elif orientation == "L":
                    channel = "ELZ"
                else:
                    channel = "EHZ"

                seed_id = f"XB.{station}..{channel}"

                results = []

                has_p = len(p_arrival_code) >= 2 and p_arrival_code[1] == "P"
                if has_p:
                    p_clean = p_arrival_code.replace("?", " ")
                    results.append({
                        "station": station,
                        "channel": channel,
                        "seed_id": seed_id,
                        "phase": "P",
                        "time": t0,
                        "onset": p_clean[0] if len(p_clean) > 0 and p_clean[0] in ("I", "E") else None,
                        "first_motion": p_clean[2] if len(p_clean) > 2 and p_clean[2] in ("U", "D") else None,
                        "weight": int(p_clean[3]) if len(p_clean) > 3 and p_clean[3].isdigit() else None,
                    })

                # detect an S marker roughly in 35-40 like your older parser
                s_positions = [i for i, c in enumerate(s) if c == "S"]
                s_positions = [pos for pos in s_positions if 35 <= pos <= 40]
                s_pos = s_positions[0] if len(s_positions) == 1 else 0

                if s_pos > 0:
                    # delay is typically left of the 'S'
                    s_wave_delay = ""
                    if len(s) > s_pos - 7:
                        s_wave_delay = s[s_pos - 7:s_pos - 1].strip()

                    delay = _safe_float(s_wave_delay) if s_wave_delay else None
                    if delay is not None:
                        results.append({
                            "station": station,
                            "channel": channel,
                            "seed_id": seed_id,
                            "phase": "S",
                            "time": t0 + float(delay),
                            "onset": None,
                            "first_motion": None,
                            "weight": None,
                        })

                return results if results else None
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # Tokenized / individual style
    # -------------------------------------------------------------------------
    toks = s.split()
    if len(toks) >= 4:
        station = toks[0].strip()
        if station.lower() in ("xxxx", "xxx") or len(station) < 2:
            return None

        phase = toks[1].strip().upper()

        # toks[2] often weight, but keep optional
        try:
            weight = int(toks[2])
        except Exception:
            weight = None

        t = _parse_yyMMddHHMMSS_frac(toks[3])
        if t is None:
            return None

        base_station = station[:3] if len(station) >= 3 else station
        channel = "EHZ"
        seed_id = f"XB.{base_station}..{channel}"

        return [{
            "station": base_station,
            "channel": channel,
            "seed_id": seed_id,
            "phase": phase,
            "time": t,
            "onset": None,
            "first_motion": None,
            "weight": weight,
        }]

    return None


# =============================================================================
# Monthly parsing: "10" blocks → events (with time-splitting)
# =============================================================================

def _split_into_time_clusters(picks: List[dict], max_gap_s: float = 4.0) -> List[List[dict]]:
    """
    Given picks from a single monthly block, split into clusters by time gaps.

    Rule: sort by time; start a new cluster when time gap between consecutive
    picks exceeds max_gap_s.
    """
    if not picks:
        return []
    picks = sorted(picks, key=lambda p: p["time"])
    clusters = [[picks[0]]]
    for p in picks[1:]:
        if float(p["time"] - clusters[-1][-1]["time"]) > max_gap_s:
            clusters.append([p])
        else:
            clusters[-1].append(p)
    return clusters


def _finalize_monthly_block(
    block_picks: List[dict],
    *,
    max_gap_s: float = 4.0,
    outlier_window_s: float = 60.0,
) -> List[dict]:
    """
    Convert a monthly "10" candidate block into one or more event dicts.

    Steps:
      - dedupe
      - split by time gaps
      - per-cluster outlier filter
      - compute origin_time (min P else min all)
    """
    if not block_picks:
        return []

    block_picks = _dedupe_picks(block_picks)

    clusters = _split_into_time_clusters(block_picks, max_gap_s=max_gap_s)
    events = []

    for cl in clusters:
        cl = filter_pick_group(cl, max_span_seconds=outlier_window_s, max_gap_seconds=max_gap_s, min_picks=1)
        if not cl:
            continue

        p_times = [p["time"] for p in cl if p.get("phase") == "P"]
        origin_time = min(p_times) if p_times else min(p["time"] for p in cl)

        events.append({
            "origin_time": origin_time,
            "picks": cl,
        })

    return events


def parse_pha_file(path, errors=None):
    """
    Parse monthly PHA file into list of events (origin_time + picks).
    Candidate blocks separated by '10' or '100'.
    Each candidate block may become multiple events via time clustering.
    """
    events: List[dict] = []
    block_picks: List[dict] = []

    path = Path(path)

    try:
        with open(path, "r", errors="ignore") as fh:
            for lineno, raw in enumerate(fh, 1):
                line = raw.rstrip("\n\r")
                if not line.strip():
                    continue

                if line.strip() in ("10", "100"):
                    events.extend(_finalize_monthly_block(block_picks))
                    block_picks = []
                    continue

                parsed = parse_phase_line(line)
                if parsed:
                    block_picks.extend(parsed if isinstance(parsed, list) else [parsed])
                else:
                    if errors is not None:
                        errors.append(f"{path.name}:{lineno}: {line.strip()}")

        # final block
        events.extend(_finalize_monthly_block(block_picks))

    except Exception as e:
        if errors is not None:
            errors.append(f"Error reading {path}: {e}")

    return events


# =============================================================================
# Individual parsing
# =============================================================================

def parse_individual_pha_file(path):
    """
    Parse an individual-event PHA file and return a flat list of picks.
    Deduped; NO grouping/splitting here.
    """
    picks: List[dict] = []
    path = Path(path)

    try:
        with open(path, "r", errors="ignore") as fh:
            for raw in fh:
                parsed = parse_phase_line(raw.rstrip("\n\r"))
                if parsed:
                    picks.extend(parsed if isinstance(parsed, list) else [parsed])
    except Exception:
        pass

    return _dedupe_picks(picks)