#!/usr/bin/env python3
"""
pha_parser.py

Shared parsing utilities for PHA files (monthly and individual formats).

This version incorporates critical logic from the legacy (2023) Pinatubo
pipeline:
  • duplicate-pick removal
  • time-based pick grouping
  • outlier rejection within pick groups
  • robust event construction from monthly PHA blocks

PUBLIC API (unchanged):
  - parse_phase_line
  - parse_pha_file
  - parse_individual_pha_file
"""

from datetime import datetime
from typing import List
from obspy import UTCDateTime
import numpy as np


# -----------------------------------------------------------------------------
# Basic utilities
# -----------------------------------------------------------------------------

def picks_are_equal(p1, p2):
    return (
        p1["seed_id"] == p2["seed_id"]
        and p1["phase"] == p2["phase"]
        and abs(p1["time"] - p2["time"]) < 0.001
    )


def remove_duplicate_picks(picks: List[dict]) -> List[dict]:
    unique = {}
    for p in picks:
        key = (p["seed_id"], p["phase"], float(p["time"]))
        if key not in unique:
            unique[key] = p
    return list(unique.values())


# -----------------------------------------------------------------------------
# Pick grouping + sanity logic (CRITICAL)
# -----------------------------------------------------------------------------

def group_picks_by_time(picks: List[dict], seconds=4.0) -> List[List[dict]]:
    """
    Group picks into clusters where each pick is within `seconds`
    of the first pick in the group.
    """
    if not picks:
        return []

    picks = sorted(picks, key=lambda p: p["time"])
    groups = []
    current = [picks[0]]

    for p in picks[1:]:
        if (p["time"] - current[0]["time"]) <= seconds:
            current.append(p)
        else:
            groups.append(current)
            current = [p]

    groups.append(current)
    return groups


def reject_outlier_groups(groups, max_span=30.0):
    """
    Remove pathological groups:
      • single-pick groups far from median
      • groups with excessive internal span
    """
    if not groups:
        return []

    all_times = [p["time"].timestamp for g in groups for p in g]
    medtime = UTCDateTime(np.median(all_times))

    good = []
    for g in groups:
        times = [p["time"] for p in g]
        span = max(times) - min(times)

        if len(g) == 1 and abs(times[0] - medtime) > max_span:
            continue

        if span > max_span:
            continue

        good.append(g)

    return good


# -----------------------------------------------------------------------------
# Phase-line parsing (UNCHANGED except formatting)
# -----------------------------------------------------------------------------

def parse_phase_line(line):
    if not line:
        return None
    s = line.rstrip("\n\r")

    # --- Fixed-width legacy format ---
    try:
        station = s[0:3].strip()
        if station and station.lower() != "xxx" and len(station) >= 2:
            orientation = s[3:4].strip()
            p_code = s[4:8].replace(" ", "?")

            timestamp_str = None
            if len(s) > 8:
                timestamp_str = (
                    s[9:24].strip().replace(" ", "0")
                    if s[8] == " "
                    else s[8:23].strip().replace(" ", "0")
                )

            if not timestamp_str:
                return None

            add_secs = 0
            if timestamp_str.endswith("60.00"):
                timestamp_str = timestamp_str.replace("60.00", "00.00")
                add_secs = 60

            dt = datetime.strptime(
                timestamp_str[:12], "%y%m%d%H%M%S"
            )
            t0 = UTCDateTime(dt) + add_secs

            channel = (
                f"EH{orientation}"
                if orientation in "ZNE"
                else "EHZ"
            )
            seed_id = f"XB.{station}..{channel}"

            picks = []

            if len(p_code) >= 2 and p_code[1] == "P":
                picks.append({
                    "station": station,
                    "channel": channel,
                    "seed_id": seed_id,
                    "phase": "P",
                    "time": t0,
                    "onset": p_code[0] if p_code[0] in ("I", "E") else None,
                    "first_motion": p_code[2] if p_code[2] in ("U", "D") else None,
                    "weight": int(p_code[3]) if p_code[3].isdigit() else None,
                })

            # crude S-detection
            if "S" in s[35:41]:
                try:
                    delay = float(s[28:34])
                    picks.append({
                        "station": station,
                        "channel": channel,
                        "seed_id": seed_id,
                        "phase": "S",
                        "time": t0 + delay,
                        "onset": None,
                        "first_motion": None,
                        "weight": None,
                    })
                except Exception:
                    pass

            return picks if picks else None
    except Exception:
        pass

    # --- Tokenized fallback ---
    toks = s.split()
    if len(toks) >= 4:
        try:
            station = toks[0]
            phase = toks[1]
            t = UTCDateTime(
                datetime.strptime(toks[3][:12], "%y%m%d%H%M%S")
            )
            seed_id = f"XB.{station[:3]}..EHZ"
            return [{
                "station": station[:3],
                "channel": "EHZ",
                "seed_id": seed_id,
                "phase": phase,
                "time": t,
                "onset": None,
                "first_motion": None,
                "weight": None,
            }]
        except Exception:
            return None

    return None


# -----------------------------------------------------------------------------
# Monthly PHA parsing (THIS IS THE BIG FIX)
# -----------------------------------------------------------------------------

def parse_pha_file(path, errors=None):
    """
    Parse monthly PHA file into a list of *true events*.

    Separator lines define candidate blocks, but each block is:
      • deduplicated
      • grouped by time
      • split or merged as required
    """
    events = []
    block_picks = []

    try:
        with open(path, "r", errors="ignore") as fh:
            for lineno, raw in enumerate(fh, 1):
                line = raw.strip()

                if not line:
                    continue

                if line in ("10", "100"):
                    events.extend(_finalize_block(block_picks))
                    block_picks = []
                    continue

                parsed = parse_phase_line(line)
                if parsed:
                    block_picks.extend(parsed)
                elif errors is not None:
                    errors.append(f"{path.name}:{lineno}: {line}")

        events.extend(_finalize_block(block_picks))

    except Exception as e:
        if errors is not None:
            errors.append(f"{path}: {e}")

    return events


def _finalize_block(picks):
    if not picks:
        return []

    picks = remove_duplicate_picks(picks)
    groups = group_picks_by_time(picks, seconds=4.0)
    groups = reject_outlier_groups(groups)

    events = []
    for g in groups:
        p_times = [p["time"] for p in g if p["phase"] == "P"]
        origin_time = min(p_times) if p_times else min(p["time"] for p in g)
        events.append({
            "origin_time": origin_time,
            "picks": g,
        })

    return events


# -----------------------------------------------------------------------------
# Individual PHA parsing (simple but deduped)
# -----------------------------------------------------------------------------

def parse_individual_pha_file(path):
    picks = []
    try:
        with open(path, "r", errors="ignore") as fh:
            for raw in fh:
                parsed = parse_phase_line(raw)
                if parsed:
                    picks.extend(parsed)
    except Exception:
        pass

    return remove_duplicate_picks(picks)

def filter_pick_outliers(picks, max_span_seconds=60):
    """
    Remove picks that are wildly inconsistent with the rest of the group.
    Uses median time as a reference.
    """
    if len(picks) < 3:
        return picks

    times = [p["time"] for p in picks]
    tmed = sorted(times)[len(times) // 2]

    filtered = [
        p for p in picks
        if abs(p["time"] - tmed) <= max_span_seconds
    ]

    return filtered