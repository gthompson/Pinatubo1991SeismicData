#!/usr/bin/env python3
"""
pha_parser.py — Pinatubo wrappers around FloVoPy's VDAP PHA readers.

All phase-card decoding now lives in ``flovopy.core.vdap``
(parse_vdap_phase_line, read_vdap_pha_blocks, read_vdap_pha_file,
dedupe_picks, filter_picks_near_median). This module keeps the names the
step-20/21 notebooks import and the Pinatubo-specific policy values:
network XB, and median-time trimming of monthly blocks (±60 s on reading,
then the notebook's own window).

Public API (used by the notebooks):
  - parse_phase_line(line) -> list[dict] | None
  - parse_pha_file(path, errors=None) -> list[{"origin_time", "picks"}]
  - parse_individual_pha_file(path) -> list[dict]
  - filter_pick_outliers(picks, max_span_seconds=60.0) -> list[dict]
  - filter_pick_group(picks, max_span_seconds=60.0, ..., rejected=None) -> list[dict]
  - _dedupe_picks(picks, time_eps_s=1e-3) -> list[dict]
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from flovopy.core.vdap import (
    dedupe_picks,
    filter_picks_near_median,
    parse_vdap_phase_line,
    read_vdap_pha_blocks,
    read_vdap_pha_file,
)

NETWORK = "XB"
BLOCK_OUTLIER_WINDOW_S = 60.0


def _dedupe_picks(picks: List[dict], time_eps_s: float = 1e-3) -> List[dict]:
    """Remove repeated picks (same seed_id/phase within eps); see flovopy dedupe_picks."""
    return dedupe_picks(picks or [], time_eps_s=time_eps_s)


def filter_pick_outliers(picks: List[dict], max_span_seconds: float = 60.0) -> List[dict]:
    """Keep picks within ±max_span_seconds of the median time (unchanged if <3 picks)."""
    if not picks:
        return picks
    return filter_picks_near_median(picks, max_span_seconds)[0]


def filter_pick_group(
    picks: List[dict],
    *,
    max_span_seconds: float = 60.0,
    max_gap_seconds: float = 4.0,
    min_picks: int = 1,
    drop_singletons_far_from_median: bool = False,
    rejected: Optional[list] = None,
) -> List[dict]:
    """Deduplicate, then apply the median window (twice if the span stays wide).

    Removed picks are appended to ``rejected`` when a list is supplied, so
    callers can log every trimmed pick. ``max_gap_seconds`` is unused and kept
    for signature compatibility.
    """
    if not picks:
        return []
    picks = _dedupe_picks(picks)
    picks, out = filter_picks_near_median(picks, max_span_seconds)
    removed = list(out)
    if picks:
        times = sorted(p["time"] for p in picks)
        if float(times[-1] - times[0]) > max_span_seconds:
            picks, out = filter_picks_near_median(picks, max_span_seconds)
            removed.extend(out)
    if rejected is not None:
        rejected.extend(removed)
    if not picks or len(picks) < min_picks:
        if rejected is not None:
            rejected.extend(picks)
        return []
    if drop_singletons_far_from_median and len(picks) == 1:
        if rejected is not None:
            rejected.extend(picks)
        return []
    return picks


def parse_phase_line(line: str):
    """Decode one PHA card with FloVoPy; return a list of picks or None."""
    try:
        picks = parse_vdap_phase_line(line, network=NETWORK)
    except (ValueError, IndexError):
        return None
    return picks or None


def _describe(item) -> str:
    if isinstance(item, dict) and "raw_lineno" in item:
        return (f"{item.get('raw_file')}:{item.get('raw_lineno')}: trimmed "
                f"{item.get('phase')} {item.get('seed_id')} {item.get('time')}")
    return str(item)


def parse_pha_file(path, errors=None):
    """Parse a monthly PHA file into events (origin_time + picks).

    Events are delimited strictly by '10'/'100' separator lines; empty blocks
    are skipped. ``block_index`` counts every separator block in the file, so
    it is a stable event key that does not change when parsing improves. Each block is deduplicated and trimmed to ±60 s of its median
    time. When ``errors`` is a list, every rejected or partly skipped line and
    every trimmed pick is appended to it.
    """
    path = Path(path)
    blocks, diagnostics = read_vdap_pha_blocks(path, network=NETWORK)
    if errors is not None:
        errors.extend(f"{d['source_file']}:{d['source_line']}: {d['severity']}: "
                      f"{d['message']} | {d['raw_line'].strip()}" for d in diagnostics)
    events = []
    for block in blocks:
        if not block["picks"]:
            continue
        rejected = []
        picks = filter_pick_group(block["picks"], max_span_seconds=BLOCK_OUTLIER_WINDOW_S,
                                  min_picks=1, rejected=rejected)
        if errors is not None:
            errors.extend(f"{_describe(p)} (> {BLOCK_OUTLIER_WINDOW_S:g} s from block median)"
                          for p in rejected)
        if not picks:
            continue
        p_times = [p["time"] for p in picks if p.get("phase") == "P"]
        origin_time = min(p_times) if p_times else min(p["time"] for p in picks)
        events.append({"origin_time": origin_time, "picks": picks,
                       "block_index": block["block_index"],
                       "first_line": block["first_line"], "last_line": block["last_line"]})
    return events


def parse_individual_pha_file(path):
    """Parse an individual-event PHA file and return deduplicated picks."""
    picks = read_vdap_pha_file(path, network=NETWORK, strict=False,
                               allow_terminal_separator=True)
    return _dedupe_picks(picks)
