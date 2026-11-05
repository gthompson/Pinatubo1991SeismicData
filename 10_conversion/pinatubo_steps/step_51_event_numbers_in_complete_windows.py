#!/usr/bin/env python3
"""
51_event_numbers_in_complete_windows.py

STEP 51 — Event-number statistics in coverage windows

This script computes event-number and composition statistics from the
ObsPy Catalog produced in Step 50, but ONLY for days where waveform,
pick, and hypocenter coverage is known to be complete.

Key design:
------------
• Trust Step 50 event semantics (event_class:*) — do NOT re-infer W/P/H
• Supports pick-only (P_ONLY) and hypocenter-only (H_ONLY) events
• Works for BOTH:
    - catalog_all.xml
    - catalog_waveform.xml

Coverage windows (inclusive):
-----------------------------
  A) 1991-05-08 .. 1991-06-09
  B) 1991-06-30 .. 1991-08-18

Outputs:
--------
• Console summary of composition counts
• Daily CSV with counts per category
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import Event
import pickle

from .shared import (get_comment_value)


# -----------------------------------------------------------------------------
# Coverage windows (inclusive)
# -----------------------------------------------------------------------------

COVER_WINDOWS = [
    ("1991-05-08", "1991-06-09"),
    ("1991-06-30", "1991-08-18"),
]


# Expected event_class labels from Step 50
EXPECTED_LABELS = [
    "W_P_H",
    "W_P",
    "W_H",
    "W_ONLY",
    "P_ONLY",
    "H_ONLY",
]


# -----------------------------------------------------------------------------
# Window helpers
# -----------------------------------------------------------------------------

def _parse_utcdate(s: str) -> UTCDateTime:
    """Convert YYYY-MM-DD → UTCDateTime at start of day."""
    return UTCDateTime(f"{s}T00:00:00Z")


def _in_windows(t: UTCDateTime, windows) -> bool:
    """Check whether a UTCDateTime falls inside any coverage window."""
    for a, b in windows:
        start = _parse_utcdate(a)
        end = _parse_utcdate(b) + 24 * 3600 - 1e-6  # inclusive
        if start <= t <= end:
            return True
    return False


# -----------------------------------------------------------------------------
# Event metadata helpers
# -----------------------------------------------------------------------------


def get_event_class(event: Event) -> str:
    """
    Read event_class from Step 50.
    Falls back to UNKNOWN if missing.
    """
    val = get_comment_value(event, "event_class:")
    if val is None:
        return "UNKNOWN"
    return val.strip().upper()


def infer_event_time(event: Event) -> Optional[UTCDateTime]:
    """
    Choose a single representative time for binning:
      1) preferred origin time
      2) first origin time
      3) waveform_starttime comment
      4) earliest pick time
    """
    o = event.preferred_origin()
    if o and o.time:
        return o.time

    if event.origins:
        o0 = event.origins[0]
        if o0 and o0.time:
            return o0.time

    wst = get_comment_value(event, "waveform_starttime:")
    if wst:
        try:
            return UTCDateTime(wst)
        except Exception:
            pass

    if event.picks:
        times = [p.time for p in event.picks if p.time]
        if times:
            return min(times)

    return None


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def run(
    *,
    catalog_pkl,
    out_daily_csv,
    windows=None
):
    """Run this Pinatubo step with explicit notebook arguments. Return tables and results for inspection."""
    from types import SimpleNamespace
    args = SimpleNamespace(
        catalog_pkl=catalog_pkl,
        out_daily_csv=out_daily_csv,
        windows=windows,
    )

    # Determine coverage windows
    if args.windows is None:
        windows = COVER_WINDOWS
    else:
        if len(args.windows) % 2 != 0:
            raise SystemExit("--windows must be start/end date pairs")
        windows = [
            (args.windows[i], args.windows[i + 1])
            for i in range(0, len(args.windows), 2)
        ]

    with open(args.catalog_pkl, "rb") as f:
        catalog = pickle.load(f)

    rows = []
    skipped_no_time = 0
    skipped_unknown = 0

    # ------------------------------------------------------------------
    # Iterate events
    # ------------------------------------------------------------------

    for ev in catalog:
        t = infer_event_time(ev)
        if t is None:
            skipped_no_time += 1
            continue

        if not _in_windows(t, windows):
            continue

        label = get_event_class(ev)
        if label not in EXPECTED_LABELS:
            skipped_unknown += 1
            continue

        day = pd.Timestamp(t.datetime).normalize()

        rows.append(
            {
                "date": day,
                "event_time_utc": t.datetime,
                "event_class": label,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        print("\nNo events found within the specified windows.")
        print("Skipped (no inferable time):", skipped_no_time)
        return

    # ------------------------------------------------------------------
    # Daily pivot table
    # ------------------------------------------------------------------

    daily = (
        df.pivot_table(
            index="date",
            columns="event_class",
            values="event_time_utc",
            aggfunc="count",
            fill_value=0,
        )
        .sort_index()
        .reset_index()
    )

    # Ensure consistent columns
    for col in EXPECTED_LABELS:
        if col not in daily.columns:
            daily[col] = 0

    daily["Total"] = daily[EXPECTED_LABELS].sum(axis=1)

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    out_csv = Path(args.out_daily_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out_csv, index=False, date_format="%Y-%m-%d")

    # Overall composition
    comp = df["event_class"].value_counts().to_dict()
    total = len(df)

    print("\nEVENT-NUMBER STATS (restricted to coverage windows)")
    print("--------------------------------------------------")
    print("Windows (inclusive):")
    for a, b in windows:
        print(f"  - {a} .. {b}")
    print(f"\nTotal events in windows: {total}")
    for k in EXPECTED_LABELS:
        print(f"{k:22s}: {comp.get(k, 0)}")

    print(f"\nDaily CSV written: {out_csv}")
    if skipped_no_time:
        print(f"Skipped events with no inferable time: {skipped_no_time}")
    if skipped_unknown:
        print(f"Skipped events with unknown event_class: {skipped_unknown}")
    return {k: v for k, v in locals().items() if (k.startswith("df") or k.startswith("catalog") or k.endswith("_rows") or k in ("events", "annotations", "errors", "stats", "diag", "merged", "suppressed", "matches", "rows", "unparsed"))}
