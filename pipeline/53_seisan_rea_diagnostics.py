#!/usr/bin/env python3
"""
53_seisan_rea_diagnostics.py

STEP 53 — Diagnostics and sanity checks for SEISAN REA catalog.

Uses EVENT_CLASS comments written in Step 52 as the authoritative source
of event composition (W, P, H), with heuristic fallback for legacy files.

Outputs:
  • Summary counts by EVENT_CLASS
  • Daily counts:
      - total events
      - events with waveforms
      - events with picks
      - events with hypocenters
  • Daily EVENT_CLASS counts
  • CSV + PNG plot

Author: GT__/ChatGPT
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def iter_sfiles(rea_dir: Path):
    for p in rea_dir.rglob("*"):
        if p.is_file() and re.search(r"\.S\d{6}$", p.name):
            yield p


def get_event_class(lines):
    """
    Extract EVENT_CLASS from Nordic comment lines.
    Expected:
      EVENT_CLASS:W+P+H
    """
    for ln in lines:
        if "EVENT_CLASS:" in ln:
            return ln.split("EVENT_CLASS:", 1)[1].strip()
    return None


def fallback_flags(lines, wave_re):
    """
    Heuristic fallback if EVENT_CLASS is missing.
    Returns: has_W, has_P, has_H
    """
    has_wave = any(wave_re.search(ln) for ln in lines)

    # Picks
    has_pick_header = any(ln.startswith(" STAT") or ln.startswith("STAT ") for ln in lines)
    stationish = re.compile(r"^[A-Z0-9]{2,5}\s+[A-Z0-9]{1,3}\s+[A-Z0-9]{0,2}")
    has_station_lines = any(stationish.match(ln) for ln in lines)
    has_picks = has_pick_header or has_station_lines

    # Hypocenter
    has_gap = any("GAP=" in ln for ln in lines)
    has_type1 = any(ln.rstrip().endswith("1") and ln[:4].strip().isdigit() for ln in lines)
    has_hypo = has_gap or has_type1

    return has_wave, has_picks, has_hypo


def day_key_from_sfilename(name: str) -> str | None:
    """
    Parse date from SEISAN S-file name:
      DD-HHMM-SSL.SYYYYMM
    """
    m = re.search(r"^(?P<dd>\d{2})-.*\.S(?P<yyyy>\d{4})(?P<mm>\d{2})$", name)
    if not m:
        return None
    try:
        return f"{int(m.group('yyyy')):04d}-{int(m.group('mm')):02d}-{int(m.group('dd')):02d}"
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 53: SEISAN REA diagnostics")
    ap.add_argument("--rea-dir", required=True, help="Top-level REA directory")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument(
        "--wavefile-regex",
        default=r"\b[MS]\.[A-Za-z0-9]{1,12}_[A-Za-z0-9]{1,6}\b",
        help="Regex to detect waveform filenames (fallback only)"
    )
    args = ap.parse_args()

    rea_dir = Path(args.rea_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    wave_re = re.compile(args.wavefile_regex)

    combo = Counter()
    daily = defaultdict(lambda: {
        "total": 0,
        "wave": 0,
        "picks": 0,
        "hypo": 0
    })

    daily_by_class = defaultdict(lambda: Counter())

    sfiles = sorted(iter_sfiles(rea_dir))
    if not sfiles:
        raise SystemExit(f"No S-files found under {rea_dir}")

    for sf in sfiles:
        try:
            lines = sf.read_text(errors="replace").splitlines()
        except Exception:
            continue

        ev_class = get_event_class(lines)

        if ev_class:
            has_W = "W" in ev_class
            has_P = "P" in ev_class
            has_H = "H" in ev_class
            label = ev_class
        else:
            has_W, has_P, has_H = fallback_flags(lines, wave_re)
            label = (
                ("W" if has_W else "") +
                ("+P" if has_P else "") +
                ("+H" if has_H else "")
            ).lstrip("+") or "none"

        combo[label] += 1

        day = day_key_from_sfilename(sf.name)
        if not day:
            continue

        daily[day]["total"] += 1
        if has_W:
            daily[day]["wave"] += 1
        if has_P:
            daily[day]["picks"] += 1
        if has_H:
            daily[day]["hypo"] += 1

        daily_by_class[day][label] += 1

    # ------------------------------------------------------------------
    # OUTPUT: SUMMARY
    # ------------------------------------------------------------------

    print("\nSTEP 53 — EVENT CLASS SUMMARY")
    print("------------------------------")
    for k, v in combo.most_common():
        print(f"{k:10s}: {v}")

    # ------------------------------------------------------------------
    # OUTPUT: DAILY CSV
    # ------------------------------------------------------------------

    df_daily = pd.DataFrame.from_dict(daily, orient="index").sort_index()
    df_daily.index = pd.to_datetime(df_daily.index)
    df_daily.index.name = "date"

    df_class = pd.DataFrame.from_dict(daily_by_class, orient="index").fillna(0).astype(int)
    df_class.index = pd.to_datetime(df_class.index)
    df_class.index.name = "date"

    df = df_daily.join(df_class, how="left")

    csv_path = out_dir / "53_daily_counts.csv"
    df.to_csv(csv_path)

    # ------------------------------------------------------------------
    # OUTPUT: PLOT
    # ------------------------------------------------------------------

    fig_path = out_dir / "53_daily_counts.png"

    plt.figure(figsize=(10, 5))
    plt.plot(df.index, df["total"], label="total events")
    plt.plot(df.index, df["wave"], label="events with waveforms")
    plt.plot(df.index, df["picks"], label="events with picks")
    plt.plot(df.index, df["hypo"], label="events with hypocenters")
    plt.legend()
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.title("SEISAN REA Daily Event Counts (Step 53)")
    plt.tight_layout()
    plt.savefig(fig_path, dpi=200)
    plt.close()

    print("\nOutputs")
    print("-------")
    print(f"Daily CSV : {csv_path}")
    print(f"Plot PNG  : {fig_path}")
    print("\nSTEP 53 COMPLETE")


if __name__ == "__main__":
    main()