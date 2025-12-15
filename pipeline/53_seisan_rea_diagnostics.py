#!/usr/bin/env python3
"""
53_seisan_rea_diagnostics.py

STEP 53 — Sanity checks, diagnostics, and plots for SEISAN REA catalog.

Outputs:
  • Summary counts of S-files with Waveforms (W), Picks (P), Hypocenters (H)
    including combinations (W, P, H, WP, WH, PH, WPH)
  • Daily time series plot:
      - total events
      - events with linked miniseed files (waveform filename lines)
      - events with phase picks
      - events with hypocenters
  • CSV of per-day counts

Waveform detection:
  • Uses regex match against full S-file lines
  • Default matches: r"\\b[MS]\\.[A-Za-z0-9]{1,12}_[A-Za-z0-9]{1,6}\\b" (generic)
  • For Pinatubo you can pass: --wavefile-regex "M\\.PNTBO_"
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd
import matplotlib.pyplot as plt


def iter_sfiles(rea_dir: Path):
    for p in rea_dir.rglob("*"):
        if p.is_file() and p.name.endswith(".S") or p.suffix.startswith(".S"):
            # SEISAN sfiles typically like "03-0845-35L.S199107"
            yield p


def parse_sfile_flags(text: str, wave_re: re.Pattern):
    """
    Return flags: has_wave, has_picks, has_hypo
    Heuristics (Nordic 1):
      - Waveform lines: match wavefile regex anywhere in file
      - Picks: presence of the pick header line starting with 'STAT'
              or any typical station-phase lines after it
      - Hypocenter: presence of 'GAP=' line OR a line ending with '1' type
                    OR typical origin line format (we use a conservative check)
    """
    lines = text.splitlines()

    has_wave = any(wave_re.search(ln) for ln in lines)

    # Picks: SEISAN pick section header is very characteristic
    has_pick_header = any(ln.startswith(" STAT") or ln.startswith("STAT ") for ln in lines)
    # fallback: look for station-like lines (3-5 char station then space then component)
    stationish = re.compile(r"^[A-Z0-9]{2,5}\s+[A-Z0-9]{1,3}\s+[A-Z0-9]{0,2}\s+[A-Z0-9]{0,5}")
    has_station_lines = any(stationish.match(ln) for ln in lines)
    has_picks = has_pick_header or has_station_lines

    # Hypocenter: GAP= line is common when solution exists
    has_gap = any("GAP=" in ln for ln in lines)
    # Or: an origin line type "1" (line type is last char, but not always present in exports)
    has_type1 = any(ln.rstrip().endswith("1") and ln[:4].strip().isdigit() for ln in lines)
    has_hypo = has_gap or has_type1

    return has_wave, has_picks, has_hypo


def day_key_from_sfilename(name: str) -> str | None:
    """
    From SEISAN filename like: 03-0845-35L.S199107
    => date is encoded in the .SYYYYMM part AND day in prefix.
    We'll parse: DD-... .SYYYYMM
    """
    m = re.search(r"^(?P<dd>\d{2})-.*\.S(?P<yyyy>\d{4})(?P<mm>\d{2})$", name)
    if not m:
        return None
    dd = int(m.group("dd"))
    yyyy = int(m.group("yyyy"))
    mm = int(m.group("mm"))
    try:
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser(description="STEP 53: SEISAN REA diagnostics")
    ap.add_argument("--rea-dir", required=True, help="Top-level REA directory")
    ap.add_argument("--out-dir", required=True, help="Output directory for plots/CSVs")
    ap.add_argument(
        "--wavefile-regex",
        default=r"\b[MS]\.[A-Za-z0-9]{1,12}_[A-Za-z0-9]{1,6}\b",
        help=r"Regex to detect waveform filename lines (e.g. 'M\.PNTBO_')."
    )
    args = ap.parse_args()

    rea_dir = Path(args.rea_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    wave_re = re.compile(args.wavefile_regex)

    # Counters
    combo = Counter()
    daily = defaultdict(lambda: {"total": 0, "wave": 0, "picks": 0, "hypo": 0})

    sfiles = sorted(rea_dir.rglob("*.S*"))  # catches .SYYYYMM
    if not sfiles:
        raise SystemExit(f"No S-files found under {rea_dir}")

    for sf in sfiles:
        try:
            text = sf.read_text(errors="replace")
        except Exception:
            continue

        has_wave, has_picks, has_hypo = parse_sfile_flags(text, wave_re)

        key = (
            ("W" if has_wave else "") +
            ("P" if has_picks else "") +
            ("H" if has_hypo else "")
        ) or "none"
        combo[key] += 1

        day = day_key_from_sfilename(sf.name)
        if day:
            daily[day]["total"] += 1
            if has_wave:
                daily[day]["wave"] += 1
            if has_picks:
                daily[day]["picks"] += 1
            if has_hypo:
                daily[day]["hypo"] += 1

    # Print combo counts (including the “any 1/2/3” question)
    print("\nSTEP 53 — SUMMARY COUNTS")
    print("------------------------")
    for k in ["WPH", "WP", "WH", "PH", "W", "P", "H", "none"]:
        if k in combo:
            print(f"{k:4s}: {combo[k]}")
    # also show everything found
    extra = [k for k in combo.keys() if k not in {"WPH","WP","WH","PH","W","P","H","none"}]
    for k in sorted(extra):
        print(f"{k:4s}: {combo[k]}")

    # Daily dataframe
    df = pd.DataFrame.from_dict(daily, orient="index").sort_index()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    csv_path = out_dir / "53_daily_counts.csv"
    df.to_csv(csv_path, index_label="date")

    # Plot
    fig_path = out_dir / "53_daily_counts.png"

    plt.figure()
    plt.plot(df.index, df["total"], label="total events")
    plt.plot(df.index, df["wave"], label="events with linked miniseed")
    plt.plot(df.index, df["picks"], label="events with phase picks")
    plt.plot(df.index, df["hypo"], label="events with hypocenters")
    plt.legend()
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.title("SEISAN REA Daily Counts (Step 53)")
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