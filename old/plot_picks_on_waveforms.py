#!/usr/bin/env python3
"""
Plot picks from two CSV files on top of *all* MiniSEED files found under a
SEISAN WAV directory.

Differences from original:
 - We no longer search by event window or time.
 - We simply iterate over *every* MiniSEED file found under wav_root.
 - Two CSV files are read; each is plotted in its own color.
 - For each MiniSEED file, the script draws the waveform(s) and the
   pick markers if present for that station/time.

Usage:
  python plot_all_picks.py \
      --wav-root path/to/WAV/PNTBO \
      --pick-a pha1.csv \
      --pick-b pha2.csv \
      --out output_directory

Dependencies: obspy, pandas, matplotlib
"""

import argparse
from pathlib import Path
from obspy import read, UTCDateTime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os


def load_picks(csv_path):
    """Load a pick CSV (must contain at least: pick_time, station, phase)."""
    df = pd.read_csv(csv_path)
    # convert pick_time to datetime
    df['pick_dt'] = pd.to_datetime(df['pick_time'])
    return df


def find_all_mseed_files(wav_root):
    """Return all MiniSEED files under wav_root recursively."""
    wav_root = Path(wav_root)
    return list(wav_root.rglob("*.M.*")) + list(wav_root.rglob("*.mseed"))


def plot_single_mseed_with_picks(mseed_path, picks_a, picks_b, outdir):
    """
    Plot one MiniSEED file and overlay picks from two pick tables.
    Each CSV gets its own color.
    """
    try:
        st = read(str(mseed_path))
    except Exception as e:
        print(f"Cannot read {mseed_path}: {e}")
        return

    if len(st) == 0:
        print(f"No traces in {mseed_path}")
        return

    # Prepare figure
    fig, ax = plt.subplots(figsize=(14, 8))
    station_order = sorted({tr.stats.station for tr in st})

    offsets = np.arange(len(station_order))[::-1] * 1.5
    station_to_offset = {sta: offsets[i] for i, sta in enumerate(station_order)}

    # Plot waveforms
    for sta in station_order:
        traces = [tr for tr in st if tr.stats.station == sta]
        if not traces:
            continue
        # prefer vertical component if present
        preferred = None
        for code in ("Z", "HZ", "EHZ", "BHZ", "HHZ", "ENZ"):
            for tr in traces:
                if tr.stats.channel.upper().endswith(code):
                    preferred = tr
                    break
            if preferred:
                break
        if preferred is None:
            preferred = traces[0]

        tr = preferred.copy()
        tr.detrend("demean")

        t0 = tr.stats.starttime
        times = tr.times(reftime=t0)
        y = tr.data.astype(float)
        y /= np.max(np.abs(y)) if np.max(np.abs(y)) > 0 else 1.0
        y = y * 0.9 + station_to_offset[sta]

        ax.plot([t0 + t for t in times], y, color="black", linewidth=0.5)

    # Pick colors
    color_a = "red"
    color_b = "blue"

    # Overlay picks
    for picks, color in [(picks_a, color_a), (picks_b, color_b)]:
        if picks is None:
            continue
        for _, row in picks.iterrows():
            station = row['station']
            if station not in station_to_offset:
                continue

            pick_time = UTCDateTime(row['pick_dt'].to_pydatetime())
            phase = str(row.get("phase", "")).upper()
            linestyle = "-" if phase.startswith("P") else "--"

            y = station_to_offset[station]
            ax.axvline(pick_time.datetime, color=color, linestyle=linestyle, linewidth=1)
            ax.plot(pick_time.datetime, y, color=color, marker="o", markersize=3)

    # Format
    ax.set_yticks(offsets)
    ax.set_yticklabels(station_order[::-1])
    ax.set_xlabel("Time (UTC)")
    ax.set_title(f"Waveform + Picks: {os.path.basename(mseed_path)}")
    fig.autofmt_xdate()
    plt.tight_layout()

    # Save
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / (Path(mseed_path).stem + "_picks.png")
    fig.savefig(outfile, dpi=150)
    plt.close(fig)
    print(f"Saved: {outfile}")


def main():
    ap = argparse.ArgumentParser(description="Plot picks from two CSVs on all MiniSEED files")
    ap.add_argument("--wav-root", required=True, help="Root directory of SEISAN WAV")
    ap.add_argument("--pick-a", required=True, help="Pick CSV file A")
    ap.add_argument("--pick-b", required=True, help="Pick CSV file B")
    ap.add_argument("--out", required=True, help="Output directory for PNGs")
    args = ap.parse_args()

    picks_a = load_picks(args.pick_a)
    picks_b = load_picks(args.pick_b)

    mseed_files = find_all_mseed_files(args.wav_root)
    print(f"Found {len(mseed_files)} MiniSEED files.")

    for f in mseed_files:
        plot_single_mseed_with_picks(f, picks_a, picks_b, args.out)


if __name__ == "__main__":
    main()