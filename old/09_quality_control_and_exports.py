#!/usr/bin/env python3
"""
07_quality_control_and_exports.py

Performs QC diagnostics on:
    - master_event_table.pkl
Outputs:
    - qc_missing_waveforms.csv
    - qc_missing_locations.csv
    - qc_daily_counts.png
    - qc_station_coverage.csv
"""

import argparse
import pandas as pd
import numpy as np
from obspy import UTCDateTime
import matplotlib.pyplot as plt
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="Quality control diagnostics.")
    ap.add_argument("--master", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    df = pd.read_pickle(args.master)

    # -----------------------------
    # Missing waveform file
    # -----------------------------
    missing_wav = df[df["waveform_file"].isna()]
    missing_wav.to_csv(out/"qc_missing_waveforms.csv", index=False)

    # -----------------------------
    # Missing location information
    # -----------------------------
    no_loc = df[df["latitude"].isna()]
    no_loc.to_csv(out/"qc_missing_locations.csv", index=False)

    # -----------------------------
    # Daily event counts
    # -----------------------------
    df["date"] = df["origin_time"].apply(lambda t: t.date())
    daily = df.groupby("date").size()

    plt.figure(figsize=(12,5))
    daily.plot(kind="bar")
    plt.title("Daily Event Counts")
    plt.tight_layout()
    plt.savefig(out/"qc_daily_counts.png")
    plt.close()

    # -----------------------------
    # Station coverage (from waveform filenames)
    # -----------------------------
    def extract_sta(fname):
        if not isinstance(fname, str):
            return None
        # filenames like: 1991/06/1991-06-10-1234-56M.PNTBO_003
        # SEISAN MiniSEED has station codes inside the actual MiniSEED, 
        # but we approximate by channel count or later enhance this
        return Path(fname).stem

    df["wav_stub"] = df["waveform_file"].apply(extract_sta)
    df["year"] = df["origin_time"].apply(lambda t: t.year)

    coverage = df.groupby(["year", "wav_stub"]).size()
    coverage.to_csv(out/"qc_station_coverage.csv")

    print("QC complete.")


if __name__ == "__main__":
    main()