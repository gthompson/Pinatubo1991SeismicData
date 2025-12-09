#!/usr/bin/env python3
"""
05_associate_phase_hypo71_waveforms.py

Associates:
    (a) Parsed PHA pick events
    (b) Parsed HYPO71 located events
    (c) Indexed waveform files (MiniSEED, via wfdisc_catalog.csv)

Output:
    - master_event_table.csv (flat table: origin_time, file, picks, hypo71 metadata)
    - master_event_table.pkl (pickled pandas for fast reuse)
"""

import argparse
import pandas as pd
from obspy import read_events, UTCDateTime
from pathlib import Path
import numpy as np

# --------------------------------------------------------------
# Helpers
# --------------------------------------------------------------

def load_wfdisc(path):
    df = pd.read_csv(path)
    # convert times to UTCDateTime objects
    df["starttime"] = df["starttime"].apply(lambda t: UTCDateTime(t))
    df["endtime"] = df["endtime"].apply(lambda t: UTCDateTime(t))
    return df


def nearest_match(event_time, df, window=2.0):
    """
    Find waveform file whose starttime is within ±window seconds of event_time.
    Returns df row or None.
    """
    dt = df["starttime"].apply(lambda t: abs(t - event_time))
    closest = dt.min()
    if closest <= window:
        return df.iloc[dt.idxmin()]
    return None


# --------------------------------------------------------------
# Main
# --------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Associate PHA, HYPO71, and WAV events.")
    ap.add_argument("--wfdisc", required=True, help="wfdisc_catalog.csv")
    ap.add_argument("--pha-catalog", required=True, help="phase_catalog.xml")
    ap.add_argument("--hypo71-catalog", required=True, help="hypo71_catalog.xml")
    ap.add_argument("--outcsv", required=True)
    ap.add_argument("--outpkl", required=True)
    ap.add_argument("--time-window", type=float, default=2.0,
                    help="Matching window in seconds (default ±2s)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    print("Loading wfdisc...")
    wfdisc = load_wfdisc(args.wfdisc)

    print("Loading PHA catalog...")
    pha_cat = read_events(args.pha_catalog)

    print("Loading HYPO71 catalog...")
    hypo_cat = read_events(args.hypo71_catalog)

    master_rows = []

    # --- build lookup by time ---
    print("Matching HYPO71 → WAV...")
    for ev in hypo_cat:
        o = ev.preferred_origin() or ev.origins[0]
        t0 = o.time

        match = nearest_match(t0, wfdisc, args.time_window)

        row = {
            "origin_time": t0.datetime,
            "latitude": o.latitude,
            "longitude": o.longitude,
            "depth_m": o.depth,
            "magnitude": ev.magnitudes[0].mag if ev.magnitudes else np.nan,
            "waveform_file": match["filename"] if match is not None else None,
            "match_seconds": abs(match["starttime"] - t0) if match is not None else None,
            "source": "HYPO71"
        }

        master_rows.append(row)

    # --- add PHA-only events ---
    print("Matching PHA → WAV...")
    for ev in pha_cat:
        picks = ev.picks
        if len(picks) == 0:
            continue

        t0 = min(p.time for p in picks)  # earliest pick

        match = nearest_match(t0, wfdisc, args.time_window)

        row = {
            "origin_time": t0.datetime,
            "latitude": None,
            "longitude": None,
            "depth_m": None,
            "magnitude": None,
            "waveform_file": match["filename"] if match is not None else None,
            "match_seconds": abs(match["starttime"] - t0) if match is not None else None,
            "source": "PHA-only"
        }

        master_rows.append(row)

    df_master = pd.DataFrame(master_rows).sort_values("origin_time")

    print(f"Saving {args.outcsv}")
    df_master.to_csv(args.outcsv, index=False)

    print(f"Saving {args.outpkl}")
    df_master.to_pickle(args.outpkl)

    print("Done.")


if __name__ == "__main__":
    main()