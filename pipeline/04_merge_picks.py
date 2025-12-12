#!/usr/bin/env python3
"""
04_merge_picks.py

STEP 04 of the Pinatubo FAIR pipeline.

Merge pick indices from:
  - primary pick table (authoritative)
  - secondary pick table

Duplicate picks are identified using:
  - same SEED id (net.sta.loc.chan)
  - same phase
  - |time difference| <= tolerance (default: 0.5 s)

Primary picks overwrite secondary picks.

Outputs:
--------
• merged pick CSV
• optional merge report CSV
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------

def normalize_seed_id(row):
    """
    Construct a normalized SEED id.
    Falls back gracefully if seed_id is missing.
    """
    if "seed_id" in row and pd.notna(row["seed_id"]):
        return row["seed_id"]
    return f"XB.{row['station']}..{row['channel']}"

def parse_time(t):
    """Parse pick_time into pandas Timestamp (tolerant)."""
    return pd.to_datetime(t, errors="coerce")

# ----------------------------------------------------------------------
# Core merge logic
# ----------------------------------------------------------------------

def merge_pick_tables(df_primary, df_secondary, tolerance):
    """
    Merge two pick DataFrames.

    df_primary   : authoritative picks
    df_secondary : secondary picks
    tolerance    : seconds
    """

    kept_rows = []
    suppressed_rows = []

    df_primary = df_primary.copy()
    df_secondary = df_secondary.copy()

    # Normalize
    df_primary["seed_norm"] = df_primary.apply(normalize_seed_id, axis=1)
    df_secondary["seed_norm"] = df_secondary.apply(normalize_seed_id, axis=1)

    df_primary["pick_time_dt"] = df_primary["pick_time"].apply(parse_time)
    df_secondary["pick_time_dt"] = df_secondary["pick_time"].apply(parse_time)

    # Index primary by (seed, phase)
    primary_groups = df_primary.groupby(["seed_norm", "phase"])

    # Keep all primary picks
    for _, row in df_primary.iterrows():
        kept_rows.append(row)

    # Process secondary picks
    for _, row in df_secondary.iterrows():
        key = (row["seed_norm"], row["phase"])

        if key not in primary_groups.groups:
            kept_rows.append(row)
            continue

        block = primary_groups.get_group(key)
        dt = np.abs(
            (block["pick_time_dt"] - row["pick_time_dt"])
            .dt.total_seconds()
        )

        if (dt <= tolerance).any():
            suppressed_rows.append(row)
        else:
            kept_rows.append(row)

    merged = (
        pd.DataFrame(kept_rows)
        .drop(columns=["seed_norm", "pick_time_dt"], errors="ignore")
        .reset_index(drop=True)
    )

    report = (
        pd.DataFrame(suppressed_rows)
        .drop(columns=["seed_norm", "pick_time_dt"], errors="ignore")
        .reset_index(drop=True)
    )

    return merged, report

# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Merge primary + secondary PHA pick CSVs"
    )

    ap.add_argument("--primary", required=True,
                    help="Primary (authoritative) pick CSV")
    ap.add_argument("--secondary", required=True,
                    help="Secondary pick CSV")
    ap.add_argument("--out", required=True,
                    help="Output merged CSV path")
    ap.add_argument("--time-tolerance", type=float, default=0.5,
                    help="Time tolerance in seconds (default: 0.5)")
    ap.add_argument("--report", default=None,
                    help="Optional CSV of suppressed duplicate picks")

    args = ap.parse_args()

    df_primary = pd.read_csv(args.primary)
    df_secondary = pd.read_csv(args.secondary)

    merged, report = merge_pick_tables(
        df_primary,
        df_secondary,
        tolerance=args.time_tolerance
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)

    print("\n=== STEP 04 SUMMARY ===")
    print(f"Primary picks:     {len(df_primary)}")
    print(f"Secondary picks:   {len(df_secondary)}")
    print(f"Merged picks:      {len(merged)}")
    print(f"Suppressed dups:   {len(report)}")
    print(f"Time tolerance:    {args.time_tolerance} s")
    print(f"Output CSV:        {out_path}")

    if args.report:
        rpt_path = Path(args.report)
        rpt_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_csv(rpt_path, index=False)
        print(f"Merge report:      {rpt_path}")

if __name__ == "__main__":
    main()