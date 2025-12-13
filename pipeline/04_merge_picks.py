#!/usr/bin/env python3
"""
04_merge_picks.py

STEP 04 of the Pinatubo FAIR pipeline.

Merge pick indices from:
  - STEP 02: individual-event PHA picks (authoritative)
  - STEP 03: monthly PHA picks (secondary)

CRITICAL FIX:
--------------
Authoritative picks are FIRST de-duplicated using:
  (seed_id, phase, |Δt| <= tolerance)

This restores the behavior of the legacy (pre-2023) pipeline and is
required for correct waveform↔pick association downstream.
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def normalize_seed_id(row):
    if pd.notna(row.get("seed_id")):
        return row["seed_id"]
    return f"XB.{row['station']}..{row['channel']}"

def parse_time(t):
    return pd.to_datetime(t, errors="coerce", utc=True)


# -----------------------------------------------------------------------------
# Deduplicate authoritative picks (legacy behavior)
# -----------------------------------------------------------------------------

def deduplicate_primary_picks(df, tolerance):
    """
    Remove duplicate authoritative picks.

    Duplicates are defined as:
      same seed_id
      same phase
      |Δt| <= tolerance
    """
    df = df.copy()
    df["seed_norm"] = df.apply(normalize_seed_id, axis=1)
    df["pick_time_dt"] = df["pick_time"].apply(parse_time)

    keep_rows = []

    for (_, phase), grp in df.groupby(["seed_norm", "phase"]):
        grp = grp.sort_values("pick_time_dt")
        used = np.zeros(len(grp), dtype=bool)

        times = grp["pick_time_dt"].to_numpy()

        for i in range(len(grp)):
            if used[i]:
                continue
            used[i] = True
            close = np.abs((times - times[i]).astype("timedelta64[ms]").astype(float) / 1000.0) <= tolerance
            used |= close
            keep_rows.append(grp.iloc[i])

    out = pd.DataFrame(keep_rows).drop(columns=["seed_norm", "pick_time_dt"])
    return out.reset_index(drop=True)


# -----------------------------------------------------------------------------
# Merge logic
# -----------------------------------------------------------------------------

def merge_pick_tables(df_primary, df_secondary, tolerance):
    kept = []
    suppressed = []

    # ---- DEDUPLICATE PRIMARY PICKS FIRST ----
    df_primary = deduplicate_primary_picks(df_primary, tolerance)

    df_primary = df_primary.copy()
    df_secondary = df_secondary.copy()

    for df in (df_primary, df_secondary):
        df["seed_norm"] = df.apply(normalize_seed_id, axis=1)
        df["pick_time_dt"] = df["pick_time"].apply(parse_time)

    prim_groups = df_primary.groupby(["seed_norm", "phase"])

    # --- Keep ALL deduplicated primary picks ---
    for _, row in df_primary.iterrows():
        row = row.copy()
        row["pick_priority"] = "primary"
        row["merged_pick_group_id"] = row["pick_group_id"]
        row["merged_pick_group_type"] = "authoritative"
        row["merged_event_hint"] = row["event_id"]
        kept.append(row)

    # --- Process secondary (monthly) picks ---
    for _, row in df_secondary.iterrows():
        row = row.copy()
        key = (row["seed_norm"], row["phase"])

        if key not in prim_groups.groups:
            row["pick_priority"] = "secondary"
            row["merged_pick_group_id"] = row["monthly_block_id"]
            row["merged_pick_group_type"] = "monthly_only"
            row["merged_event_hint"] = None
            kept.append(row)
            continue

        block = prim_groups.get_group(key)
        dt = np.abs(
            (block["pick_time_dt"] - row["pick_time_dt"]).dt.total_seconds()
        )

        if (dt <= tolerance).any():
            suppressed.append(row)
        else:
            row["pick_priority"] = "secondary"
            row["merged_pick_group_id"] = row["monthly_block_id"]
            row["merged_pick_group_type"] = "monthly_only"
            row["merged_event_hint"] = None
            kept.append(row)

    merged = (
        pd.DataFrame(kept)
        .drop(columns=["seed_norm", "pick_time_dt"], errors="ignore")
        .reset_index(drop=True)
    )

    suppressed_df = (
        pd.DataFrame(suppressed)
        .drop(columns=["seed_norm", "pick_time_dt"], errors="ignore")
        .reset_index(drop=True)
    )

    return merged, suppressed_df


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 04: Merge PHA pick tables")
    ap.add_argument("--primary", required=True)
    ap.add_argument("--secondary", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--time-tolerance", type=float, default=0.5)
    ap.add_argument("--report", default=None)

    args = ap.parse_args()

    df_primary = pd.read_csv(args.primary)
    df_secondary = pd.read_csv(args.secondary)

    merged, suppressed = merge_pick_tables(
        df_primary, df_secondary, args.time_tolerance
    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out, index=False)

    print("\n=== STEP 04 SUMMARY ===")
    print(f"Authoritative picks (raw): {len(df_primary)}")
    print(f"Authoritative picks (deduped): {len(merged[merged['pick_priority']=='primary'])}")
    print(f"Monthly picks:       {len(df_secondary)}")
    print(f"Merged picks:        {len(merged)}")
    print(f"Suppressed picks:    {len(suppressed)}")
    print(f"Output CSV:          {out}")

    if args.report:
        rpt = Path(args.report)
        rpt.parent.mkdir(parents=True, exist_ok=True)
        suppressed.to_csv(rpt, index=False)
        print(f"Suppressed CSV:      {rpt}")


if __name__ == "__main__":
    main()