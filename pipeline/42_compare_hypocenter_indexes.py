#!/usr/bin/env python3
"""
42_compare_hypocenter_indexes.py

STEP 42 of the Pinatubo FAIR pipeline

Exact comparison of two hypocenter index CSV files.

Comparison fields:
  • origin_time (ISO-8601 UTC string)
  • latitude
  • longitude
  • depth_km
  • magnitude
"""

import argparse
from pathlib import Path
import pandas as pd

COMPARE_COLS = [
    "origin_time",
    "latitude",
    "longitude",
    "depth_km",
    "magnitude",
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hypo40", required=True)
    ap.add_argument("--hypo41", required=True)
    ap.add_argument("--out-prefix", required=True)
    args = ap.parse_args()

    df40 = pd.read_csv(args.hypo40)[COMPARE_COLS].drop_duplicates()
    df41 = pd.read_csv(args.hypo41)[COMPARE_COLS].drop_duplicates()

    merged = df41.merge(df40, how="left", on=COMPARE_COLS, indicator=True)
    pinaall_not_in_40 = merged[merged["_merge"] == "left_only"].drop(columns="_merge")

    merged_rev = df40.merge(df41, how="left", on=COMPARE_COLS, indicator=True)
    hypo40_not_in_pinaall = merged_rev[merged_rev["_merge"] == "left_only"].drop(columns="_merge")

    out_dir = Path(args.out_prefix).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    out1 = out_dir / "42_pinaall_not_in_40.csv"
    out2 = out_dir / "42_40_not_in_pinaall.csv"

    pinaall_not_in_40.to_csv(out1, index=False)
    hypo40_not_in_pinaall.to_csv(out2, index=False)

    print("\nHYPOCENTER INDEX COMPARISON (EXACT MATCH)")
    print("========================================")
    print(f"Rows in 40: {len(df40)}")
    print(f"Rows in 41: {len(df41)}\n")
    print(f"PINAALL rows found in 40: {len(df41) - len(pinaall_not_in_40)}")
    print(f"PINAALL rows NOT in 40:   {len(pinaall_not_in_40)}\n")
    print(f"40 rows NOT in PINAALL:   {len(hypo40_not_in_pinaall)}\n")
    print(f"Wrote: {out1}")
    print(f"Wrote: {out2}")

if __name__ == "__main__":
    main()