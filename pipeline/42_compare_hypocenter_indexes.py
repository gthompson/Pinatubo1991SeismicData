#!/usr/bin/env python3
"""
07_compare_hypocenter_indexes.py

STEP 07 of the Pinatubo FAIR pipeline

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
    ap.add_argument("--hypo05", required=True)
    ap.add_argument("--hypo06", required=True)
    ap.add_argument("--out-prefix", required=True)
    args = ap.parse_args()

    df05 = pd.read_csv(args.hypo05)[COMPARE_COLS].drop_duplicates()
    df06 = pd.read_csv(args.hypo06)[COMPARE_COLS].drop_duplicates()

    merged = df06.merge(df05, how="left", on=COMPARE_COLS, indicator=True)
    pinaall_not_in_05 = merged[merged["_merge"] == "left_only"].drop(columns="_merge")

    merged_rev = df05.merge(df06, how="left", on=COMPARE_COLS, indicator=True)
    hypo05_not_in_pinaall = merged_rev[merged_rev["_merge"] == "left_only"].drop(columns="_merge")

    out_dir = Path(args.out_prefix).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    out1 = out_dir / "07_pinaall_not_in_05.csv"
    out2 = out_dir / "07_05_not_in_pinaall.csv"

    pinaall_not_in_05.to_csv(out1, index=False)
    hypo05_not_in_pinaall.to_csv(out2, index=False)

    print("\nHYPOCENTER INDEX COMPARISON (EXACT MATCH)")
    print("========================================")
    print(f"Rows in 05: {len(df05)}")
    print(f"Rows in 06: {len(df06)}\n")
    print(f"PINAALL rows found in 05: {len(df06) - len(pinaall_not_in_05)}")
    print(f"PINAALL rows NOT in 05:   {len(pinaall_not_in_05)}\n")
    print(f"05 rows NOT in PINAALL:   {len(hypo05_not_in_pinaall)}\n")
    print(f"Wrote: {out1}")
    print(f"Wrote: {out2}")

if __name__ == "__main__":
    main()