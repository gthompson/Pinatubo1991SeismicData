#!/usr/bin/env python3
"""
03_parse_monthly_phase_files.py

STEP 03 of the Pinatubo FAIR pipeline.

Parse PHIVOLCS / VDAP *monthly* PHA files into a flat pick index CSV.

This step is intentionally simple and tolerant:
• NO QuakeML is written
• NO attempt is made to define event identity
• Picks are reconciled later against individual PHA files (Step 04)

Inputs (LEGACY, read-only):
---------------------------
--pha-dir     Directory containing monthly *.PHA files

Outputs (FAIR only):
--------------------
--out-csv     Flat pick index CSV
--error-log   Lines that could not be parsed
"""

import argparse
from pathlib import Path
import pandas as pd

# shared parser
from pha_parser import parse_pha_file


def main():
    ap = argparse.ArgumentParser(
        description="Parse monthly PHA files into flat pick index CSV"
    )
    ap.add_argument(
        "--pha-dir",
        required=True,
        help="Directory containing monthly *.PHA files (LEGACY)",
    )
    ap.add_argument(
        "--out-csv",
        required=True,
        help="Output CSV file (FAIR)",
    )
    ap.add_argument(
        "--error-log",
        required=True,
        help="Output parse error log (FAIR)",
    )
    args = ap.parse_args()

    pha_dir = Path(args.pha_dir)
    out_csv = Path(args.out_csv)
    error_log = Path(args.error_log)

    if not pha_dir.exists():
        raise SystemExit(f"PHA directory does not exist: {pha_dir}")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    error_log.parent.mkdir(parents=True, exist_ok=True)

    pha_files = sorted(pha_dir.glob("*.PHA"))
    if not pha_files:
        raise SystemExit(f"No .PHA files found in {pha_dir}")

    rows = []
    errors = []

    print(f"Found {len(pha_files)} monthly PHA files")

    for pha_file in pha_files:
        events = parse_pha_file(pha_file, errors)
        print(f"  {pha_file.name}: {len(events)} event blocks")

        for event_idx, ev in enumerate(events):
            for pick in ev["picks"]:
                rows.append({
                    "source": "monthly",
                    "pha_file": pha_file.name,
                    "event_seq": event_idx,
                    "seed_id": pick.get("seed_id"),
                    "station": pick.get("station"),
                    "channel": pick.get("channel"),
                    "phase": pick.get("phase"),
                    "pick_time": str(pick.get("time")),
                    "onset": pick.get("onset"),
                    "first_motion": pick.get("first_motion"),
                    "weight": pick.get("weight"),
                })

    if not rows:
        print("No picks parsed from monthly PHA files.")
        return

    df = pd.DataFrame(rows)

    df.to_csv(out_csv, index=False)

    with open(error_log, "w") as f:
        for e in errors:
            f.write(e + "\n")

    print("\n=== STEP 03 SUMMARY ===")
    print(f"Monthly PHA files: {len(pha_files)}")
    print(f"Total picks:       {len(df)}")
    print(f"CSV written:       {out_csv}")
    print(f"Errors logged:     {error_log}")


if __name__ == "__main__":
    main()