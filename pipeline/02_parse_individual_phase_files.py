#!/usr/bin/env python3
"""
02_parse_individual_phase_files.py

STEP 02 of the Pinatubo FAIR pipeline.

Parse individual-event PHA files (authoritative picks) and build a
flat pick index *with explicit event grouping metadata*.

Each individual PHA file represents one authoritative legacy event.

Outputs:
--------
• CSV pick index with explicit grouping fields
• Error log
"""

import argparse
from pathlib import Path
import pandas as pd

from pha_parser import parse_individual_pha_file


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def extract_event_id(pha_path: Path) -> str:
    """
    Extract YYMMDDNN event id from a PHA filename.
    Example: 910603A1.PHA → 910603A1
    """
    return pha_path.stem


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="STEP 02: Parse individual-event PHA files"
    )
    ap.add_argument("--pha-root", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--error-log", required=True)
    ap.add_argument("--glob", default="**/*.PHA")

    args = ap.parse_args()

    pha_root = Path(args.pha_root)
    out_csv = Path(args.out_csv)
    error_log = Path(args.error_log)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    error_log.parent.mkdir(parents=True, exist_ok=True)

    pha_files = sorted(pha_root.glob(args.glob))
    if not pha_files:
        raise SystemExit(f"No PHA files found under {pha_root}")

    rows = []
    errors = []

    print(f"Found {len(pha_files)} individual PHA files")

    pick_counter = 0

    for pha_file in pha_files:
        event_id = extract_event_id(pha_file)

        try:
            picks = parse_individual_pha_file(pha_file)
            picks = filter_pick_outliers(picks, max_span_seconds=60)
        except Exception as e:
            errors.append(f"{pha_file}: {e}")
            continue

        if not picks:
            continue

        for p in picks:
            pick_counter += 1

            rows.append({
                # ---- Event grouping (CRITICAL) ----
                "event_id": event_id,
                "event_id_source": "individual_pha_filename",
                "pick_group_id": event_id,
                "pick_group_type": "individual_pha",

                # ---- Pick identity ----
                "pick_id": f"indpha_{event_id}_{pick_counter}",
                "pha_file": pha_file.name,

                # ---- Seismic metadata ----
                "station": p.get("station"),
                "channel": p.get("channel"),
                "seed_id": p.get("seed_id"),
                "phase": p.get("phase"),
                "pick_time": str(p.get("time")),

                # ---- Pick attributes ----
                "onset": p.get("onset"),
                "first_motion": p.get("first_motion"),
                "weight": p.get("weight"),
            })

    if not rows:
        print("No picks parsed; exiting.")
        return

    df = (
        pd.DataFrame(rows)
        .sort_values(["event_id", "station", "phase", "pick_time"])
        .reset_index(drop=True)
    )

    df.to_csv(out_csv, index=False)

    with open(error_log, "w") as f:
        for line in errors:
            f.write(line + "\n")

    print("\n=== STEP 02 SUMMARY ===")
    print(f"Legacy events:      {df['event_id'].nunique()}")
    print(f"Total picks:        {len(df)}")
    print(f"CSV written:        {out_csv}")
    print(f"Errors logged:      {error_log}")


if __name__ == "__main__":
    main()