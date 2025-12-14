#!/usr/bin/env python3
"""
21_parse_monthly_phase_files.py

STEP 21 of the Pinatubo FAIR pipeline.

Parse monthly PHA files into a flat pick index while preserving
block-level grouping defined by separator lines, with internal
sanity checks on pick timing.

Outlier picks within a block are removed using logic adapted
from the legacy 2021–2023 pipeline.
"""

import argparse
from pathlib import Path
import pandas as pd

from pha_parser import (
    parse_pha_file,
    filter_pick_group,   # NEW — imported from refactored pha_parser.py
)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Parse monthly PHA files into flat pick index CSV"
    )
    ap.add_argument("--pha-dir", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--error-log", required=True)
    ap.add_argument(
        "--max-pick-span",
        type=float,
        default=30.0,
        help="Maximum allowed time span (s) within a pick block",
    )

    args = ap.parse_args()

    pha_dir = Path(args.pha_dir)
    out_csv = Path(args.out_csv)
    error_log = Path(args.error_log)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    error_log.parent.mkdir(parents=True, exist_ok=True)

    pha_files = sorted(pha_dir.glob("*.PHA"))
    if not pha_files:
        raise SystemExit(f"No .PHA files found in {pha_dir}")

    rows = []
    errors = []

    print(f"Found {len(pha_files)} monthly PHA files")

    kept_blocks = 0
    dropped_blocks = 0
    trimmed_blocks = 0

    for pha_file in pha_files:
        events = parse_pha_file(pha_file, errors)

        for block_idx, ev in enumerate(events):
            monthly_block_id = f"{pha_file.stem}_block{block_idx:04d}"

            raw_picks = ev.get("picks", [])
            if not raw_picks:
                dropped_blocks += 1
                continue

            # -------------------------------------------------------------
            # Sanity filter picks within the block
            # -------------------------------------------------------------
            filtered_picks = filter_pick_group(
                raw_picks,
                max_span_seconds=args.max_pick_span,
            )

            if not filtered_picks:
                dropped_blocks += 1
                errors.append(
                    f"{pha_file.name}:{monthly_block_id}: all picks rejected as outliers"
                )
                continue

            if len(filtered_picks) < len(raw_picks):
                trimmed_blocks += 1

            kept_blocks += 1

            # -------------------------------------------------------------
            # Emit rows
            # -------------------------------------------------------------
            for p in filtered_picks:
                rows.append({
                    # --- event provenance ---
                    "event_source": "monthly",
                    "monthly_file": pha_file.name,
                    "monthly_block_id": monthly_block_id,

                    # --- pick info ---
                    "seed_id": p.get("seed_id"),
                    "station": p.get("station"),
                    "channel": p.get("channel"),
                    "phase": p.get("phase"),
                    "pick_time": str(p.get("time")),

                    # --- attributes ---
                    "onset": p.get("onset"),
                    "first_motion": p.get("first_motion"),
                    "weight": p.get("weight"),
                })

    if not rows:
        raise SystemExit("No valid monthly picks produced")

    df = pd.DataFrame(rows)
    df.sort_values(
        ["monthly_block_id", "station", "pick_time"],
        inplace=True,
    )

    df.to_csv(out_csv, index=False)

    with open(error_log, "w") as f:
        for e in errors:
            f.write(e + "\n")

    print("\n=== STEP 21 SUMMARY ===")
    print(f"Monthly PHA files:      {len(pha_files)}")
    print(f"Blocks kept:            {kept_blocks}")
    print(f"Blocks trimmed:         {trimmed_blocks}")
    print(f"Blocks dropped:         {dropped_blocks}")
    print(f"Total picks written:    {len(df)}")
    print(f"CSV written:            {out_csv}")
    print(f"Errors logged:          {error_log}")


if __name__ == "__main__":
    main()