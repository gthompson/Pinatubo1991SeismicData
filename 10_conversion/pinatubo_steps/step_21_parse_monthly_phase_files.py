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

from pathlib import Path
import pandas as pd

from .pha_parser import (
    parse_pha_file,
    filter_pick_group,   # NEW — imported from refactored pha_parser.py
)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def run(
    *,
    pha_dir,
    out_csv,
    error_log,
    max_pick_span=30.0
):
    """Run this Pinatubo step with explicit notebook arguments. Return tables and results for inspection."""
    from types import SimpleNamespace
    args = SimpleNamespace(
        pha_dir=pha_dir,
        out_csv=out_csv,
        error_log=error_log,
        max_pick_span=max_pick_span,
    )

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

    pick_counter = 0

    for pha_file in pha_files:
        events = parse_pha_file(pha_file, errors)

        for block_idx, ev in enumerate(events):
            event_id = f"{pha_file.stem}_block{block_idx:04d}"

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
                    f"{pha_file.name}:{event_id}: all picks rejected as outliers"
                )
                continue

            if len(filtered_picks) < len(raw_picks):
                trimmed_blocks += 1

            kept_blocks += 1

            # -------------------------------------------------------------
            # Emit rows
            # -------------------------------------------------------------
            for p in filtered_picks:
                pick_counter += 1

                rows.append({
                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),                    
                    "event_id": event_id,
                    "event_id_source": "monthly_phase_block",
                    "pick_id": f"monpha_{event_id}_{pick_counter}",
                    "pha_file": pha_file.name,
                    "station": p.get("station"),
                    "channel": p.get("channel"),
                    "seed_id": p.get("seed_id"),
                    "phase": p.get("phase"),
                    "pick_time": str(p.get("time")),
                    "onset": p.get("onset"),
                    "first_motion": p.get("first_motion"),
                    "weight": p.get("weight"),
                })


    if not rows:
        raise SystemExit("No valid monthly picks produced")

    df = pd.DataFrame(rows)
    df.sort_values(
        ["event_id", "station", "pick_time"],
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
    return {k: v for k, v in locals().items() if (k.startswith("df") or k.startswith("catalog") or k.endswith("_rows") or k in ("events", "annotations", "errors", "stats", "diag", "merged", "suppressed", "matches", "rows", "unparsed"))}
