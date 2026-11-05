#!/usr/bin/env python3
"""
40_build_hypocenter_index.py

STEP 40 of the Pinatubo FAIR pipeline

Build a tolerant hypocenter index from a HYPO71-style summary file.

This step:
  • parses origin time, location, depth, magnitude
  • preserves provenance and raw text
  • logs unparsed lines
  • DOES NOT create QuakeML

QuakeML is deferred until hypocenters from all sources
have been associated into unified seismic events.

Outputs:
--------
• CSV hypocenter index
• text log of unparsed lines
"""

from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from flovopy.core.vdap import parse_hypo71_line as _parse_vdap_hypo71_line

PARSER_ID = "vdap_hypo71_adapter_v2"


# ============================================================================
# HYPO71-LIKE LINE PARSER
# ============================================================================

def parse_hypo71_line(line):
    """Adapt the authoritative VDAP parser to the unchanged Step-40 CSV schema."""
    value = _parse_vdap_hypo71_line(line)
    if value is None:
        return None
    return dict(origin_time=value['origin_time'].isoformat(),latitude=value['latitude'],
        longitude=value['longitude'],depth_km=value['depth_km'],magnitude=value['magnitude'],
        nass=value['n_ass_raw'],rms=value['time_residual_raw'])



# ============================================================================
# MAIN
# ============================================================================

def run(
    *,
    summary_file,
    out_csv,
    error_log
):
    """Run this Pinatubo step with explicit notebook arguments. Return tables and results for inspection."""
    from types import SimpleNamespace
    args = SimpleNamespace(
        summary_file=summary_file,
        out_csv=out_csv,
        error_log=error_log,
    )

    src = Path(args.summary_file)
    if not src.exists():
        raise SystemExit(f"File not found: {src}")

    rows = []
    unparsed: List[str] = []

    with src.open("r", errors="ignore") as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.rstrip("\n")
            if not line.strip():
                continue

            parsed = parse_hypo71_line(line)
            if parsed is None:
                unparsed.append(f"{lineno}: {raw}")
                continue

            parsed.update({
                "source_file": src.name,
                "source_line": lineno,
                "parser": PARSER_ID,
                "raw_line": line,
            })
            rows.append(parsed)

    # --- Write outputs ---
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["origin_time", "latitude", "longitude", "depth_km", "magnitude", "nass", "rms", "source_file", "source_line", "parser", "raw_line"]).to_csv(out_csv, index=False)

    err_path = Path(args.error_log)
    err_path.parent.mkdir(parents=True, exist_ok=True)
    with err_path.open("w") as f:
        for l in unparsed:
            f.write(l if l.endswith("\n") else l + "\n")

    # --- Summary ---
    print("\nSTEP 40 — HYPOCENTER INDEX BUILT")
    print("--------------------------------")
    print(f"Source file:     {src}")
    print(f"Parsed rows:     {len(rows)}")
    print(f"Unparsed lines:  {len(unparsed)}")
    print(f"Index CSV:       {out_csv}")
    print(f"Error log:       {err_path}")
    return {k: v for k, v in locals().items() if (k.startswith("df") or k.startswith("catalog") or k.endswith("_rows") or k in ("events", "annotations", "errors", "stats", "diag", "merged", "suppressed", "matches", "rows", "unparsed"))}
