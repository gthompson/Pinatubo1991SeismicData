#!/usr/bin/env python3
"""
05_build_hypocenter_index.py

STEP 05 of the Pinatubo FAIR pipeline

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

import argparse
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from obspy import UTCDateTime

PARSER_ID = "hypo71_pinatubo_v1"


# ============================================================================
# HYPO71-LIKE LINE PARSER
# ============================================================================

def parse_hypo71_line(line: str) -> Optional[Dict]:
    """
    Parse one HYPO71 / Pinatubo-style summary line.

    Tolerant of:
      • variable spacing
      • minute / second rollover (>=60)

    Returns dict or None if parsing fails.
    """
    try:
        # --- Date / time ---
        year = int(line[0:2])
        month = int(line[2:4])
        day = int(line[4:6])

        hour = int(line[7:9] or 0)
        minute = int(line[9:11] or 0)
        seconds = float(line[12:17] or 0)

        year = year + 1900 if year >= 70 else year + 2000

        extra = 0.0
        if seconds >= 60.0:
            seconds %= 60.0
            extra += 60.0
        if minute >= 60:
            minute %= 60
            extra += 3600.0

        ot = UTCDateTime(year, month, day, hour, minute, seconds) + extra

        # --- Latitude ---
        lat_deg = int(line[17:20])
        lat_hem = line[20].lower()
        lat_min = float(line[21:26])

        lat = lat_deg + lat_min / 60.0
        if lat_hem == "s":
            lat *= -1

        # --- Longitude ---
        lon_deg = int(line[27:30])
        lon_hem = line[30].lower()
        lon_min = float(line[31:36])

        lon = lon_deg + lon_min / 60.0
        if lon_hem == "w":
            lon *= -1

        # --- Depth & magnitude ---
        depth_km = float(line[37:43])
        magnitude = float(line[44:50])

        # --- Optional fields ---
        nass = line[51:53].strip() or None
        rms = line[62:].strip() or None

        return {
            "origin_time": ot.isoformat(),
            "latitude": lat,
            "longitude": lon,
            "depth_km": depth_km,
            "magnitude": magnitude,
            "nass": nass,
            "rms": rms,
        }

    except Exception:
        return None


# ============================================================================
# MAIN
# ============================================================================

def main():
    ap = argparse.ArgumentParser(
        description="STEP 05: Build hypocenter index from HYPO71-style summary file"
    )
    ap.add_argument("--summary-file", required=True,
                    help="Input HYPO71-style summary file")
    ap.add_argument("--out-csv", required=True,
                    help="Output hypocenter index CSV")
    ap.add_argument("--error-log", required=True,
                    help="Unparsed lines log")

    args = ap.parse_args()

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
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    err_path = Path(args.error_log)
    err_path.parent.mkdir(parents=True, exist_ok=True)
    with err_path.open("w") as f:
        for l in unparsed:
            f.write(l if l.endswith("\n") else l + "\n")

    # --- Summary ---
    print("\nSTEP 05 — HYPOCENTER INDEX BUILT")
    print("--------------------------------")
    print(f"Source file:     {src}")
    print(f"Parsed rows:     {len(rows)}")
    print(f"Unparsed lines:  {len(unparsed)}")
    print(f"Index CSV:       {out_csv}")
    print(f"Error log:       {err_path}")


if __name__ == "__main__":
    main()