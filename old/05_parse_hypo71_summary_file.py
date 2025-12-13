#!/usr/bin/env python3
"""
05_parse_hypo71_summary_file.py

STEP 05 of the Pinatubo FAIR pipeline

Parse a HYPO71-style earthquake summary file (e.g. PINAALL.SUM)
into:
  • a simple flat CSV hypocenter index
  • a minimal QuakeML catalog (origins + magnitudes only)
  • a log of unparsed lines

This step is intentionally tolerant:
 - lines that cannot be parsed are logged but do not halt the run
 - only hypocenter information is extracted (origin, lat/lon, depth, mag)
 - arrival / phase information is explicitly ignored

Inputs
------
--summary-file   Full path to the HYPO71 summary file to parse

Outputs (launcher defines paths)
------
--out-xml        QuakeML catalog
--out-csv        Event summary CSV
--error-log      Unparsed lines

"""

import argparse
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import Catalog, Event, Origin, Magnitude, Comment


# ============================================================================
# HYPO71 LINE PARSER
# ============================================================================

def parse_hypo71_line(line: str) -> Optional[Dict]:
    """
    Parse one HYPO71 summary line.

    Expected classic fixed-width format:

    YYMMDD HHMM SS.SS LAT H LATM LON H LONM DEPTH MAG NASS ... RMS

    Returns dictionary or None if parsing fails.
    """
    try:
        # Date / time
        year = int(line[0:2])
        month = int(line[2:4])
        day = int(line[4:6])
        hour = int(line[7:9] or 0)
        minute = int(line[9:11] or 0)
        seconds = float(line[12:17] or 0)

        # Expand 2-digit year
        year = year + 1900 if year >= 70 else year + 2000

        # Handle HYPO71-style rollover seconds (e.g., SS >= 60)
        extra_time = 0.0
        if seconds >= 60.0:
            seconds = seconds % 60
            extra_time += 60.0
        if minute >= 60.0:
            minute = minute % 60
            extra_time += 3600.0           

        ot = UTCDateTime(year, month, day, hour, minute, seconds) + extra_time

        # Latitude
        lat_deg = int(line[17:20])
        lat_hem = line[20].upper()
        lat_min = float(line[21:26])

        lat = lat_deg + lat_min / 60.0
        if lat_hem == "S":
            lat *= -1

        # Longitude
        lon_deg = int(line[27:30])
        lon_hem = line[30].upper()
        lon_min = float(line[31:36])

        lon = lon_deg + lon_min / 60.0
        if lon_hem == "W":
            lon *= -1

        # Depth & magnitude
        depth_km = float(line[37:43])
        magnitude = float(line[44:50])

        # Optional fields
        nass = line[51:53].strip() or None
        rms = line[62:].strip() or None

        return {
            "origin_time": ot,
            "latitude": lat,
            "longitude": lon,
            "depth_km": depth_km,
            "magnitude": magnitude,
            "nass": nass,
            "rms": rms,
        }

    except Exception as e:
        print(f"Failed to parse line: {line} ({e})")
        return None


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="STEP 05: Parse HYPO71 summary file into QuakeML + CSV"
    )

    parser.add_argument("--summary-file", required=True,
                        help="Path to HYPO71 summary file (e.g., PINAALL.SUM)")

    parser.add_argument("--out-xml", required=True,
                        help="Output QuakeML path")

    parser.add_argument("--out-csv", required=True,
                        help="Output CSV path")

    parser.add_argument("--error-log", required=True,
                        help="Output log for unparsed lines")

    parser.add_argument("--verbose", action="store_true",
                        help="Verbose output")

    args = parser.parse_args()

    summary_path = Path(args.summary_file)
    if not summary_path.exists():
        raise SystemExit(f"HYPO71 summary file not found: {summary_path}")

    catalog = Catalog()
    rows = []
    unparsed: List[str] = []

    # ----------------------------------------------------------------------
    # Parse file line-by-line
    # ----------------------------------------------------------------------
    with summary_path.open("r", errors="ignore") as f:
        for lineno, raw_line in enumerate(f, start=1):
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue

            parsed = parse_hypo71_line(line)
            if parsed is None:
                unparsed.append(f"{lineno}: {raw_line}")
                continue

            # Build ObsPy objects
            ev = Event()
            ori = Origin(
                time=parsed["origin_time"],
                latitude=parsed["latitude"],
                longitude=parsed["longitude"],
                depth=parsed["depth_km"] * 1000.0,
            )
            mag = Magnitude(mag=parsed["magnitude"])

            if parsed["nass"] is not None:
                ori.comments.append(Comment(text=f"nass: {parsed['nass']}"))
            if parsed["rms"] is not None:
                ori.comments.append(Comment(text=f"rms: {parsed['rms']}"))

            ev.origins.append(ori)
            ev.magnitudes.append(mag)
            catalog.append(ev)

            rows.append({
                "origin_time": str(parsed["origin_time"]),
                "latitude": parsed["latitude"],
                "longitude": parsed["longitude"],
                "depth_km": parsed["depth_km"],
                "magnitude": parsed["magnitude"],
                "nass": parsed["nass"],
                "rms": parsed["rms"],
                "source_file": str(summary_path),
            })

    # ----------------------------------------------------------------------
    # Write outputs
    # ----------------------------------------------------------------------
    pd.DataFrame(rows).to_csv(args.out_csv, index=False)
    catalog.write(args.out_xml, format="QUAKEML")

    with open(args.error_log, "w") as f:
        for line in unparsed:
            f.write(line if line.endswith("\n") else line + "\n")

    # ----------------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------------
    print("\nSTEP 05 — HYPO71 SUMMARY PARSE COMPLETE")
    print("--------------------------------------")
    print(f"File parsed:       {summary_path}")
    print(f"Events extracted:  {len(catalog)}")
    print(f"CSV written:       {args.out_csv}")
    print(f"QuakeML written:   {args.out_xml}")
    print(f"Unparsed lines:    {len(unparsed)} → {args.error_log}")


if __name__ == "__main__":
    main()