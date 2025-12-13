#!/usr/bin/env python3
"""
04_parse_hypo71_summary_file.py

Parse a HYPO71-style earthquake summary file (e.g. PINAALL.SUM)
into a modern ObsPy Catalog and a simple flat CSV index.

This step is intentionally tolerant:
 - lines that cannot be parsed are logged, but do not halt the run
 - only hypocenter information is extracted (origin, lat/lon, depth, mag)
 - HYPO71 arrival/phase information is ignored here

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
        # Time fields
        year = int(line[0:2])
        month = int(line[2:4])
        day = int(line[4:6])
        hour = int(line[7:9] or 0)
        minute = int(line[9:11] or 0)
        seconds = float(line[12:17] or 0)

        # Year expansion (70–99 → 1970–1999, 00–69 → 2000–2069)
        year = year + 1900 if year >= 70 else year + 2000

        # Latitude
        lat_deg = int(line[17:20])
        lat_hem = line[20].upper()
        lat_min = float(line[21:26])

        # Longitude
        lon_deg = int(line[27:30])
        lon_hem = line[30].upper()
        lon_min = float(line[31:36])

        # Depth & magnitude
        depth_km = float(line[37:43])
        magnitude = float(line[44:50])

        # Optional fields
        nass = line[51:53].strip() or None
        rms = line[62:].strip() or None

        # Construct UTCDateTime
        ot = UTCDateTime(year, month, day, hour, minute, seconds)

        # Convert coordinates
        lat = lat_deg + lat_min / 60.0
        if lat_hem == "S":
            lat *= -1

        lon = lon_deg + lon_min / 60.0
        if lon_hem == "W":
            lon *= -1

        return {
            "origin_time": ot,
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
    parser = argparse.ArgumentParser(
        description="Parse a HYPO71 summary file into QuakeML + CSV"
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

    # Storage
    catalog = Catalog()
    rows = []
    unparsed: List[str] = []

    # ----------------------------------------------------------------------
    # Read & parse file line-by-line
    # ----------------------------------------------------------------------
    with summary_path.open("r") as f:
        for lineno, raw_line in enumerate(f, start=1):
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue

            parsed = parse_hypo71_line(line)
            if parsed is None:
                unparsed.append(f"{lineno}: {raw_line}")
                continue

            # Build ObsPy Event
            ev = Event()
            ori = Origin(
                time=parsed["origin_time"],
                latitude=parsed["latitude"],
                longitude=parsed["longitude"],
                depth=parsed["depth_km"] * 1000.0,  # km → m
            )
            mag = Magnitude(mag=parsed["magnitude"])

            # Optional metadata
            if parsed["nass"] is not None:
                ori.comments.append(Comment(text=f"nass: {parsed['nass']}"))
            if parsed["rms"] is not None:
                ori.comments.append(Comment(text=f"rms: {parsed['rms']}"))

            ev.origins.append(ori)
            ev.magnitudes.append(mag)
            catalog.append(ev)

            # CSV row
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
    # CSV
    df = pd.DataFrame(rows)
    df.to_csv(args.out_csv, index=False)

    # QuakeML
    catalog.write(args.out_xml, format="QUAKEML")

    # Error log
    with open(args.error_log, "w") as f:
        for line in unparsed:
            f.write(line if line.endswith("\n") else line + "\n")

    # ----------------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------------
    print("\nHYPO71 SUMMARY PARSE COMPLETE")
    print("--------------------------------")
    print(f"File parsed:       {summary_path}")
    print(f"Events extracted:  {len(catalog)}")
    print(f"CSV written:       {args.out_csv}")
    print(f"QuakeML written:   {args.out_xml}")
    print(f"Unparsed lines:    {len(unparsed)} → {args.error_log}")


if __name__ == "__main__":
    main()