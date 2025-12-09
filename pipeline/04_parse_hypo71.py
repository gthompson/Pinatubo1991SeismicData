#!/usr/bin/env python3
"""
04_parse_hypo71.py

Parse legacy VDAP / HYPO71 earthquake summary files for the 1991
Mount Pinatubo seismic network into a modern ObsPy Catalog and CSV.

This is a standalone script that reimplements the HYPO71 parsing
logic from flovopy.core.vdap (parse_hypo71_line / parse_hypo71_file),
but without importing flovopy.

Inputs
------
- One or more HYPO71 summary files (fixed-width format), either:
    * A single file path, or
    * A directory + glob pattern (e.g. '*.SUM')

Outputs
-------
- hypo71_catalog.xml  : QuakeML event catalog
- hypo71_events.csv   : Tabular event summary (origin, lat/lon, depth, mag, etc.)
- hypo71_parse_errors.log : List of lines that failed to parse

Example
-------
Parse a single file:
    python 04_parse_hypo71.py --input /path/to/PINAALL.SUM

Parse all *.SUM files in a directory:
    python 04_parse_hypo71.py --input /path/to/hypo71_dir --glob "*.SUM"
"""

import argparse
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import Catalog, Event, Origin, Magnitude, Comment


# ============================================================================
# HYPO71 PARSERS (adapted from flovopy.core.vdap)
# ============================================================================

def parse_hypo71_line(line: str) -> Optional[Dict]:
    """
    Parses a single line of HYPO71 earthquake location output.

    Assumes the classic HYPO71 fixed-width format used in many VDAP catalogs.
    Returns a dictionary of origin time, lat/lon, depth, magnitude, etc.,
    or None if parsing fails.

    This implementation mirrors flovopy.core.vdap.parse_hypo71_line.
    """
    try:
        # Date and time
        year = int(line[0:2])
        month = int(line[2:4])
        day = int(line[4:6])

        hour = int(line[7:9]) if line[7:9].strip() else 0
        minute = int(line[9:11]) if line[9:11].strip() else 0
        seconds = float(line[12:17]) if line[12:17].strip() else 0.0

        # Latitude: degrees, minutes, hemisphere
        lat_deg = int(line[17:20].strip())
        lat_hem = line[20].strip().upper()
        lat_min = float(line[21:26].strip())

        # Longitude: degrees, minutes, hemisphere
        lon_deg = int(line[27:30].strip())
        lon_hem = line[30].strip().upper()
        lon_min = float(line[31:36].strip())

        # Depth (km) and magnitude
        depth = float(line[37:43].strip())
        magnitude = float(line[44:50].strip())

        # Number of associated phases, time residual (RMS)
        n_ass = int(line[51:53].strip())
        time_residual = float(line[62:].strip())

        # Expand 2-digit year
        year = year + 1900 if year >= 70 else year + 2000

        # Handle minute == 60 edge case
        add_seconds = 0
        if minute == 60:
            minute = 0
            add_seconds = 60

        origin_time = UTCDateTime(year, month, day, hour, minute, seconds) + add_seconds

        # Convert to decimal degrees
        latitude = lat_deg + lat_min / 60.0
        if lat_hem == "S":
            latitude = -latitude

        longitude = lon_deg + lon_min / 60.0
        if lon_hem == "W":
            longitude = -longitude

        return {
            "origin_time": origin_time,
            "latitude": latitude,
            "longitude": longitude,
            "depth_km": depth,
            "magnitude": magnitude,
            "n_ass": n_ass,
            "time_residual": time_residual,
        }

    except Exception as e:
        # For a batch script, we do not want to crash on a single bad line
        print(f"Failed to parse HYPO71 line: '{line.strip()}' | Error: {e}")
        return None


def parse_hypo71_file(file_path: Path) -> Tuple[Catalog, List[str]]:
    """
    Parses a HYPO71 file into an ObsPy Catalog and returns a list of unparsed lines.

    Mirrors flovopy.core.vdap.parse_hypo71_file, but is fully standalone.
    """
    catalog = Catalog()
    unparsed_lines: List[str] = []

    with open(file_path, "r") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line.strip():
                # Skip completely blank lines
                continue

            event_data = parse_hypo71_line(line)
            if event_data is None:
                unparsed_lines.append(raw_line)
                continue

            # Build ObsPy Event objects
            event = Event()

            origin = Origin(
                time=event_data["origin_time"],
                latitude=event_data["latitude"],
                longitude=event_data["longitude"],
                depth=event_data["depth_km"] * 1000.0,  # km → m
            )

            mag = Magnitude(mag=event_data["magnitude"])

            # Store some metadata as comments on the origin
            origin.comments.append(
                Comment(text=f"n_ass: {event_data['n_ass']}")
            )
            origin.comments.append(
                Comment(text=f"time_residual: {event_data['time_residual']} sec")
            )

            event.origins.append(origin)
            event.magnitudes.append(mag)
            catalog.append(event)

    print(f"{file_path}: Parsed {len(catalog)} events | Unparsed lines: {len(unparsed_lines)}")
    return catalog, unparsed_lines


# ============================================================================
# UTILITY: MERGE MULTIPLE CATALOGS AND WRITE OUTPUTS
# ============================================================================

def merge_catalogs(catalogs: List[Catalog]) -> Catalog:
    """
    Merge a list of ObsPy Catalogs into one.
    """
    merged = Catalog()
    for cat in catalogs:
        for ev in cat:
            merged.append(ev)
    return merged


def catalog_to_dataframe(catalog: Catalog, source_files: List[str]) -> pd.DataFrame:
    """
    Convert a catalog to a flat pandas DataFrame.

    Assumes:
    - One origin per event (index 0)
    - One magnitude per event (index 0)

    source_files: list of file paths (same length/order as events)
    """
    rows = []
    for idx, (event, src) in enumerate(zip(catalog, source_files)):
        if event.origins:
            origin = event.origins[0]
        else:
            origin = None
        if event.magnitudes:
            mag = event.magnitudes[0]
        else:
            mag = None

        rows.append(
            {
                "event_index": idx,
                "source_file": src,
                "origin_time": getattr(origin, "time", None),
                "latitude": getattr(origin, "latitude", None),
                "longitude": getattr(origin, "longitude", None),
                "depth_km": getattr(origin, "depth", None) / 1000.0 if getattr(origin, "depth", None) is not None else None,
                "magnitude": getattr(mag, "mag", None),
            }
        )

    return pd.DataFrame(rows)


# ============================================================================
# MAIN SCRIPT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Parse HYPO71 summary files into QuakeML + CSV."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to a HYPO71 file OR a directory containing HYPO71 files.",
    )
    parser.add_argument(
        "--glob",
        default="*.SUM",
        help="Glob pattern if --input is a directory (default: *.SUM).",
    )
    parser.add_argument(
        "--out-csv",
        default="hypo71_events.csv",
        help="Output CSV filename (default: hypo71_events.csv).",
    )
    parser.add_argument(
        "--out-xml",
        default="hypo71_catalog.xml",
        help="Output QuakeML filename (default: hypo71_catalog.xml).",
    )
    parser.add_argument(
        "--error-log",
        default="hypo71_parse_errors.log",
        help="Output log file for unparsed HYPO71 lines.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print additional progress information.",
    )

    args = parser.parse_args()
    input_path = Path(args.input)

    # ----------------------------------------------------------------------
    # Discover files
    # ----------------------------------------------------------------------
    if input_path.is_file():
        files = [input_path]
    elif input_path.is_dir():
        files = sorted(input_path.glob(args.glob))
    else:
        raise SystemExit(f"--input path does not exist: {input_path}")

    if not files:
        raise SystemExit(f"No HYPO71 files found under {input_path} with pattern {args.glob}")

    if args.verbose:
        print(f"Found {len(files)} HYPO71 file(s) to parse.")

    # ----------------------------------------------------------------------
    # Parse all files
    # ----------------------------------------------------------------------
    catalogs: List[Catalog] = []
    source_files_for_events: List[str] = []
    all_unparsed_lines: List[str] = []

    for fpath in files:
        if args.verbose:
            print(f"Parsing {fpath} ...")
        cat, unparsed = parse_hypo71_file(fpath)

        catalogs.append(cat)
        all_unparsed_lines.extend(
            [f"# {fpath}\n", *[ln if ln.endswith("\n") else ln + "\n" for ln in unparsed]]
        )

        # Track source file per event for the CSV
        source_files_for_events.extend([str(fpath)] * len(cat))

    # ----------------------------------------------------------------------
    # Merge catalogs and write outputs
    # ----------------------------------------------------------------------
    merged_catalog = merge_catalogs(catalogs)
    n_events = len(merged_catalog)

    if args.verbose:
        print(f"Merged catalog contains {n_events} events.")

    # Write QuakeML
    out_xml_path = Path(args.out_xml)
    merged_catalog.write(str(out_xml_path), format="QUAKEML")
    print(f"✅ Wrote QuakeML catalog: {out_xml_path} (events: {n_events})")

    # Write CSV
    df = catalog_to_dataframe(merged_catalog, source_files_for_events)
    out_csv_path = Path(args.out_csv)
    df.to_csv(out_csv_path, index=False)
    print(f"✅ Wrote event summary CSV: {out_csv_path} (rows: {len(df)})")

    # Write parse error log if any
    out_err_path = Path(args.error_log)
    if all_unparsed_lines:
        with open(out_err_path, "w") as f:
            f.writelines(all_unparsed_lines)
        print(f"⚠️  Wrote parse error log: {out_err_path} (lines: {len(all_unparsed_lines)})")
    else:
        print("✅ No parse errors encountered (no error log written).")


if __name__ == "__main__":
    main()