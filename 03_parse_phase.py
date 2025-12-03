#!/usr/bin/env python3
"""
03_parse_phase.py

Parse PHIVOLCS/VDAP PHA files (manually picked phase files)
from the 1991 Pinatubo seismic sequence.

Outputs:
--------
1. pha_events.csv     - flattened table of all picks
2. pha_catalog.xml    - ObsPy Catalog with Event/Origin/Picks
3. pha_parse_errors.log

Assumptions:
------------
• PHA files follow SEISAN-style PHA block format
• Block starts with a header line containing event time
• Followed by station lines containing P/S picks
• Blank line separates events

This parser is intentionally strict but tolerant:
- missing fields are logged but not fatal
- malformed blocks are skipped

"""

import argparse
import os
import re
from pathlib import Path
import pandas as pd
from obspy.core.event import (
    Catalog, Event, Origin, Pick, WaveformStreamID, Arrival
)
from obspy import UTCDateTime


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------

PHA_HEADER_RE = re.compile(
    r"^(\d{4})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+([0-9.]+)"
)

STATION_LINE_RE = re.compile(
    r"^(\w{2,4})\s+([PpSs])\s+([0-9.]+)\s+([0-9.]+)?"
)


def parse_header(line):
    """
    Parse event header line from PHA block.

    Example:
        1991 06 12 08 51 00.22  LP event...
    """
    m = PHA_HEADER_RE.match(line)
    if not m:
        return None

    year, month, day, hour, minute, sec = m.groups()
    try:
        t = UTCDateTime(
            int(year), int(month), int(day),
            int(hour), int(minute), float(sec)
        )
    except Exception:
        return None

    return t


def parse_phase_line(line):
    """
    Parse a station pick line from PHA.

    Expected:
        STN   P  12.34  0.7
        STN   S  15.28
    """
    m = STATION_LINE_RE.match(line)
    if not m:
        return None

    sta, ph, time, weight = m.groups()
    return {
        "station": sta,
        "phase": ph.upper(),
        "time": float(time),
        "weight": float(weight) if weight else None
    }


# ----------------------------------------------------------------------
# Main parser
# ----------------------------------------------------------------------

def parse_pha_file(path, errors):
    """
    Parse a single PHA file into a list of events, each containing:
    {
      "origin_time": UTCDateTime,
      "picks": [ ... ]
    }
    """
    events = []
    current_event = None

    with open(path, "r", errors="ignore") as f:
        for raw in f:
            line = raw.strip()

            # Skip blank lines: close event block
            if not line:
                if current_event:
                    events.append(current_event)
                    current_event = None
                continue

            # Header line?
            t0 = parse_header(line)
            if t0:
                if current_event:
                    events.append(current_event)
                current_event = {"origin_time": t0, "picks": []}
                continue

            # Station pick line?
            if current_event:
                p = parse_phase_line(line)
                if p:
                    current_event["picks"].append(p)
                else:
                    errors.append(f"Unparsed line in {path}: {line}")
            else:
                errors.append(f"Line outside event block in {path}: {line}")

    # Append last event if missing terminator
    if current_event:
        events.append(current_event)

    return events


# ----------------------------------------------------------------------
# Convert parsed dicts → ObsPy Catalog + CSV
# ----------------------------------------------------------------------

def build_quakeml_and_table(all_events):
    cat = Catalog()
    rows = []

    for ev in all_events:
        origin = Origin(time=ev["origin_time"])
        event = Event(origins=[origin])

        for p in ev["picks"]:
            # Compute absolute pick time: origin + offset
            pt = origin.time + p["time"]

            pick = Pick(
                time=pt,
                waveform_id=WaveformStreamID(
                    station_code=p["station"],
                    network_code="XB",   # fixed for Pinatubo
                    location_code="",
                    channel_code="EHZ"   # placeholder; station orientation unknown here
                ),
                phase_hint=p["phase"]
            )

            # Add to event
            event.picks.append(pick)
            arr = Arrival(
                pick_id=pick.resource_id,
                phase=p["phase"],
                time_weight=p["weight"] if p["weight"] is not None else 1.0
            )
            origin.arrivals.append(arr)

            # Table row
            rows.append({
                "event_origin": str(origin.time),
                "station": p["station"],
                "phase": p["phase"],
                "pick_offset": p["time"],
                "absolute_pick": str(pt),
                "weight": p["weight"]
            })

        cat.append(event)

    df = pd.DataFrame(rows)
    return cat, df


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Parse Pinatubo PHA phase files.")
    ap.add_argument("--pha-dir", required=True,
                    help="Directory containing *.PHA files")
    ap.add_argument("--outdir", required=True,
                    help="Output directory for CSV + QuakeML")
    args = ap.parse_args()

    pha_dir = Path(args.pha_dir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    pha_files = sorted(pha_dir.glob("*.PHA"))
    if not pha_files:
        raise SystemExit(f"No PHA files found in {pha_dir}")

    errors = []
    all_events = []

    for fpath in pha_files:
        events = parse_pha_file(fpath, errors)
        all_events.extend(events)
        print(f"Parsed {len(events)} events from {fpath.name}")

    # Build catalog + table
    catalog, df = build_quakeml_and_table(all_events)

    # Outputs
    csv_path = outdir / "pha_events.csv"
    qml_path = outdir / "pha_catalog.xml"
    log_path = outdir / "pha_parse_errors.log"

    df.to_csv(csv_path, index=False)
    catalog.write(qml_path, format="QUAKEML")

    with open(log_path, "w") as f:
        for line in errors:
            f.write(line + "\n")

    print("\nSummary")
    print("-------")
    print(f"Total events parsed:   {len(all_events)}")
    print(f"Total picks:           {len(df)}")
    print(f"CSV:                   {csv_path}")
    print(f"QuakeML catalog:       {qml_path}")
    print(f"Errors logged:         {log_path}")


if __name__ == "__main__":
    main()