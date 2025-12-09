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
from datetime import datetime
import pandas as pd
from obspy.core.event import (
    Catalog, Event, Origin, Pick, WaveformStreamID, Arrival
)
from obspy import UTCDateTime


# ----------------------------------------------------------------------
# Utilities - FIXED CHARACTER POSITION PARSING
# (Based on legacy PHIVOLCS PHA format)
# ----------------------------------------------------------------------

def parse_phase_line(line):
    """
    Parse a single phase line using fixed character positions.
    
    Format (fixed positions):
    - Positions 0-3: Station code (3 chars)
    - Position 3: Orientation (Z/N/E/L)
    - Positions 4-8: P-arrival code (e.g., "IP  " or "E D 2")
    - Positions 8-24: Timestamp (YYMMDDHHMMSS.FF)
    - Position 35-40: S-wave marker (contains 'S' if S-wave present)
    - Before S-wave: S-wave delay
    - After S-wave: S-arrival code
    
    Returns dict with phase data or None if line is not a valid phase line.
    """
    line = line.rstrip()
    
    # Skip event separators
    if line.strip() in ("10", "100"):
        return None
    
    # Extract station code (first 3 chars)
    station = line[0:3].strip()
    if not station or station.lower() == 'xxx' or len(station) < 2:
        return None
    
    # Extract orientation (position 3)
    orientation = line[3:4].strip() if len(line) > 3 else ""
    
    # Extract P-arrival code (positions 4-8)
    p_arrival_code = line[4:8].replace(' ', '?') if len(line) > 4 else ""
    
    # Determine timestamp position and extract
    timestamp_str = None
    if len(line) > 8:
        if line[8] == ' ':
            # Standard case: positions 9-24
            timestamp_str = line[9:24].strip().replace(" ", "0") if len(line) >= 24 else None
        else:
            # Alternative: positions 8-23
            timestamp_str = line[8:23].strip().replace(" ", "0") if len(line) >= 23 else None
    
    if not timestamp_str:
        return None
    
    # Find S-wave marker position (look for 'S' in range 35-40)
    s_pos = 0
    s_positions = [i for i, char in enumerate(line) if char == 'S']
    s_positions = [pos for pos in s_positions if 35 <= pos <= 40]
    if len(s_positions) == 1:
        s_pos = s_positions[0]
    
    # Extract S-wave delay (if S-wave present)
    s_wave_delay = ""
    if s_pos > 0 and len(line) > s_pos - 7:
        s_wave_delay = line[s_pos-7:s_pos-1].strip()
    
    # Extract S-arrival code
    s_arrival_code = ""
    if s_pos > 0:
        if len(line) > s_pos + 3:
            s_arrival_code = line[s_pos-1:s_pos+3].replace(' ', '?')
        else:
            s_arrival_code = line[s_pos-1:].ljust(4).replace(' ', '?')
    
    # Check for P-wave (must have 'P' at position 1 of arrival code)
    has_p_wave = len(p_arrival_code) >= 2 and p_arrival_code[1] == "P"
    
    # Check for S-wave (marker 'S' must be present)
    has_s_wave = s_pos > 0
    
    # Convert timestamp string to UTCDateTime
    add_secs = 0
    if timestamp_str.endswith('60.00'):
        timestamp_str = timestamp_str.replace('60.00', '00.00')
        add_secs = 60
    if timestamp_str[-7:-5] == '60':
        timestamp_str = timestamp_str.replace('60', '00', 1)
        add_secs += 3600
    
    try:
        if len(timestamp_str) > 12 and '.' in timestamp_str:
            dt = datetime.strptime(timestamp_str, "%y%m%d%H%M%S.%f")
        else:
            dt = datetime.strptime(timestamp_str[:12], "%y%m%d%H%M%S")
        timestamp = UTCDateTime(dt) + add_secs
    except (ValueError, IndexError):
        return None
    
    # Determine SEED channel code from orientation
    if orientation in "ZNE":
        channel = f"EH{orientation}"
    elif orientation == "L":
        channel = "ELZ"
    else:
        channel = f"??{orientation}" if orientation else "EHZ"
    
    seed_id = f"XB.{station}..{channel}"
    
    # Build result dict with P and/or S picks
    results = []
    
    # P-wave pick
    if has_p_wave:
        p_arrival_code_clean = p_arrival_code.replace("?", " ")
        results.append({
            "station": station,
            "channel": channel,
            "seed_id": seed_id,
            "phase": "P",
            "time": timestamp,
            "onset": p_arrival_code_clean[0] if len(p_arrival_code_clean) > 0 and p_arrival_code_clean[0] in ["I", "E"] else None,
            "first_motion": p_arrival_code_clean[2] if len(p_arrival_code_clean) > 2 and p_arrival_code_clean[2] in ["U", "D"] else None,
            "weight": int(p_arrival_code_clean[3]) if len(p_arrival_code_clean) > 3 and p_arrival_code_clean[3].isdigit() else None,
        })
    
    # S-wave pick
    if has_s_wave and s_wave_delay and s_wave_delay.replace(".", "").replace("-", "").isdigit():
        s_arrival_code_clean = s_arrival_code.replace("?", " ")
        s_time = timestamp + float(s_wave_delay)
        results.append({
            "station": station,
            "channel": channel,
            "seed_id": seed_id,
            "phase": "S",
            "time": s_time,
            "onset": s_arrival_code_clean[0] if len(s_arrival_code_clean) > 0 and s_arrival_code_clean[0] in ["I", "E"] else None,
            "first_motion": s_arrival_code_clean[2] if len(s_arrival_code_clean) > 2 and s_arrival_code_clean[2] in ["U", "D"] else None,
            "weight": int(s_arrival_code_clean[3]) if len(s_arrival_code_clean) > 3 and s_arrival_code_clean[3].isdigit() else None,
        })
    
    return results if results else None


# ----------------------------------------------------------------------
# Main parser
# ----------------------------------------------------------------------

def parse_pha_file(path, errors):
    """
    Parse a single PHA file into a list of events.
    
    Events are separated by lines containing just "10" or "100".
    Each event contains a list of picks parsed from subsequent lines.
    
    Returns list of dicts:
    {
      "picks": [ {"station": ..., "phase": ..., "time": ..., ...}, ... ]
    }
    """
    events = []
    current_picks = []

    with open(path, "r", errors="ignore") as f:
        for line_num, raw_line in enumerate(f, 1):
            line = raw_line.rstrip()
            
            # Event separator: "10" or "100"
            if line.strip() in ("10", "100"):
                if current_picks:
                    # Find origin time from the earliest P-wave pick
                    origin_time = min(
                        (p["time"] for p in current_picks if p["phase"] == "P"),
                        default=None
                    )
                    if not origin_time:
                        # Fall back to earliest pick time
                        origin_time = min((p["time"] for p in current_picks), default=None)
                    
                    if origin_time:
                        events.append({"origin_time": origin_time, "picks": current_picks})
                    current_picks = []
                continue
            
            # Parse the line
            parse_results = parse_phase_line(line)
            if parse_results:
                # parse_phase_line returns a list of dicts (one per phase)
                if isinstance(parse_results, list):
                    current_picks.extend(parse_results)
                else:
                    current_picks.append(parse_results)
            elif line.strip():  # Only log non-empty lines that couldn't be parsed
                errors.append(f"{path.name}:{line_num}: {line}")

    # Handle last event if file doesn't end with separator
    if current_picks:
        origin_time = min(
            (p["time"] for p in current_picks if p["phase"] == "P"),
            default=None
        )
        if not origin_time:
            origin_time = min((p["time"] for p in current_picks), default=None)
        
        if origin_time:
            events.append({"origin_time": origin_time, "picks": current_picks})

    return events


# ----------------------------------------------------------------------
# Convert parsed dicts → ObsPy Catalog + CSV
# ----------------------------------------------------------------------

def build_quakeml_and_table(all_events):
    """
    Convert parsed event dicts into ObsPy Catalog and CSV table.
    """
    cat = Catalog()
    rows = []

    for ev in all_events:
        origin = Origin(time=ev["origin_time"])
        event = Event(origins=[origin])

        for pick_dict in ev["picks"]:
            # Create ObsPy Pick object
            pick = Pick(
                time=pick_dict["time"],
                waveform_id=WaveformStreamID(
                    network_code="XB",
                    station_code=pick_dict["station"],
                    location_code="",
                    channel_code=pick_dict["channel"]
                ),
                phase_hint=pick_dict["phase"]
            )
            event.picks.append(pick)

            # Create Arrival object linking to origin
            arrival = Arrival(
                pick_id=pick.resource_id,
                phase=pick_dict["phase"],
                time_weight=pick_dict.get("weight", 1) if pick_dict.get("weight") is not None else 1
            )
            origin.arrivals.append(arrival)

            # Add row to CSV table
            rows.append({
                "event_origin": str(origin.time),
                "station": pick_dict["station"],
                "channel": pick_dict["channel"],
                "phase": pick_dict["phase"],
                "pick_time": str(pick_dict["time"]),
                "pick_offset_from_origin": (pick_dict["time"] - origin.time),
                "onset": pick_dict.get("onset"),
                "first_motion": pick_dict.get("first_motion"),
                "weight": pick_dict.get("weight")
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
    ap.add_argument("--out-dir", required=True,
                    help="Output directory for CSV + QuakeML")
    args = ap.parse_args()

    pha_dir = Path(args.pha_dir)
    outdir = Path(args.out_dir)
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