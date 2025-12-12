#!/usr/bin/env python3
"""
03_parse_monthly_phase_files.py

STEP 03 of the Pinatubo FAIR pipeline:
Parse PHIVOLCS/VDAP monthly PHA files into:
    • 03_pha_events.csv
    • 03_pha_catalog.xml
    • 03_pha_parse_errors.log

Input:
    --pha-dir   Directory containing *.PHA files (LEGACY)
Output:
    --out-dir   FAIR/pha directory (all outputs live here)

This version does NOT write anything into LEGACY.
"""

import argparse
import re
from pathlib import Path
import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import (
    Catalog, Event, Origin, Pick, WaveformStreamID, Arrival
)

# Import shared parsing logic
from pha_parser import parse_pha_file as parse_pha_file_from_module

# ----------------------------------------------------------------------
# Convert parsed dict structures → ObsPy Catalog + flattened CSV
# ----------------------------------------------------------------------

def build_quakeml_and_table(all_events):
    catalog = Catalog()
    rows = []

    for ev in all_events:
        origin = Origin(time=ev["origin_time"])
        event = Event(origins=[origin])

        for pick_dict in ev["picks"]:
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

            arrival = Arrival(
                pick_id=pick.resource_id,
                phase=pick_dict["phase"],
                time_weight=pick_dict.get("weight", 1)
            )
            origin.arrivals.append(arrival)

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

        catalog.append(event)

    return catalog, pd.DataFrame(rows)


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Parse Pinatubo PHA monthly files.")
    ap.add_argument("--pha-dir", required=True, help="Directory containing *.PHA input files")
    ap.add_argument("--out-dir", required=True, help="Output directory under FAIR")
    args = ap.parse_args()

    pha_dir = Path(args.pha_dir)
    outdir = Path(args.out_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    pha_files = sorted(pha_dir.glob("*.PHA"))
    if not pha_files:
        raise SystemExit(f"No .PHA files found in {pha_dir}")

    errors = []
    all_events = []

    print(f"Found {len(pha_files)} PHA files. Parsing…")

    for fpath in pha_files:
        events = parse_pha_file_from_module(fpath, errors)
        all_events.extend(events)
        print(f"  {fpath.name}: {len(events)} events")

    # Build catalog + CSV
    catalog, df = build_quakeml_and_table(all_events)

    # File outputs with step prefix
    csv_path = outdir / "03_pha_events.csv"
    qml_path = outdir / "03_pha_catalog.xml"
    log_path = outdir / "03_pha_parse_errors.log"

    df.to_csv(csv_path, index=False)
    catalog.write(qml_path, format="QUAKEML")

    with open(log_path, "w") as f:
        for err in errors:
            f.write(err + "\n")

    print("\n=== STEP 03 SUMMARY ===")
    print(f"Total events parsed:   {len(all_events)}")
    print(f"Total picks:           {len(df)}")
    print(f"CSV:                   {csv_path}")
    print(f"QuakeML catalog:       {qml_path}")
    print(f"Errors logged:         {log_path}")


if __name__ == "__main__":
    main()