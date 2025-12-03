#!/usr/bin/env python3
"""
06_build_unified_catalog.py

Construct a unified QuakeML event catalog by merging:
    • HYPO71 located events
    • PHA-only events
    • Waveform associations from master_event_table.pkl

Outputs:
    - unified_catalog.xml (QuakeML)
    - optional SEISAN REA tree
"""

import argparse
import pandas as pd
from obspy import Catalog, Event, Origin, Magnitude, UTCDateTime, Comment
from obspy.core.event import WaveformStreamID
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="Build unified QuakeML catalog.")
    ap.add_argument("--master", required=True, help="master_event_table.pkl")
    ap.add_argument("--outxml", required=True)
    ap.add_argument("--write-seisan", action="store_true")
    ap.add_argument("--seisan-top", default=None)
    args = ap.parse_args()

    df = pd.read_pickle(args.master)

    cat = Catalog()

    for _, row in df.iterrows():
        ev = Event()

        # --- origin ---
        o = Origin(time=UTCDateTime(row["origin_time"]))
        if pd.notnull(row["latitude"]):
            o.latitude = row["latitude"]
            o.longitude = row["longitude"]
        if pd.notnull(row["depth_m"]):
            o.depth = row["depth_m"]

        # --- magnitude ---
        if pd.notnull(row["magnitude"]):
            ev.magnitudes = [Magnitude(mag=row["magnitude"])]

        # --- waveform reference ---
        if row["waveform_file"]:
            w = WaveformStreamID(
                network_code="XB",
                station_code="*",
                channel_code="EH?",
                location_code="",
                resource_uri=row["waveform_file"]
            )
            o.comments.append(Comment(text=f"waveform_file={row['waveform_file']}"))
            ev.waveform_id = w

        o.comments.append(Comment(text=f"event_source={row['source']}"))
        ev.origins.append(o)
        cat.append(ev)

    print(f"Writing {args.outxml}")
    cat.write(args.outxml, format="QUAKEML")

    # --- optional SEISAN export ---
    if args.write_seisan:
        import obspy.io.seisan.core as ssc

        if args.seisan_top is None:
            raise ValueError("Must specify --seisan-top for SEISAN writing.")

        outdir = Path(args.seisan_top)
        outdir.mkdir(exist_ok=True, parents=True)
        print("Writing SEISAN REA structure...")
        ssc.catalog_to_seisan_rea(cat, directory=str(outdir))

    print("Unified catalog built.")


if __name__ == "__main__":
    main()