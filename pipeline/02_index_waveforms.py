#!/usr/bin/env python3
"""
02_index_waveforms.py

Create a modern “wfdisc-like” index of all MiniSEED WAV files produced by
01_dmx_to_seisanWAV.py for the 1991 Pinatubo dataset.

Outputs:
--------
• metadata/wfdisc_catalog.csv
• metadata/wfdisc_catalog.xml  (simple QuakeML catalog)
• Terminal summary

Usage:
------
python 02_index_waveforms.py \
    --seisan-top /path/to/FAIR/SEISAN/WAV/PNTBO \
    --db PNTBO
"""

import argparse
from pathlib import Path
import pandas as pd
from obspy import read, UTCDateTime
from obspy.core.event import Catalog, Event, Origin, Comment


def discover_mseed_files(seisan_wav_db: Path, db: str):
    """
    Return sorted list of all MiniSEED WAV files in the SEISAN WAV database.
    Pattern:
        WAV/<DB>/<YYYY>/<MM>/*M.<DB>_*
    """
    #pattern = seisan_wav_db / db / "*" / "*" / f"*M.{db}_*" # 01 process did not create db subdir
    pattern = f"*/*/*M.{db}_*"
    return sorted(seisan_wav_db.glob(pattern))


def extract_metadata_from_mseed(path: Path):
    """
    Read a MiniSEED file and return metadata describing:
    - starttime
    - endtime
    - stations present
    - channels present
    - number of traces
    """

    try:
        st = read(str(path))
    except Exception as e:
        print(f"⚠️  Failed to read {path}: {e}")
        return None

    try:
        start = min(tr.stats.starttime for tr in st)
        end = max(tr.stats.endtime for tr in st)
        stations = sorted({tr.stats.station for tr in st})
        channels = sorted({tr.stats.channel for tr in st})
    except Exception as e:
        print(f"⚠️  Error extracting metadata from {path}: {e}")
        return None

    return {
        "file": str(path),
        "starttime": start,
        "endtime": end,
        "stations": ",".join(stations),
        "channels": ",".join(channels),
        "ntraces": len(st)
    }


def main():
    parser = argparse.ArgumentParser(description="Index SEISAN WAV MiniSEED files")
    parser.add_argument("--seisan-wav-db", required=True,
                        help="WAV/DB directory containing <YYYY>/<MM>")
    parser.add_argument("--db", required=True,
                        help="SEISAN database name (e.g., PNTBO)")
    parser.add_argument("--metadata-path", required=True,
                        help="output directory for event metadata")      
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    seisan_wav_db = Path(args.seisan_wav_db) 
    db = args.db

    # Output paths
    out_dir = Path(args.metadata_path)
    out_dir.mkdir(exist_ok=True)
    csv_path = Path(args.metadata_path) / "02_wfdisc_catalog.csv"
    xml_path = Path(args.metadata_path) / "02_wfdisc_catalog.xml"

    # ----------------------------------------------------------------------
    # Locate MiniSEED files
    # ----------------------------------------------------------------------
    files = discover_mseed_files(seisan_wav_db, db)
    print(f"Found {len(files)} MiniSEED WAV files")

    rows = []
    for path in files:
        meta = extract_metadata_from_mseed(path)
        if meta:
            rows.append(meta)
            if args.verbose:
                print(f"Indexed: {path}")

    # ----------------------------------------------------------------------
    # Save CSV
    # ----------------------------------------------------------------------
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    print(f"Saved CSV → {csv_path}")

    # ----------------------------------------------------------------------
    # Build simple QuakeML catalog for downstream use - REMOVE?
    # ----------------------------------------------------------------------
    catalog = Catalog()
    for _, row in df.iterrows():
        try:
            ev = Event()
            ori = Origin(time=row["starttime"])
            ev.origins.append(ori)
            ev.comments.append(Comment(text=f"mseed_file: {row['file']}"))
            catalog.append(ev)
        except Exception as e:
            print(f"⚠️ Error creating event for {row['file']}: {e}")

    catalog.write(xml_path, format="QUAKEML")
    print(f"Saved QuakeML → {xml_path}")

    # ----------------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------------
    print("\nIndexing summary")
    print("----------------")
    print(f"Files indexed:  {len(df)}")
    print(f"Output CSV:     {csv_path}")
    print(f"Output QuakeML: {xml_path}")


if __name__ == "__main__":
    main()