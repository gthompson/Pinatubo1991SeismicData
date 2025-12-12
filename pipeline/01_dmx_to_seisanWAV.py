#!/usr/bin/env python3
"""
01_dmx_to_seisanWAV.py

Convert legacy VDAP / PHIVOLCS SUDS/DMX triggered waveform files
from the 1991 Pinatubo seismic network into organized SEISAN-style
MiniSEED WAV archives.

This script is a standalone, dependency-minimized version of the
Pinatubo FAIR conversion pipeline. It uses only Python standard libs,
ObsPy, NumPy, Pandas, and the helper functions implemented below.

Outputs:
--------
• Year/month SEISAN-like WAV directory containing MiniSEED files
• Trace ID mapping table (CSV + LaTeX)
• Terminal summary of conversion performance

Example:
--------
python 01_dmx_to_seisanWAV.py \
    --rawtop /path/to/LEGACY/WAVEFORM_DATA/SUDS \
    --seisan-top /path/to/FAIR/SEISAN/WAV/PNTBO \
    --db PNTBO \
    --net XB \
    --fix-fs 100.0
"""

import argparse
from pathlib import Path
import pandas as pd
from obspy import read, Stream, UTCDateTime
import numpy as np
import os

# ============================================================================
# Helper functions (standalone replacements for flovopy functionality)
# ============================================================================

def read_DMX_file(DMXfile, fix=True, defaultnet=""):
    """
    Reads a DMX waveform file into an ObsPy Stream.

    DMX files from Pinatubo include:
    - short-period channels
    - embedded IRIG timing channels
    - telemetry offsets of +2048 counts (removed if fix=True)
    """
    try:
        st = read(DMXfile)
    except Exception as e:
        print(f"Failed to read DMX file {DMXfile}: {e}")
        return Stream()

    if fix:
        for tr in st:
            if tr.stats.network in ["unk", "", None]:
                tr.stats.network = defaultnet
            # Remove 12-bit telemetry offset
            tr.data = tr.data.astype(float) - 2048.0

    return st


def _is_empty_trace(tr):
    """Return True if the trace contains no useful data."""
    if tr.stats.npts == 0:
        return True
    if np.all(np.isnan(tr.data)):
        return True
    if np.all(tr.data == tr.data[0]):
        return True
    return False


def remove_empty_traces(st, inplace=False):
    """Remove flat / zero / NaN-only ObsPy traces."""
    if inplace:
        to_remove = [tr for tr in st if _is_empty_trace(tr)]
        for tr in to_remove:
            st.remove(tr)
        return None
    else:
        return Stream(tr for tr in st if not _is_empty_trace(tr))


def remove_IRIG_channel(st: Stream) -> None:
    """Remove IRIG timing traces (case-insensitive, various spellings)."""
    to_remove = []
    for tr in st:
        sta = getattr(tr.stats, "station", "")
        if sta and sta.upper().startswith("IRIG"):
            to_remove.append(tr)
    for tr in to_remove:
        st.remove(tr)


def write_wavfile(st: Stream, seisan_wav_db: str, dbstring: str,
                  numchans: int, year_month_dirs=True, fmt="MSEED") -> str:
    """
    Writes a SEISAN-style MiniSEED file using year/month directory layout.

    Filename: YYYY-MM-DD-HHMM-SSS.DBSTRING_NUMCHANS
    """
    if not st or len(st) == 0:
        raise ValueError("Cannot write empty stream to WAV output.")
    
    # Merge and sort traces - added after upload of MiniSEED to EarthScope   
    st.sort(keys=['station', 'channel'])
    st.merge(method=1, fill_value=0)
    st.trim(st[0].stats.starttime, st[0].stats.endtime, pad=True, fill_value=0)

    t = st[0].stats.starttime   # UTCDateTime
    basename = "%4d-%02d-%02d-%02d%02d-%02dM.%s_%03d" % (
        t.year, t.month, t.day,
        t.hour, t.minute, t.second,
        dbstring, numchans
    )

    if year_month_dirs:
        out_dir = Path(seisan_wav_db) / f"{t.year:04d}" / f"{t.month:02d}"
    else:
        out_dir = Path(seisan_wav_db)

    out_dir.mkdir(parents=True, exist_ok=True)
    seisan_wavfile_path = out_dir / basename

    # Write MiniSEED file
    st.write(str(seisan_wavfile_path), format="MSEED")

    return str(seisan_wavfile_path)


# ============================================================================
# Pinatubo-specific ID normalization
# ============================================================================
def fix_pinatubo_trace_id(trace, netcode="XB", verbose=False):
    """
    Pinatubo-specific trace ID standardization.

    Rules:
    - Station name = first 3 chars of DMX station code
    - Orientation = 4th char (Z/N/E)
    - Channel = 'EH' + orientation
    - Location code = ''
    - Network = netcode
    """
    original = trace.id

    raw_sta = (trace.stats.station or "").upper()
    raw_chan = (trace.stats.channel or "").upper()

    # Extract station + orientation
    if len(raw_sta) < 4:
        # Should not happen, but fail gracefully
        sta = raw_sta[:3]
        orient = "Z"
    else:
        sta = raw_sta[:3]           # first 3 characters
        orient = raw_sta[3]         # orientation letter

    if orient not in ["Z", "N", "E"]:
        orient = "Z"                # fallback, but should never happen for Pinatubo

    # Assign standardized components
    trace.stats.network = netcode
    trace.stats.station = sta
    trace.stats.channel = f"EH{orient}"
    trace.stats.location = ""

    changed = (trace.id != original)
    if verbose and changed:
        print(f"{original} → {trace.id}")

    return changed

def fix_sampling_rate(st: Stream, fs: float = 100.0):
    """Force trace.stats.sampling_rate to constant fs (metadata only)."""
    for tr in st:
        tr.stats.sampling_rate = fs


def discover_dmx_files(rawtop: Path, pattern: str):
    """Recursively discover DMX files."""
    return sorted(Path(rawtop).glob(pattern))


# ============================================================================
# Main
# ============================================================================

def main():
    ap = argparse.ArgumentParser(description="Convert DMX to SEISAN WAV MiniSEED")
    ap.add_argument("--rawtop", required=True)
    ap.add_argument("--seisan-wav-db", required=True)
    ap.add_argument("--db", default="PNTBO")
    ap.add_argument("--net", default="XB")
    ap.add_argument("--fix-fs", type=float, default=None)
    ap.add_argument("--glob", default="**/*.DMX")
    ap.add_argument("--max-files", type=int, default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    rawtop = Path(args.rawtop)
    seisan_wav_db = Path(args.seisan_wav_db)

    dmx_files = discover_dmx_files(rawtop, args.glob)
    if args.max_files:
        dmx_files = dmx_files[:args.max_files]

    if not dmx_files:
        raise SystemExit(f"No DMX files found under {rawtop}")

    trace_map = {}

    n_found = len(dmx_files)
    n_ok = 0
    n_empty = 0
    n_fail = 0

    if args.verbose:
        print(f"Found {n_found} DMX files")

    for i, dmx in enumerate(dmx_files, 1):
        if args.verbose:
            print(f"[{i}/{n_found}] {dmx}")

        # ---- read DMX ----
        st = read_DMX_file(str(dmx), fix=True, defaultnet=args.net)
        if not st or len(st) == 0:
            n_fail += 1
            continue

        # ---- remove empty traces ----
        st = remove_empty_traces(st, inplace=False)
        if not st or len(st) == 0:
            n_empty += 1
            continue

        # ---- remove IRIG ----
        remove_IRIG_channel(st)
        if not st or len(st) == 0:
            n_empty += 1
            continue

        # ---- force sampling rate if requested ----
        if args.fix_fs is not None:
            fix_sampling_rate(st, fs=args.fix_fs)

        # ---- fix all trace IDs ----
        for tr in st:
            before = tr.id
            fix_pinatubo_trace_id(tr, netcode=args.net)
            after = tr.id

            if before not in trace_map:
                trace_map[before] = after

        # ---- write MiniSEED file ----
        try:
            seisan_wavfile_path = write_wavfile(
                st=st,
                seisan_wav_db=str(seisan_wav_db),
                dbstring=args.db,
                numchans=len(st),
                year_month_dirs=True,
                fmt="MSEED"
            )
            n_ok += 1
        except Exception as e:
            n_fail += 1
            if args.verbose:
                print(f"Write failed: {e}")

    # =========================================================================
    # Write mapping tables
    # =========================================================================
    seisan_wav_db.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [(k, v) for k, v in trace_map.items()],
        columns=["original_id", "fixed_id"]
    ).sort_values("original_id")

    csv_path = seisan_wav_db / "metadata_trace_id_mapping.csv"
    tex_path = seisan_wav_db / "metadata_trace_id_mapping.tex"

    df.to_csv(csv_path, index=False)
    df.to_latex(
        tex_path,
        index=False,
        caption="Mapping from original DMX trace IDs to SEED-compliant IDs.",
        label="tab:trace_id_mapping",
        escape=False
    )

    # =========================================================================
    # Summary
    # =========================================================================
    print("\nConversion summary")
    print("------------------")
    print(f"DMX files found:           {n_found}")
    print(f"Successfully written:      {n_ok}")
    print(f"Empty/zero/IRIG-only:      {n_empty}")
    print(f"Failed read/write:         {n_fail}")
    print(f"Trace ID mappings saved:   {len(df)}")
    print(f"  CSV: {csv_path}")
    print(f"  TeX: {tex_path}")


if __name__ == "__main__":
    main()