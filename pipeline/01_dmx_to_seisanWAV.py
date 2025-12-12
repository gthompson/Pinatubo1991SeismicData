#!/usr/bin/env python3
"""
01_dmx_to_seisanWAV.py

Convert legacy VDAP / PHIVOLCS SUDS/DMX triggered waveform files
from the 1991 Pinatubo seismic network into organized SEISAN-style
MiniSEED WAV archives AND build a waveform index in one pass.

Outputs
-------
SEISAN WAV files:
    WAV/<DB>/<YYYY>/<MM>/YYYY-MM-DD-HHMM-SSM.<DB>_<NCHAN>

Metadata (written to seisan-wav-db root):
    01_waveform_index.csv
    01_trace_id_mapping.csv
    01_trace_id_mapping.tex

Key concepts
------------
• event_id = YYMMDDNN derived from DMX filename
• original DMX file path preserved for provenance
• MiniSEED is written once; metadata captured in memory
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
from obspy import read, Stream, UTCDateTime
import re

# ============================================================================
# Helpers
# ============================================================================

def read_DMX_file(dmxfile, fix=True, defaultnet=""):
    try:
        st = read(dmxfile)
    except Exception as e:
        print(f"❌ Failed to read DMX: {dmxfile} | {e}")
        return Stream()

    if fix:
        for tr in st:
            if tr.stats.network in ("", "unk", None):
                tr.stats.network = defaultnet
            tr.data = tr.data.astype(float) - 2048.0

    return st


def _is_empty_trace(tr):
    if tr.stats.npts == 0:
        return True
    if np.all(np.isnan(tr.data)):
        return True
    if np.all(tr.data == tr.data[0]):
        return True
    return False


def remove_empty_traces(st):
    return Stream(tr for tr in st if not _is_empty_trace(tr))


def remove_IRIG_channel(st):
    to_remove = []
    for tr in st:
        sta = getattr(tr.stats, "station", "")
        if sta and sta.upper().startswith("IRIG"):
            to_remove.append(tr)
    for tr in to_remove:
        st.remove(tr)


def fix_sampling_rate(st, fs):
    for tr in st:
        tr.stats.sampling_rate = fs


def fix_pinatubo_trace_id(trace, netcode="XB"):
    original = trace.id

    raw_sta = (trace.stats.station or "").upper()
    if len(raw_sta) >= 4:
        sta = raw_sta[:3]
        orient = raw_sta[3]
    else:
        sta = raw_sta[:3]
        orient = "Z"

    if orient not in ("Z", "N", "E"):
        orient = "Z"

    trace.stats.network = netcode
    trace.stats.station = sta
    trace.stats.location = ""
    trace.stats.channel = f"EH{orient}"

    return original, trace.id


def write_wavfile(st, seisan_wav_db, db, nchan):
    st.sort(keys=["station", "channel"])
    st.merge(method=1, fill_value=0)
    st.trim(st[0].stats.starttime, st[0].stats.endtime,
            pad=True, fill_value=0)

    t = st[0].stats.starttime
    fname = (
        f"{t.year:04d}-{t.month:02d}-{t.day:02d}-"
        f"{t.hour:02d}{t.minute:02d}-{t.second:02d}M."
        f"{db}_{nchan:03d}"
    )

    outdir = Path(seisan_wav_db) / f"{t.year:04d}" / f"{t.month:02d}"
    outdir.mkdir(parents=True, exist_ok=True)

    outpath = outdir / fname
    st.write(str(outpath), format="MSEED")
    return outpath


def discover_dmx_files(rawtop, pattern):
    return sorted(Path(rawtop).glob(pattern))


def extract_event_id(dmx_path: Path):
    """
    Extract YYMMDDNN from YYMMDDNN.DMX
    """
    m = re.match(r"(\d{6}[0-9A-Z]{2})\.DMX$", dmx_path.name.upper())
    return m.group(1) if m else None


# ============================================================================
# Main
# ============================================================================

def main():
    ap = argparse.ArgumentParser(
        description="Convert DMX to SEISAN WAV and build waveform index"
    )
    ap.add_argument("--rawtop", required=True)
    ap.add_argument("--seisan-wav-db", required=True)
    ap.add_argument("--db", default="PNTBO")
    ap.add_argument("--net", default="XB")
    ap.add_argument("--fix-fs", type=float, default=None)
    ap.add_argument("--glob", default="**/*.DMX")
    ap.add_argument("--max-files", type=int)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    rawtop = Path(args.rawtop)
    seisan_wav_db = Path(args.seisan_wav_db)
    seisan_wav_db.mkdir(parents=True, exist_ok=True)

    dmx_files = discover_dmx_files(rawtop, args.glob)
    if args.max_files:
        dmx_files = dmx_files[:args.max_files]

    if not dmx_files:
        raise SystemExit("No DMX files found")

    waveform_rows = []
    trace_map = {}

    n_ok = n_empty = n_fail = 0

    for i, dmx in enumerate(dmx_files, 1):
        event_id = extract_event_id(dmx)
        if not event_id:
            print(f"⚠️  Cannot derive event_id from {dmx.name}")
            n_fail += 1
            continue

        if args.verbose:
            print(f"[{i}/{len(dmx_files)}] {event_id}")

        st = read_DMX_file(str(dmx), defaultnet=args.net)
        if not st:
            n_fail += 1
            continue

        st = remove_empty_traces(st)
        remove_IRIG_channel(st)
        if not st:
            n_empty += 1
            continue

        if args.fix_fs:
            fix_sampling_rate(st, args.fix_fs)

        for tr in st:
            before, after = fix_pinatubo_trace_id(tr, args.net)
            trace_map.setdefault(before, after)

        try:
            wavpath = write_wavfile(
                st, seisan_wav_db, args.db, len(st)
            )
        except Exception as e:
            print(f"❌ Write failed for {event_id}: {e}")
            n_fail += 1
            continue

        start = min(tr.stats.starttime for tr in st)
        end = max(tr.stats.endtime for tr in st)

        waveform_rows.append({
            "event_id": event_id,
            "dmx_file": str(dmx),
            "wav_file": str(wavpath),
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "stations": ",".join(sorted({tr.stats.station for tr in st})),
            "channels": ",".join(sorted({tr.stats.channel for tr in st})),
            "ntraces": len(st)
        })

        n_ok += 1

    # =========================================================================
    # Write outputs
    # =========================================================================

    df_index = pd.DataFrame(waveform_rows)
    index_csv = seisan_wav_db / "01_waveform_index.csv"
    df_index.to_csv(index_csv, index=False)

    df_map = pd.DataFrame(
        [(k, v) for k, v in trace_map.items()],
        columns=["original_id", "fixed_id"]
    ).sort_values("original_id")

    map_csv = seisan_wav_db / "01_trace_id_mapping.csv"
    map_tex = seisan_wav_db / "01_trace_id_mapping.tex"

    df_map.to_csv(map_csv, index=False)
    df_map.to_latex(
        map_tex,
        index=False,
        caption="Mapping from original DMX trace IDs to SEED-compliant IDs.",
        label="tab:trace_id_mapping",
        escape=False
    )

    # =========================================================================
    # Summary
    # =========================================================================
    print("\nStep 01 summary")
    print("--------------")
    print(f"DMX files processed:   {len(dmx_files)}")
    print(f"Written WAV files:    {n_ok}")
    print(f"Empty skipped:        {n_empty}")
    print(f"Failures:             {n_fail}")
    print(f"Waveform index:       {index_csv}")
    print(f"Trace ID map:         {map_csv}")


if __name__ == "__main__":
    main()