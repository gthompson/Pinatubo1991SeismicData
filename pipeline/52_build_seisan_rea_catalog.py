#!/usr/bin/env python3
"""
52_build_seisan_rea_catalog.py

STEP 52 — Build a SEISAN REA catalog from ObsPy QuakeML (Step 50).

Key design principles
---------------------
• Never fabricate hypocenters
• Never leak local S-files
• Preserve semantic provenance in Nordic comments
• Use QuakeML (Step 50) as the single source of truth
"""

from __future__ import annotations

import argparse
import copy
import os
import tempfile
from pathlib import Path
from typing import Optional, List, Tuple

from obspy import UTCDateTime
from obspy.core.event import Event, Origin, Comment, ResourceIdentifier, Catalog
from obspy.io.nordic.core import _write_nordic
import pickle


# -----------------------------------------------------------------------------
# Comment helpers
# -----------------------------------------------------------------------------

def get_comment_value(event: Event, prefix: str) -> Optional[str]:
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith(prefix):
            return txt.split(prefix, 1)[1].strip()
    return None


def extract_wavefiles(event: Event) -> List[str]:
    wavs = []
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith("wavfile:"):
            wavs.append(os.path.basename(txt.split("wavfile:", 1)[1].strip()))
    return list(dict.fromkeys(wavs))  # de-dupe, preserve order


def extract_waveform_starttime(event: Event) -> Optional[UTCDateTime]:
    val = get_comment_value(event, "waveform_starttime:")
    if val:
        try:
            return UTCDateTime(val)
        except Exception:
            return None
    return None


def infer_event_class(event: Event) -> str:
    val = get_comment_value(event, "event_class:")
    return val if val else "UNKNOWN"


def infer_event_type(event: Event, default="L") -> str:
    val = get_comment_value(event, "mainclass:")
    return val if val else default


# -----------------------------------------------------------------------------
# Origin handling (strict!)
# -----------------------------------------------------------------------------

def ensure_origin(event: Event) -> Tuple[Optional[Origin], str]:
    """
    Ensure event has an origin time ONLY for ordering / filename purposes.
    This does NOT imply a real hypocenter.
    """

    # 1. Real hypocenter (preferred or first)
    if event.origins:
        o = event.preferred_origin() or event.origins[0]
        if o.time:
            event.preferred_origin_id = o.resource_id
            return o, "REAL_HYPOCENTER"

    # 2. Waveform starttime
    t = extract_waveform_starttime(event)
    if t:
        o = Origin(time=t, resource_id=ResourceIdentifier("origin/derived/waveform"))
        event.origins = [o]
        event.preferred_origin_id = o.resource_id
        return o, "DERIVED_FROM_WAVEFORM_STARTTIME"

    # 3. Earliest pick
    if event.picks:
        times = [p.time for p in event.picks if p.time]
        if times:
            o = Origin(time=min(times), resource_id=ResourceIdentifier("origin/derived/pick"))
            event.origins = [o]
            event.preferred_origin_id = o.resource_id
            return o, "DERIVED_FROM_EARLIEST_PICK"

    return None, "TIMELESS"


def event_sort_key(event: Event) -> UTCDateTime:
    o = event.preferred_origin()
    if o and o.time:
        return o.time
    return UTCDateTime(9999, 1, 1)


# -----------------------------------------------------------------------------
# Nordic helpers
# -----------------------------------------------------------------------------

def nordic_sfile_basename(evtime: UTCDateTime, evtype: str) -> str:
    return (
        f"{evtime.day:02d}-"
        f"{evtime.hour:02d}{evtime.minute:02d}-"
        f"{evtime.second:02d}{evtype}.S"
        f"{evtime.year:04d}{evtime.month:02d}"
    )


def unique_sfile_name(ymdir: Path, base: str) -> str:
    if not (ymdir / base).exists():
        return base
    i = 1
    while True:
        candidate = f"{base}_{i}"
        if not (ymdir / candidate).exists():
            return candidate
        i += 1


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 52: Build SEISAN REA catalog")
    ap.add_argument("--catalog-pkl", required=True)
    ap.add_argument("--rea-dir", required=True)
    ap.add_argument("--author", default="GT__")
    ap.add_argument("--evtype", default="L")
    ap.add_argument("--nordic-format", default="OLD", choices=["OLD", "NEW"])
    ap.add_argument("--write-select", action="store_true")
    args = ap.parse_args()

    rea_dir = Path(args.rea_dir)
    rea_dir.mkdir(parents=True, exist_ok=True)

    print("Reading QuakeML catalog")
    with open(args.catalog_pkl, "rb") as f:
        catalog = pickle.load(f)

    # Ensure every event has a time anchor
    stats = {"REAL_HYPOCENTER": 0, "DERIVED_FROM_WAVEFORM_STARTTIME": 0,
             "DERIVED_FROM_EARLIEST_PICK": 0, "TIMELESS": 0}

    for ev in catalog:
        _, how = ensure_origin(ev)
        stats[how] += 1

    # Sort catalog
    catalog.events.sort(key=event_sort_key)

    # ------------------------------------------------------------------
    # WRITE SELECT FILES (optional)
    # ------------------------------------------------------------------

    if args.write_select:
        wavefiles_all = [" ".join(extract_wavefiles(ev)) for ev in catalog]
        print("Writing select_all.out")
        catalog.write(
            str(rea_dir / "select_all.out"),
            format="NORDIC",
            userid=args.author,
            evtype=args.evtype,
            wavefiles=wavefiles_all,
            nordic_format=args.nordic_format,
            high_accuracy=False,
        )

    # ------------------------------------------------------------------
    # WRITE INDIVIDUAL S-FILES
    # ------------------------------------------------------------------

    written = 0
    skipped = 0

    for ev in catalog:
        e = copy.deepcopy(ev)
        o = e.preferred_origin()

        if o is None or o.time is None:
            skipped += 1
            continue

        ymdir = rea_dir / f"{o.time.year:04d}" / f"{o.time.month:02d}"
        ymdir.mkdir(parents=True, exist_ok=True)

        evtype = infer_event_type(e, args.evtype)
        base = nordic_sfile_basename(o.time, evtype)
        sfile = unique_sfile_name(ymdir, base)

        # --- provenance comments ---
        e.comments = list(e.comments or [])
        e.comments.append(Comment(text=f"EVENT_CLASS:{infer_event_class(e)}"))
        e.comments.append(Comment(text=f"WAVEFORM_PRESENT:{bool(extract_wavefiles(e))}"))
        e.comments.append(Comment(text=f"PICKS_PRESENT:{bool(e.picks)}"))
        e.comments.append(Comment(text=f"HYPOCENTER_PRESENT:{bool(ev.origins)}"))

        # --- If not a real hypocenter, strip location fields ---
        origin_status = get_comment_value(ev, "origin_derived_from:")
        if not ev.origins or origin_status:
            for o0 in e.origins:
                o0.latitude = None
                o0.longitude = None
                o0.depth = None

        e.comments.append(Comment(text=f"ORIGIN_STATUS:{stats and 'DERIVED' or 'REAL'}"))

        # --- Write using temp cwd ---
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                _write_nordic(
                    e,
                    sfile,
                    outdir=str(ymdir),
                    userid=args.author,
                    evtype=evtype,
                    wavefiles=extract_wavefiles(e),
                    nordic_format=args.nordic_format,
                    overwrite=True,
                    explosion=False,
                    high_accuracy=False,
                )
            finally:
                os.chdir(cwd)

        written += 1

    print("\nSTEP 52 COMPLETE")
    print("----------------")
    print(f"Events total:        {len(catalog)}")
    print(f"S-files written:     {written}")
    print(f"Timeless skipped:    {skipped}")
    print("Origin summary:")
    for k, v in stats.items():
        print(f"  {k:30s}: {v}")
    print(f"REA directory:       {rea_dir}")


if __name__ == "__main__":
    main()