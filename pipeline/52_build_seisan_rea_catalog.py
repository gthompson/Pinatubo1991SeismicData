#!/usr/bin/env python3
"""
52_build_seisan_rea_catalog.py

STEP 52 — Build a SEISAN REA catalog from ObsPy QuakeML (Step 50).

Outputs:
  1) Individual Nordic S-files into:
       REA/YYYY/MM/
  2) Nordic "select" exports:
       select_all.out   (all events)
       select_wav.out   (events with >=1 linked waveform)

Rules:
------
• ALL events become S-files (no skipping)
• If no hypocenter/origin exists, derive an Origin time using:
    1) waveform start time (comment: waveform_starttime:...)
    2) earliest pick time
  (If neither exists, the event is written to diagnostics and skipped as "timeless")
• Waveform filenames are attached via Nordic type-6 lines (wavefiles=...)
• QuakeML provenance comments can be stripped before writing S-files
"""

from __future__ import annotations

import argparse
import copy
import os
from pathlib import Path
from typing import Optional, List, Tuple

from obspy import read_events, UTCDateTime
from obspy.core.event import Origin, Comment, ResourceIdentifier, Catalog, Event
from obspy.io.nordic.core import blanksfile, _write_nordic


# -----------------------------------------------------------------------------
# Comment parsing
# -----------------------------------------------------------------------------

def extract_wavefiles(event: Event) -> List[str]:
    """
    Extract waveform filenames from event comments.
    Expected QuakeML comment format:
      wavfile:<filename or path>
    Returns unique basenames in original order.
    """
    wavs: List[str] = []
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith("wavfile:"):
            val = txt.split("wavfile:", 1)[1].strip()
            if val:
                wavs.append(os.path.basename(val))

    # de-dupe preserving order
    seen = set()
    out = []
    for w in wavs:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def extract_waveform_starttime(event: Event) -> Optional[UTCDateTime]:
    """
    Extract waveform start time from comments if present.
    Expected:
      waveform_starttime:1991-05-01T03:10:09.950000+00:00
    """
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith("waveform_starttime:"):
            raw = txt.split("waveform_starttime:", 1)[1].strip()
            try:
                return UTCDateTime(raw)
            except Exception:
                return None
    return None


def infer_event_type(event: Event, default: str = "L") -> str:
    """
    Determine SEISAN event type.
    Uses comment mainclass:<X> if present.
    """
    for c in (event.comments or []):
        txt = (c.text or "")
        if "mainclass:" in txt:
            return txt.split("mainclass:", 1)[1].strip() or default
    return default


# -----------------------------------------------------------------------------
# Origin handling + sorting
# -----------------------------------------------------------------------------

def ensure_origin(event: Event) -> Tuple[Optional[Origin], Optional[str]]:
    """
    Ensure event has at least one origin.
    If missing, derive one from waveform_starttime or earliest pick.

    Returns:
      (origin, derived_from) where derived_from is one of:
        "existing", "waveform_starttime", "earliest_pick", or None (timeless)
    """
    if event.origins:
        origin = event.preferred_origin() or event.origins[0]
        if origin and origin.time:
            if event.preferred_origin_id is None:
                event.preferred_origin_id = origin.resource_id
            return origin, "existing"

    t = extract_waveform_starttime(event)
    if t is not None:
        origin = Origin(time=t, resource_id=ResourceIdentifier("origin/derived"))
        event.origins.append(origin)
        event.preferred_origin_id = origin.resource_id
        event.comments = list(event.comments or [])
        event.comments.append(Comment(text="origin_derived_from:waveform_starttime"))
        return origin, "waveform_starttime"

    if event.picks:
        t = min(p.time for p in event.picks if p.time is not None)
        origin = Origin(time=t, resource_id=ResourceIdentifier("origin/derived"))
        event.origins.append(origin)
        event.preferred_origin_id = origin.resource_id
        event.comments = list(event.comments or [])
        event.comments.append(Comment(text="origin_derived_from:earliest_pick"))
        return origin, "earliest_pick"

    return None, None


def event_sort_key(event: Event) -> UTCDateTime:
    """
    Sort key:
      1) preferred origin time
      2) first origin time
      3) earliest pick time
      4) far future
    """
    o = event.preferred_origin()
    if o and o.time:
        return o.time
    if event.origins and event.origins[0].time:
        return event.origins[0].time
    if event.picks:
        times = [p.time for p in event.picks if p.time is not None]
        if times:
            return min(times)
    return UTCDateTime(9999, 1, 1)


def strip_provenance_comments(event: Event) -> None:
    """
    Remove provenance comments you do NOT want to appear as Nordic comment lines.
    Keeps other comments (if any).
    """
    keep: List[Comment] = []
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith("waveform_starttime:"):
            continue
        if txt.startswith("origin_derived_from:"):
            continue
        if txt.startswith("wavfile:"):
            continue
        keep.append(c)
    event.comments = keep


def nordic_wavefile_string(event: Event) -> str:
    """
    ObsPy Nordic writer expects one string per event.
    Multiple wavefiles are space-separated in that string.
    """
    wavs = extract_wavefiles(event)
    return " ".join(wavs) if wavs else ""

def unique_sfile_path(ymdir, sfilename):
    path = ymdir / sfilename
    if not path.exists():
        return path

    stem = sfilename
    i = 1
    while True:
        candidate = ymdir / f"{stem}_{i}"
        if not candidate.exists():
            return candidate
        i += 1
# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 52: Build SEISAN REA catalog")
    ap.add_argument("--quakeml", required=True, help="Step 50 QuakeML file")
    ap.add_argument("--rea-dir", required=True, help="Top-level REA directory")
    ap.add_argument("--author", default="GT__", help="SEISAN author code")
    ap.add_argument("--evtype", default="L", help="Default SEISAN event type")
    ap.add_argument("--nordic-format", default="OLD", choices=["OLD", "NEW"])
    ap.add_argument("--write-select", action="store_true", help="Also write select_all.out and select_wav.out")
    ap.add_argument("--strip-provenance", action="store_true", help="Strip provenance comments from S-files")
    args = ap.parse_args()

    rea_dir = Path(args.rea_dir)
    rea_dir.mkdir(parents=True, exist_ok=True)

    print("Reading QuakeML catalog")
    catalog = read_events(args.quakeml)

    # Ensure every event has an origin BEFORE sorting/writing anything
    timeless = 0
    derived = {"existing": 0, "waveform_starttime": 0, "earliest_pick": 0}
    for ev in catalog:
        o, how = ensure_origin(ev)
        if o is None:
            timeless += 1
        else:
            derived[how] += 1

    # Sort
    catalog.events.sort(key=event_sort_key)

    # Build per-event wavefile strings aligned with catalog order
    wavefiles_all = [nordic_wavefile_string(ev) for ev in catalog]

    # Optional: write select exports (single-file Nordic)
    if args.write_select:
        select_all = rea_dir / "select_all.out"
        print(f"Writing Nordic select (all): {select_all}")
        catalog.write(
            str(select_all),
            format="NORDIC",
            userid=args.author,
            evtype=args.evtype,
            wavefiles=wavefiles_all,
            nordic_format=args.nordic_format,
            high_accuracy=False,
        )

        # select_wav: only events with wavefiles
        wav_mask = [bool(w.strip()) for w in wavefiles_all]
        cat_wav = Catalog(events=[ev for ev, keep in zip(catalog, wav_mask) if keep])
        wavefiles_wav = [w for w, keep in zip(wavefiles_all, wav_mask) if keep]

        select_wav = rea_dir / "select_wav.out"
        print(f"Writing Nordic select (wav-only): {select_wav}")
        cat_wav.write(
            str(select_wav),
            format="NORDIC",
            userid=args.author,
            evtype=args.evtype,
            wavefiles=wavefiles_wav,
            nordic_format=args.nordic_format,
            high_accuracy=False,
        )

    # Write individual S-files into REA/YYYY/MM
    written = 0
    skipped_timeless = 0

    for ev in catalog:
        # Work on a copy so we don't mutate the catalog used for select exports
        e = copy.deepcopy(ev)

        origin = e.preferred_origin() or (e.origins[0] if e.origins else None)
        if origin is None or origin.time is None:
            skipped_timeless += 1
            continue

        otime = origin.time
        ymdir = rea_dir / f"{otime.year:04d}" / f"{otime.month:02d}"
        ymdir.mkdir(parents=True, exist_ok=True)

        evtype = infer_event_type(e, default=args.evtype)
        wavfiles = extract_wavefiles(e)

        if args.strip_provenance:
            strip_provenance_comments(e)

        sfilename = blanksfile(
            "",
            evtype,
            args.author,
            evtime=origin.time,
            nordic_format=args.nordic_format,
        )


        sfile_path = unique_sfile_path(ymdir, sfilename)
        _write_nordic(
            e,
            sfile_path,
            userid=args.author,
            evtype=evtype,
            outdir=str(ymdir),
            wavefiles=wavfiles,
            explosion=False,
            nordic_format=args.nordic_format,
            overwrite=True,
            high_accuracy=False,
            )

        written += 1

    print("\nSTEP 52 COMPLETE")
    print("----------------")
    print(f"Events total:                 {len(catalog)}")
    print(f"Derived origin counts:        {derived}")
    print(f"Timeless events (no time):    {timeless}  (skipped in S-file writing: {skipped_timeless})")
    print(f"S-files written:              {written}")
    print(f"REA directory:                {rea_dir}")


if __name__ == "__main__":
    main()