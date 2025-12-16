#!/usr/bin/env python3
"""
52_build_seisan_rea_catalog.py

STEP 52 — Build a SEISAN REA catalog from ObsPy QuakeML (Step 50).

Outputs
-------
1) Individual Nordic S-files into:
       REA/YYYY/MM/

2) Nordic select exports (optional):
       select_all.out   (all events)
       select_wav.out   (events with ≥1 waveform)

Rules
-----
• ALL events are written (unless truly timeless)
• If no origin exists, derive one using:
    1) waveform_starttime comment
    2) earliest pick time
• Waveform filenames written as Nordic type-6 lines
• QuakeML provenance comments are stripped before S-file writing
• Deterministic, collision-safe S-file naming
"""

from __future__ import annotations

import argparse
import copy
import os
from pathlib import Path
from typing import Optional, List, Tuple

from obspy import read_events, UTCDateTime
from obspy.core.event import Event, Origin, Comment, ResourceIdentifier, Catalog
from obspy.io.nordic.core import _write_nordic


# -----------------------------------------------------------------------------
# Comment parsing
# -----------------------------------------------------------------------------

def extract_wavefiles(event: Event) -> List[str]:
    """
    Extract waveform filenames from QuakeML comments.
    Expected:
      wavfile:<filename or path>
    Returns unique basenames in original order.
    """
    wavs: List[str] = []
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith("wavfile:"):
            wavs.append(os.path.basename(txt.split("wavfile:", 1)[1].strip()))

    seen = set()
    out = []
    for w in wavs:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def extract_waveform_starttime(event: Event) -> Optional[UTCDateTime]:
    """
    Extract waveform start time from:
      waveform_starttime:ISO8601
    """
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith("waveform_starttime:"):
            try:
                return UTCDateTime(txt.split("waveform_starttime:", 1)[1].strip())
            except Exception:
                return None
    return None


def infer_event_type(event: Event, default: str = "L") -> str:
    for c in (event.comments or []):
        txt = c.text or ""
        if "mainclass:" in txt:
            return txt.split("mainclass:", 1)[1].strip() or default
    return default


# -----------------------------------------------------------------------------
# Origin handling
# -----------------------------------------------------------------------------

def ensure_origin(event: Event) -> Tuple[Optional[Origin], Optional[str]]:
    """
    Ensure event has an origin.

    Returns:
      (origin, source) where source is:
        existing | waveform_starttime | earliest_pick | None
    """
    if event.origins:
        o = event.preferred_origin() or event.origins[0]
        if o.time:
            if event.preferred_origin_id is None:
                event.preferred_origin_id = o.resource_id
            return o, "existing"

    t = extract_waveform_starttime(event)
    if t:
        o = Origin(time=t, resource_id=ResourceIdentifier("origin/derived"))
        event.origins.append(o)
        event.preferred_origin_id = o.resource_id
        event.comments.append(Comment(text="origin_derived_from:waveform_starttime"))
        return o, "waveform_starttime"

    if event.picks:
        times = [p.time for p in event.picks if p.time]
        if times:
            o = Origin(time=min(times), resource_id=ResourceIdentifier("origin/derived"))
            event.origins.append(o)
            event.preferred_origin_id = o.resource_id
            event.comments.append(Comment(text="origin_derived_from:earliest_pick"))
            return o, "earliest_pick"

    return None, None


def event_sort_key(event: Event) -> UTCDateTime:
    o = event.preferred_origin()
    if o and o.time:
        return o.time
    if event.origins and event.origins[0].time:
        return event.origins[0].time
    if event.picks:
        times = [p.time for p in event.picks if p.time]
        if times:
            return min(times)
    return UTCDateTime(9999, 1, 1)


# -----------------------------------------------------------------------------
# Nordic helpers
# -----------------------------------------------------------------------------

def strip_provenance_comments(event: Event) -> None:
    """
    Remove QuakeML-only provenance comments before writing Nordic files.
    """
    keep = []
    for c in (event.comments or []):
        txt = (c.text or "").strip()
        if txt.startswith(("waveform_starttime:", "origin_derived_from:", "wavfile:")):
            continue
        keep.append(c)
    event.comments = keep

def nordic_sfile_basename(evtime: UTCDateTime, evtype: str) -> str:
    """
    Build a SEISAN/Nordic S-file basename like:
      09-0248-29L.S199105
    """
    return (
        f"{evtime.day:02d}-"
        f"{evtime.hour:02d}{evtime.minute:02d}-"
        f"{evtime.second:02d}{evtype}.S"
        f"{evtime.year:04d}{evtime.month:02d}"
    )

def nordic_wavefile_string(event: Event) -> str:
    """
    ObsPy Nordic writer expects one string per event.
    """
    wavs = extract_wavefiles(event)
    return " ".join(wavs) if wavs else ""


def unique_sfile_name(ymdir: Path, base: str) -> str:
    """
    Ensure filename uniqueness inside YYYY/MM directory.
    """
    if not (ymdir / base).exists():
        return base

    stem = base
    i = 1
    while True:
        candidate = f"{stem}_{i}"
        if not (ymdir / candidate).exists():
            return candidate
        i += 1

def read_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    return [ln.rstrip("\n") for ln in path.read_text().splitlines()]


def cleanup_local_sfile(local_path: Path, rea_path: Path) -> bool:
    """
    Compare local S-file to canonical REA S-file.
    Remove lines already present in REA copy.
    Delete local file if empty after cleanup.

    Returns:
        True if local file was deleted, False otherwise.
    """
    if not local_path.exists() or not rea_path.exists():
        return False

    local_lines = read_lines(local_path)
    rea_lines = set(read_lines(rea_path))

    unique_lines = [ln for ln in local_lines if ln not in rea_lines]

    if not unique_lines:
        local_path.unlink()
        return True

    local_path.write_text("\n".join(unique_lines) + "\n")
    return False

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 52: Build SEISAN REA catalog")
    ap.add_argument("--quakeml", required=True)
    ap.add_argument("--rea-dir", required=True)
    ap.add_argument("--author", default="GT__")
    ap.add_argument("--evtype", default="L")
    ap.add_argument("--nordic-format", default="OLD", choices=["OLD", "NEW"])
    ap.add_argument("--write-select", action="store_true")
    ap.add_argument("--strip-provenance", action="store_true")
    args = ap.parse_args()

    rea_dir = Path(args.rea_dir)
    rea_dir.mkdir(parents=True, exist_ok=True)

    print("Reading QuakeML catalog")
    catalog = read_events(args.quakeml)

    # Ensure origins
    derived_stats = {"existing": 0, "waveform_starttime": 0, "earliest_pick": 0}
    timeless = 0
    for ev in catalog:
        _, how = ensure_origin(ev)
        if how:
            derived_stats[how] += 1
        else:
            timeless += 1

    # Sort catalog
    catalog.events.sort(key=event_sort_key)

    # Build wavefile strings (aligned!)
    wavefiles_all = [nordic_wavefile_string(ev) for ev in catalog]

    # ------------------------------------------------------------------
    # SELECT FILES
    # ------------------------------------------------------------------

    if args.write_select:
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

        wav_mask = [bool(w.strip()) for w in wavefiles_all]
        cat_wav = Catalog(events=[e for e, k in zip(catalog, wav_mask) if k])
        wavfiles_wav = [w for w, k in zip(wavefiles_all, wav_mask) if k]

        print("Writing select_wav.out")
        cat_wav.write(
            str(rea_dir / "select_wav.out"),
            format="NORDIC",
            userid=args.author,
            evtype=args.evtype,
            wavefiles=wavfiles_wav,
            nordic_format=args.nordic_format,
            high_accuracy=False,
        )

    # ------------------------------------------------------------------
    # INDIVIDUAL S-FILES
    # ------------------------------------------------------------------
    written = 0
    skipped = 0
    cleaned = 0
    kept = 0

    for ev in catalog:
        e = copy.deepcopy(ev)
        o = e.preferred_origin()

        if o is None or o.time is None:
            skipped += 1
            continue

        ymdir = rea_dir / f"{o.time.year:04d}" / f"{o.time.month:02d}"
        ymdir.mkdir(parents=True, exist_ok=True)

        evtype = infer_event_type(e, args.evtype)
        wavfiles = extract_wavefiles(e)

        if args.strip_provenance:
            strip_provenance_comments(e)

        base = nordic_sfile_basename(o.time, evtype)
        sfile_name = unique_sfile_name(ymdir, base)
        rea_path = ymdir / sfile_name
        local_path = Path(sfile_name)  # if ObsPy stages anything with this name

        sfile_name = unique_sfile_name(ymdir, base)
        rea_path = ymdir / sfile_name
        local_path = Path(sfile_name)   # ObsPy staging artifact

        _write_nordic(
            e,
            sfile_name,
            outdir=str(ymdir),
            userid=args.author,
            evtype=evtype,
            wavefiles=wavfiles,
            nordic_format=args.nordic_format,
            overwrite=True,
            explosion=False,
            high_accuracy=False,
        )

        # ---- CLEANUP LOCAL COPY ----
        if local_path.exists() and rea_path.exists():
            deleted = cleanup_local_sfile(local_path, rea_path)
            if deleted:
                cleaned += 1
            else:
                kept += 1

        written += 1

    print("\nSTEP 52 COMPLETE")
    print("----------------")
    print(f"Events total:              {len(catalog)}")
    print(f"Derived origin counts:     {derived_stats}")
    print(f"Timeless events skipped:   {skipped}")
    print(f"S-files written:           {written}")
    print(f"Local S-files deleted:     {cleaned}")
    print(f"Local S-files retained:    {kept}")
    print(f"REA directory:             {rea_dir}")


if __name__ == "__main__":
    main()