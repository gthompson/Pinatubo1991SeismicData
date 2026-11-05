"""Pinatubo catalog metadata policies and thin FLOVOpy REA export wrapper."""
from __future__ import annotations
import os
import pickle
import hashlib
from pathlib import Path
from typing import List, Optional
from obspy import UTCDateTime
from obspy.core.event import Event, Comment, ResourceIdentifier
from flovopy.seisanio.rea_export import export_catalog_to_rea
from .shared import get_comment_value

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


def _anchor_time(event):
    # Pinatubo policy, deliberately not a generic representative-event-time API.
    time = extract_waveform_starttime(event)
    if time is not None:
        return time, 'waveform_starttime'
    times = [p.time for p in event.picks if p.time is not None]
    return (min(times), 'earliest_pick') if times else None


def _prepare_event(event):
    timed = [o for o in event.origins if o.time is not None]
    real = bool(timed) and not get_comment_value(event, 'origin_derived_from:') and any(
        o.latitude is not None and o.longitude is not None for o in timed)
    if not real:
        for origin in event.origins:
            origin.latitude = origin.longitude = origin.depth = None
    prefixes = ('EVENT_CLASS:', 'WAVEFORM_PRESENT:', 'PICKS_PRESENT:',
                'HYPOCENTER_PRESENT:', 'ORIGIN_STATUS:')
    event.comments = [c for c in event.comments if not (c.text or '').startswith(prefixes)]
    values = dict(EVENT_CLASS=infer_event_class(event),
        WAVEFORM_PRESENT=bool(extract_wavefiles(event)), PICKS_PRESENT=bool(event.picks),
        HYPOCENTER_PRESENT=real, ORIGIN_STATUS='REAL' if real else 'DERIVED')
    event.comments.extend(Comment(text=f'{key}:{value}', resource_id=ResourceIdentifier(
        'smi:local/pinatubo/export-comment/' + hashlib.sha256(
            (str(event.resource_id)+'|'+key).encode()).hexdigest())) for key, value in values.items())
    return event


def run(*, catalog_pkl, rea_dir, author='GT__', evtype='L', nordic_format='OLD',
        write_select=False, collision='error', max_shift_seconds=30, overwrite=False):
    """Export Step-50 catalog, reporting writes/reuse/failures instead of suffixing names.

    collision='shift' explicitly permits filename-only shifts for same-second events;
    actual origin and pick times are never shifted. Default collisions are failures.
    """
    with open(catalog_pkl, 'rb') as stream:
        catalog = pickle.load(stream)
    result = export_catalog_to_rea(
        catalog, rea_dir, author=author, event_type=lambda e: infer_event_type(e, evtype),
        nordic_format=nordic_format, high_accuracy=False,
        prepare_event=_prepare_event, waveform_files=extract_wavefiles,
        anchor_time=_anchor_time, collision=collision, max_shift_seconds=max_shift_seconds,
        overwrite=overwrite, write_select=write_select,
    )
    print('STEP 52:', {name: len(result[name]) for name in ('written','reused','skipped','failed')})
    if result['failed']:
        print('Export failures:', result['failed'])
    return result
