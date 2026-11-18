"""Shared Pinatubo table/plot conventions and adapters to FLOVOpy primitives.

Historical parsing, block grouping, source priority and event classification
remain in their source-specific modules. XB is a project default here only.
"""
import pandas as pd
from flovopy.core.event_utils import (
    parse_seed_id, get_comment_value, associate_events_by_time,
)

DEFAULT_NETWORK = 'XB'


def to_dt(series):
    """Coerce invalid project-table times to NaT, using UTC."""
    return pd.to_datetime(series, format='mixed', utc=True, errors='coerce')


def ensure_cols(df, columns):
    """Return a copy with missing optional columns filled with pd.NA."""
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result


def normalize_seed_id_df(df):
    """Prefer existing IDs; otherwise use the Pinatubo XB.station..channel form."""
    def text(column):
        return df.get(column, pd.Series('', index=df.index, dtype='string')).astype('string').fillna('').str.strip()
    sid = text('seed_id')
    fallback = _fallback_id(text('station'), text('channel'))
    return sid.where(sid.ne(''), fallback).astype('string')


def _fallback_id(station, channel):
    return DEFAULT_NETWORK + '.' + station + '..' + channel


def normalize_seed_id(row):
    """Scalar adapter using the same missing-field policy as table normalization."""
    def text(key):
        value = row.get(key)
        return '' if pd.isna(value) else str(value).strip()
    return text('seed_id') or _fallback_id(text('station'), text('channel'))


def safe_series_get(row, key, default=None):
    value = row.get(key, default)
    return default if pd.isna(value) else value


def savefig(path):
    """Save and close the active figure with the pipeline's plotting defaults."""
    from pathlib import Path
    import matplotlib.pyplot as plt
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()


def associate_one_to_one_by_time(
    df_wave, df_hypo, wave_id_col='event_id', wave_time_col='event_time',
    hypo_id_col='event_id', hypo_time_col='preferred_origin_time', tolerance_seconds=10.0,
):
    """Compatibility adapter for Step-50's original column names."""
    return associate_events_by_time(
        df_wave, df_hypo, left_id_col=wave_id_col, left_time_col=wave_time_col,
        right_id_col=hypo_id_col, right_time_col=hypo_time_col,
        tolerance_seconds=tolerance_seconds, policy='one_to_one',
    ).rename(columns={'left_id': 'event_id_w', 'left_time': 'event_time',
                      'right_id': 'event_id_h', 'right_time': 'preferred_origin_time'})


def associate_nearest_by_time(df_wave, df_hypo, tolerance_seconds=10.0):
    """Compatibility adapter for the original reuse-permitting policy."""
    return associate_events_by_time(
        df_wave, df_hypo, left_id_col='event_id', left_time_col='event_time',
        right_id_col='event_id', right_time_col='preferred_origin_time',
        tolerance_seconds=tolerance_seconds, policy='nearest',
    ).rename(columns={'left_id': 'event_id_w', 'left_time': 'event_time',
                      'right_id': 'event_id_h', 'right_time': 'preferred_origin_time'})


def _ns(series):
    """UTC times as int64 nanoseconds (NaT -> min int)."""
    return to_dt(series).dt.tz_convert(None).values.astype('datetime64[ns]').astype('int64')


def associate_hypocenters(df_evt, df_map, df_hypo, *, max_first_arrival_s=10.0, min_first_arrival_s=-0.5,
                          expected_first_arrival_s=1.7, origin_lead_s=10.0, expected_origin_offset_s=9.5):
    """Link hypocentres to step-32 events: through picks first, then by record window.

    Stage 1 (``picks``): a hypocentre links to an event whose first P pick
    (first pick of any phase if it has no P) is ``min_first_arrival_s`` to
    ``max_first_arrival_s`` after the origin; the closest to
    ``expected_first_arrival_s`` wins. Any event with picks qualifies.

    Stage 2 (``window``): remaining hypocentres link to a waveform event
    (step-32 class WAV_ONLY or WAV+PICKS) whose record window, extended
    ``origin_lead_s`` before its start, contains the origin; the start
    closest to ``origin - expected_origin_offset_s`` wins. Triggered records
    start a few seconds before the first arrival, so origins normally fall
    after the start (Pinatubo median 9.5 s).

    Both stages are greedy one-to-one, best score first, ties broken by IDs.
    Returns (matches, inside) where ``matches`` has event_id_w, event_id_h,
    method, origin_time, event_time, origin_minus_start_s,
    first_arrival_minus_origin_s and abs_dt_s (|origin - start|), and
    ``inside`` maps each unmatched hypocentre whose origin lies inside some
    (already linked) waveform record to that event_id.
    """
    import math
    import numpy as np
    for name, value in dict(max_first_arrival_s=max_first_arrival_s, min_first_arrival_s=min_first_arrival_s,
                            expected_first_arrival_s=expected_first_arrival_s, origin_lead_s=origin_lead_s,
                            expected_origin_offset_s=expected_origin_offset_s).items():
        if not math.isfinite(value):
            raise ValueError(f'{name} must be finite')
    if origin_lead_s < 0 or max_first_arrival_s < min_first_arrival_s:
        raise ValueError('origin_lead_s must be >= 0 and max_first_arrival_s >= min_first_arrival_s')
    for df, label in ((df_evt, 'events'), (df_hypo, 'hypocentres')):
        if df['event_id'].duplicated().any():
            raise ValueError(f'Duplicate {label} event_id values')
    columns = ['event_id_w', 'event_id_h', 'method', 'origin_time', 'event_time', 'origin_minus_start_s',
               'first_arrival_minus_origin_s', 'abs_dt_s']
    hyp = df_hypo[['event_id', 'preferred_origin_time']].copy()
    hyp['_t'] = to_dt(hyp['preferred_origin_time'])
    hyp = hyp.dropna(subset=['_t']).reset_index(drop=True)
    if hyp.empty or df_evt.empty:
        return pd.DataFrame(columns=columns), {}
    th = _ns(hyp['_t'])
    S = 1_000_000_000

    evt = df_evt.copy()
    evt['_start'] = to_dt(evt['event_time'] if 'event_time' in evt else evt['starttime'])
    evt['_end'] = to_dt(evt['endtime']) if 'endtime' in evt else evt['_start']
    start_by_id = dict(zip(evt['event_id'], evt['_start']))

    candidates = []  # (stage, score, hid, eid, first_arrival_s)
    if df_map is not None and len(df_map) and 'pick_time' in df_map:
        mp = df_map[['event_id', 'pick_time'] + (['phase'] if 'phase' in df_map else [])].copy()
        mp['_t'] = to_dt(mp['pick_time'])
        mp = mp.dropna(subset=['_t'])
        is_p = mp['phase'].astype('string').fillna('').str.upper().str.startswith('P') if 'phase' in mp else False
        first = mp[is_p].groupby('event_id')['_t'].min().combine_first(mp.groupby('event_id')['_t'].min())
        first = first[first.index.isin(set(evt['event_id']))].sort_values()
        fv, fid = _ns(first), first.index.to_numpy()
        lo = np.searchsorted(fv, th + int(min_first_arrival_s * S), side='left')
        hi = np.searchsorted(fv, th + int(max_first_arrival_s * S), side='right')
        for k in range(len(hyp)):
            for j in range(lo[k], hi[k]):
                fa = (fv[j] - th[k]) / S
                candidates.append((0, abs(fa - expected_first_arrival_s), hyp.at[k, 'event_id'], fid[j], fa))

    wav = evt[evt.get('event_class', pd.Series('WAV_ONLY', index=evt.index)).isin(['WAV_ONLY', 'WAV+PICKS'])]
    wav = wav.dropna(subset=['_start', '_end']).sort_values('_start')
    ws, we, wid = _ns(wav['_start']), _ns(wav['_end']), wav['event_id'].to_numpy()
    longest = int((we - ws).max()) if len(ws) else 0
    inside_any = {}
    if len(ws):
        lo = np.searchsorted(ws, th - longest, side='left')
        hi = np.searchsorted(ws, th + int(origin_lead_s * S), side='right')
        for k in range(len(hyp)):
            for j in range(lo[k], hi[k]):
                if ws[j] - int(origin_lead_s * S) <= th[k] <= we[j]:
                    off = (th[k] - ws[j]) / S
                    candidates.append((1, abs(off - expected_origin_offset_s), hyp.at[k, 'event_id'], wid[j], None))
                    if ws[j] <= th[k]:
                        inside_any.setdefault(hyp.at[k, 'event_id'], []).append((abs(off - expected_origin_offset_s), wid[j]))

    origin_by_id = dict(zip(hyp['event_id'], hyp['_t']))
    used_h, used_w, rows = set(), set(), []
    for stage, score, hid, eid, fa in sorted(candidates, key=lambda c: (c[0], c[1], str(c[2]), str(c[3]))):
        if hid in used_h or eid in used_w:
            continue
        used_h.add(hid); used_w.add(eid)
        t0, ts = origin_by_id[hid], start_by_id[eid]
        off = (t0 - ts).total_seconds()
        rows.append(dict(event_id_w=eid, event_id_h=hid, method='picks' if stage == 0 else 'window',
                         origin_time=t0, event_time=ts, origin_minus_start_s=off,
                         first_arrival_minus_origin_s=fa, abs_dt_s=abs(off)))
    matches = pd.DataFrame(rows, columns=columns)
    inside = {hid: min(c)[1] for hid, c in inside_any.items() if hid not in used_h}
    return matches, inside
