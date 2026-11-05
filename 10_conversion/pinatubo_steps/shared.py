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
