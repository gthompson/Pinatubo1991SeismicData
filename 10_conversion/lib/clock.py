"""Pinatubo clock corrections to UTC (``time_basis: "utc"``, the default in conversion_config.json).

The rules live in ``10_conversion/clock_regimes.csv`` (one row per clock
period, with evidence and confidence) and are applied by
``flovopy.core.clock.ClockRegimes``. Each step converts its own raw times
where it reads them and keeps the raw value next to the corrected one:

- step 10: waveform start times (``waveform_time_correction``), stream ``waveform``;
  the rule must agree with both the DMX header ``localtime`` field written by
  MDETECT (the marker) and the file sequence
- step 20: individual PHA picks, stream ``pick_individual`` (sequence = event ID)
- step 21: monthly PHA picks, stream ``pick_monthly`` (sequence = file:line)
- step 40: hypocenters, streams ``hypo_pinaall`` (sequence = line) and
  ``hypo_allsum`` (repeated 26 May window resolved by matching PINAALL.DAT)
- steps 61-62: RSAM and EVNT labels (stream ``tandy_rsam``, the Tandy Model 100
  RSAM PC) and SSAM labels (stream ``ssam``, MDETECT on WILLIE)
- steps 63-67: tilt (stream ``sleepy_tilt``, SLEEPY), rain and mudflow
  (``ricky_lahar``, RICKY; rain assumed) and the hand-entered hourly CAB counts (``cab_hourly_counts``). COSPEC, the daily PIE counts and
  the radio maps carry dates only and keep them (``clock_status`` ``date_only``).
  A label that no rule covers (rain and mudflow from 15 Nov) keeps its recorded
  value, flagged ``unverified``.

Every converted record also gets ``clock_uncertainty_s`` (nominal absolute-time
uncertainty of its rule). With ``time_basis: "recorded"`` every helper here is
a no-op and recorded times are kept.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd

from flovopy.core.clock import ClockRegimes

TABLE = Path(__file__).resolve().parents[1] / "clock_regimes.csv"
# Marker for products whose labels are calendar dates (no time of day): kept as written.
DATE_ONLY = "date_only"
# BOB product -> clock-table stream, or {series: stream} where series differ. "BOB" names the archive;
# no data were time-stamped by the BOB PC. Each stream is the clock of the PC that acquired the data:
# RSAM/EVNT on the Tandy Model 100, SSAM on WILLIE (MDETECT), tilt on SLEEPY, lahar (and, by inference,
# rain) on RICKY. The hourly CAB counts were entered by hand and have their own rule.
BOB_PRODUCT_STREAMS = {
    "rsam": "tandy_rsam", "rsam_events": "tandy_rsam", "ssam": "ssam", "ssam_data": "ssam",
    "tilt": "sleepy_tilt", "rain": "ricky_lahar", "mudflow": "ricky_lahar",
    "counts": {"CAB_CNT.DAT": "cab_hourly_counts", "CAB_HR.DAT": "cab_hourly_counts", "PIE_CNT.DAT": DATE_ONLY},
    "cospec": DATE_ONLY, "radio": DATE_ONLY,
}


def bob_stream(product: str, series=None):
    """Clock-table stream for a BOB product (and series), DATE_ONLY, or None if no rule exists."""
    stream = BOB_PRODUCT_STREAMS.get(product)
    if isinstance(stream, dict):
        return stream.get(str(series).split("/")[-1])
    return stream


def time_basis(config) -> str:
    basis = str(config.get("time_basis", "utc")).lower()
    if basis not in ("recorded", "utc"):
        raise ValueError('time_basis must be "recorded" or "utc"')
    return basis


def utc_enabled(config) -> bool:
    return time_basis(config) == "utc"


@lru_cache(maxsize=4)
def regimes(path: str | None = None) -> ClockRegimes:
    return ClockRegimes.read_csv(path or TABLE)


def table_path(config) -> Path:
    value = config.get("clock_regimes")
    if not value:
        return TABLE
    path = Path(value).expanduser()
    return path if path.is_absolute() else Path(config["_config_file"]).parent / path


def _regimes(config) -> ClockRegimes:
    return regimes(str(table_path(config)))


def waveform_time_correction(config):
    """Return a FloVoPy convert_dmx_to_seisan ``time_correction`` callable, or None."""
    if not utc_enabled(config):
        return None
    table = _regimes(config)

    def correct(path, event_id, starttime, header=None):
        marker = (header or {}).get("localtime")
        info = table.lookup("waveform", starttime, event_id, marker)
        return info["offset_s"], info["regime"] if info["offset_s"] is not None else info["status"]
    return correct


def apply_pick_clock(df: pd.DataFrame, config, stream: str) -> pd.DataFrame:
    """Convert ``pick_time`` to UTC for step 20 (individual) or 21 (monthly)."""
    if not utc_enabled(config) or df.empty:
        return df
    table = _regimes(config)
    out = df.copy()
    if stream == "pick_individual":
        out["_seq"] = out["event_id"].astype(str)
    elif stream == "pick_monthly":
        out["_seq"] = (out["pha_file"].astype(str) + ":" +
                       out["raw_lineno"].astype(int).map("{:06d}".format))
    else:
        raise ValueError(stream)
    out = table.apply(out, stream, "pick_time", seq_column="_seq")
    return out.drop(columns="_seq")


def apply_hypocenter_clock(pinaall: pd.DataFrame, allsum: pd.DataFrame, config,
                           waveform_index: pd.DataFrame | None = None):
    """Convert ``origin_time`` of the PINAALL.DAT and Pinatubo_all.sum indexes.

    Pinatubo_all.sum is in time order (and relocated), so its records in the
    repeated 26 May wall-clock window cannot be placed by position. Each is
    resolved, in order of preference, by

    1. the PINAALL.DAT record within 1 s and 0.05 degrees (takes its regime);
    2. the UTC-run waveform index (step 10): the hypocenters' wall clock is
       H - 7 h, so the DMX file whose raw header window contains raw + 7 h
       (10 s pretrigger allowed) decides, provided exactly one file does.

    Records still unresolved stay ``ambiguous`` with no time.
    """
    if not utc_enabled(config):
        return pinaall, allsum
    table = _regimes(config)
    pinaall = pinaall.copy()
    pinaall["_seq"] = pinaall["source_line"].astype(int).map("{:06d}".format)
    pinaall = table.apply(pinaall, "hypo_pinaall", "origin_time", seq_column="_seq").drop(columns="_seq")
    allsum = table.apply(allsum, "hypo_allsum", "origin_time")
    ref = pinaall.assign(_raw=pd.to_datetime(pinaall.raw_origin_time, format="ISO8601"))
    wav = None
    if waveform_index is not None and "raw_starttime" in waveform_index:
        wav = waveform_index.assign(
            _start=pd.to_datetime(waveform_index.raw_starttime, format="ISO8601"),
            _end=pd.to_datetime(waveform_index.raw_starttime, format="ISO8601") +
            (pd.to_datetime(waveform_index.endtime, format="ISO8601") -
             pd.to_datetime(waveform_index.starttime, format="ISO8601")))

    def resolve(idx, offset, uncertainty, regime, how):
        raw = pd.Timestamp(allsum.at[idx, "raw_origin_time"])
        allsum.loc[idx, "origin_time"] = (raw + pd.Timedelta(seconds=offset)).isoformat()
        allsum.loc[idx, ["clock_regime", "clock_offset_s", "clock_uncertainty_s", "clock_confidence",
                         "clock_status"]] = [regime, offset, uncertainty, "high", how]

    for idx in allsum.index[allsum.clock_status.eq("ambiguous")]:
        row = allsum.loc[idx]
        raw = pd.Timestamp(row.raw_origin_time)
        hit = ref[((ref._raw - raw).abs() <= pd.Timedelta(seconds=1)) &
                  ((ref.latitude - row.latitude).abs() <= 0.05) &
                  ((ref.longitude - row.longitude).abs() <= 0.05) &
                  ref.clock_status.eq("resolved")]
        if len(hit) == 1:
            resolve(idx, float(hit.clock_offset_s.iloc[0]), float(hit.clock_uncertainty_s.iloc[0]),
                    hit.clock_regime.iloc[0] + "_matched",
                    f"resolved_by_pinaall_line_{int(hit.source_line.iloc[0])}")
            continue
        if wav is not None:
            h = raw + pd.Timedelta(hours=7)
            files = wav[(wav._start - pd.Timedelta(seconds=10) <= h) & (h <= wav._end)]
            if len(files) == 1:
                marker = files.dmx_localtime.iloc[0] if "dmx_localtime" in files else None
                info = table.lookup("waveform", files._start.iloc[0].isoformat(), files.event_id.iloc[0],
                                    None if pd.isna(marker) or ";" in str(marker) else marker)
                if info["offset_s"] is not None:
                    # Hypocenter offset = waveform offset + 7 h (W = H - 7 h in May).
                    resolve(idx, info["offset_s"] + 7 * 3600.0, info["uncertainty_s"], "W_from_" + info["regime"],
                            f"resolved_by_waveform_{files.event_id.iloc[0]}")
    return pinaall, allsum


def apply_bob_clock(frame: pd.DataFrame, config, product: str) -> pd.DataFrame:
    """Convert BOB ``recorded_time`` labels to UTC where a clock rule exists.

    Labels that no rule covers keep their recorded value (``clock_status`` ``unverified``);
    date-only labels are kept as written (``date_only``). The raw label is always kept in
    ``raw_recorded_time``.
    """
    if not utc_enabled(config):
        return frame
    out = frame.copy()
    times = pd.to_datetime(out["recorded_time"], errors="coerce")
    series = out["series"] if "series" in out else pd.Series(None, index=out.index)
    streams = series.map(lambda s: bob_stream(product, s))
    table = _regimes(config)
    lookup = {}
    for stream, t in set(zip(streams, times)):
        if pd.isna(t):
            info = dict(regime="no_time", offset_s=None, uncertainty_s=None, confidence=None, status="no_time")
        elif stream == DATE_ONLY:
            info = dict(regime="date_label_retained", offset_s=None, uncertainty_s=None, confidence=None,
                        status="date_only")
        elif stream is None:
            info = dict(regime="unverified_label_retained", offset_s=None, uncertainty_s=None, confidence=None,
                        status="unverified")
        else:
            info = table.lookup(stream, pd.Timestamp(t).isoformat())
            if info["offset_s"] is None:  # outside every rule for this stream
                info = dict(info, regime="unverified_label_retained", status="unverified")
        lookup[(stream, t)] = info
    infos = [lookup[(s, t)] for s, t in zip(streams, times)]
    offsets = pd.Series([i["offset_s"] for i in infos], index=out.index, dtype=float)
    out["raw_recorded_time"] = out["recorded_time"]
    out["recorded_time"] = times + pd.to_timedelta(offsets.fillna(0), unit="s")
    out["clock_regime"] = [i["regime"] for i in infos]
    out["clock_offset_s"] = offsets
    out["clock_uncertainty_s"] = pd.Series([i["uncertainty_s"] for i in infos], index=out.index, dtype=float)
    out["clock_confidence"] = [i["confidence"] for i in infos]
    out["clock_status"] = [i["status"] for i in infos]
    return out


TIME_POLICY = {
    "recorded": "Recorded labels retained; no timezone or drift correction",
    "utc": ("Times converted to UTC with 10_conversion/clock_regimes.csv; raw labels kept in "
            "raw_* columns; COSPEC, daily PIE counts and radio maps keep their dates; rain and mudflow "
            "labels from 15 Nov 1991 are retained (unverified)"),
}
