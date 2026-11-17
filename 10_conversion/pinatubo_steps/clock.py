"""Pinatubo clock corrections to UTC (used only when ``time_basis`` is "utc").

The rules live in ``10_conversion/clock_regimes.csv`` (one row per clock
period, with evidence and confidence) and are applied by
``flovopy.core.clock.ClockRegimes``. Each step converts its own raw times
where it reads them and keeps the raw value next to the corrected one:

- step 10: waveform start times (``waveform_time_correction``), stream ``waveform``
- step 20: individual PHA picks, stream ``pick_individual`` (sequence = event ID)
- step 21: monthly PHA picks, stream ``pick_monthly`` (sequence = file:line)
- step 40: hypocenters, streams ``hypo_pinaall`` (sequence = line) and
  ``hypo_allsum`` (repeated 26 May window resolved by matching PINAALL.DAT)
- steps 61-62: BOB RSAM, EVNT and SSAM labels, stream ``bob_seismic``. Other
  BOB products (tilt, rain, mudflow, COSPEC, counts, radio) have no verified
  clock and keep their recorded labels, flagged ``unverified``.

With the default ``time_basis: "recorded"`` every helper here is a no-op, so
the normal run is unchanged.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd

from flovopy.core.clock import ClockRegimes

TABLE = Path(__file__).resolve().parents[1] / "clock_regimes.csv"
BOB_SEISMIC_PRODUCTS = {"rsam", "rsam_events", "ssam", "ssam_data"}


def time_basis(config) -> str:
    basis = str(config.get("time_basis", "recorded")).lower()
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

    def correct(path, event_id, starttime):
        info = table.lookup("waveform", starttime, event_id)
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

    def resolve(idx, offset, regime, how):
        raw = pd.Timestamp(allsum.at[idx, "raw_origin_time"])
        allsum.loc[idx, "origin_time"] = (raw + pd.Timedelta(seconds=offset)).isoformat()
        allsum.loc[idx, ["clock_regime", "clock_offset_s", "clock_confidence", "clock_status"]] = [
            regime, offset, "high", how]

    for idx in allsum.index[allsum.clock_status.eq("ambiguous")]:
        row = allsum.loc[idx]
        raw = pd.Timestamp(row.raw_origin_time)
        hit = ref[((ref._raw - raw).abs() <= pd.Timedelta(seconds=1)) &
                  ((ref.latitude - row.latitude).abs() <= 0.05) &
                  ((ref.longitude - row.longitude).abs() <= 0.05) &
                  ref.clock_status.eq("resolved")]
        if len(hit) == 1:
            resolve(idx, float(hit.clock_offset_s.iloc[0]), hit.clock_regime.iloc[0] + "_matched",
                    f"resolved_by_pinaall_line_{int(hit.source_line.iloc[0])}")
            continue
        if wav is not None:
            h = raw + pd.Timedelta(hours=7)
            files = wav[(wav._start - pd.Timedelta(seconds=10) <= h) & (h <= wav._end)]
            if len(files) == 1:
                info = table.lookup("waveform", files._start.iloc[0].isoformat(), files.event_id.iloc[0])
                if info["offset_s"] is not None:
                    # Hypocenter offset = waveform offset + 7 h (W = H - 7 h in May).
                    resolve(idx, info["offset_s"] + 7 * 3600.0, "W_from_" + info["regime"],
                            f"resolved_by_waveform_{files.event_id.iloc[0]}")
    return pinaall, allsum


def apply_bob_clock(frame: pd.DataFrame, config, product: str) -> pd.DataFrame:
    """Convert BOB ``recorded_time`` labels to UTC where the clock is verified."""
    if not utc_enabled(config):
        return frame
    out = frame.copy()
    if product not in BOB_SEISMIC_PRODUCTS:
        out["raw_recorded_time"] = out["recorded_time"]
        out["clock_regime"] = "unverified_label_retained"
        out["clock_offset_s"] = float("nan")
        out["clock_confidence"] = None
        out["clock_status"] = "unverified"
        return out
    times = pd.to_datetime(out["recorded_time"], errors="coerce")
    labels = times.dropna().unique()
    table = _regimes(config)
    lookup = {t: table.lookup("bob_seismic", pd.Timestamp(t).isoformat()) for t in labels}
    out["raw_recorded_time"] = out["recorded_time"]
    offsets = times.map(lambda t: lookup[t]["offset_s"] if pd.notna(t) else None)
    out["recorded_time"] = times + pd.to_timedelta(offsets.astype(float), unit="s")
    out["clock_regime"] = times.map(lambda t: lookup[t]["regime"] if pd.notna(t) else "no_time")
    out["clock_offset_s"] = offsets.astype(float)
    out["clock_confidence"] = times.map(lambda t: lookup[t]["confidence"] if pd.notna(t) else None)
    out["clock_status"] = times.map(lambda t: lookup[t]["status"] if pd.notna(t) else "no_time")
    return out


TIME_POLICY = {
    "recorded": "Recorded labels retained; no timezone or drift correction",
    "utc": ("Times converted to UTC with 10_conversion/clock_regimes.csv; raw labels kept in "
            "raw_* columns; BOB tilt/rain/mudflow/COSPEC/counts/radio labels retained (unverified)"),
}
