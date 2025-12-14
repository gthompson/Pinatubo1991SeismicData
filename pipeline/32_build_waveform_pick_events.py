#!/usr/bin/env python3
"""
32_build_event_catalog.py

STEP 32 of the Pinatubo FAIR pipeline.

Build the authoritative event catalog by integrating:
  - STEP 10 waveform index (waveform-defined event windows)
  - STEP 22 merged pick table (primary + secondary picks with provenance)
  - STEP 30 individual pick ↔ waveform map
    Used ONLY to bind authoritative analyst picks to waveform events.
    Step 32 never infers events from Step 30 — it only enriches waveform events.

Event classes:
  - WAV_ONLY    : waveform exists, no picks assigned
  - WAV+PICKS   : waveform exists, ≥1 pick assigned
  - PICKS_ONLY  : picks exist, no waveform matched/available

Event IDs:
  - waveform-defined events:  event_id = f"wav_{waveform_id}"
  - pick-only events:        event_id = f"pick_{pick_group_id}"  (pick_group_id == STEP22.event_id)

Hard guarantees:
  - No pick_id appears in more than one output event
  - No pick row is silently dropped (unassigned picks become PICKS_ONLY events)

Notes:
  - Authoritative pick times for individual picks are reconstructed from STEP 30 offsets
    and STEP 10 waveform start times (avoids PHA timestamp quirks).
  - Monthly picks are attached to waveform events only by time-window inclusion (± tolerance).
  - STEP 22 is assumed to have already suppressed monthly picks duplicating primary picks
    (seed_norm+phase within tolerance) at the global level.

Outputs:
  - Event CSV: one row per event
  - Pick map CSV: one row per assigned pick (individual + monthly), preserving provenance
  - QC CSV: one row per event, plus run-level summary
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Tuple, Optional, List

import pandas as pd


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def validate_step30_map(df_map: pd.DataFrame, df_picks: pd.DataFrame) -> None:
    """
    Sanity checks:
      - All pick_id values in Step 30 must exist in Step 22
      - No pick_id appears multiple times with different waveform_id
    """
    missing = set(df_map["pick_id"]) - set(df_picks["pick_id"])
    if missing:
        raise ValueError(
            f"STEP 30 map contains pick_id values not present in STEP 22: "
            f"{sorted(list(missing))[:10]} ..."
        )

    dup = (
        df_map.groupby("pick_id")["waveform_id"]
        .nunique()
        .reset_index()
        .query("waveform_id > 1")
    )
    if not dup.empty:
        raise ValueError(
            "STEP 30 map assigns the same pick_id to multiple waveforms. "
            "This violates analyst-pick assumptions."
        )

def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="mixed", utc=True, errors="coerce")


def normalize_seed_id_df(df: pd.DataFrame) -> pd.Series:
    """
    Prefer seed_id if present; otherwise construct XB.<station>..<channel>.
    """
    sid = df.get("seed_id", pd.Series([pd.NA] * len(df), index=df.index))
    sid = sid.astype("string").fillna("").str.strip()
    has = sid.ne("")

    sta = df.get("station", pd.Series([""] * len(df), index=df.index)).astype("string").fillna("").str.strip()
    cha = df.get("channel", pd.Series([""] * len(df), index=df.index)).astype("string").fillna("").str.strip()

    fallback = "XB." + sta + ".." + cha
    return sid.where(has, fallback).astype("string")


def ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def infer_pick_priority(df: pd.DataFrame) -> pd.Series:
    """
    Prefer existing pick_priority if provided by STEP 22.
    Otherwise infer from event_id_source / provenance.
    """
    if "pick_priority" in df.columns:
        s = df["pick_priority"].astype("string").fillna("").str.lower().str.strip()
        # normalize variants
        s = s.replace({"prim": "primary", "sec": "secondary"})
        # fallback inference where blank
        blank = s.eq("")
        if blank.any():
            src = df.get("event_id_source", pd.Series([""] * len(df), index=df.index)).astype("string").fillna("").str.lower()
            s = s.mask(blank & src.str.contains("individual"), "primary")
            s = s.mask(blank & ~src.str.contains("individual"), "secondary")
        return s

    src = df.get("event_id_source", pd.Series([""] * len(df), index=df.index)).astype("string").fillna("").str.lower()
    out = pd.Series(["secondary"] * len(df), index=df.index, dtype="string")
    out = out.mask(src.str.contains("individual"), "primary")
    return out


def dedupe_picks_within_event(
    df: pd.DataFrame,
    *,
    time_col: str,
    tol: pd.Timedelta,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Event-local dedupe by (seed_norm, phase) within tolerance.
    Keeps first occurrence in sort order.
    """
    if df.empty:
        return df.copy(), df.iloc[0:0].copy()

    df = df.copy()

    # Ensure keys exist
    df = ensure_cols(df, ["seed_norm", "phase"])
    df["phase"] = df["phase"].astype("string")

    # Stable ordering: primary first, then by time
    if "pick_priority" in df.columns:
        pri = df["pick_priority"].astype("string").fillna("")
        # primary sorts before secondary
        pri_rank = pri.map({"primary": 0, "secondary": 1}).fillna(2).astype(int)
        df["_pri_rank"] = pri_rank
        df = df.sort_values(["_pri_rank", "seed_norm", "phase", time_col]).reset_index(drop=True)
    else:
        df = df.sort_values(["seed_norm", "phase", time_col]).reset_index(drop=True)

    key_cols = ["seed_norm", "phase"]
    df["_dt"] = df.groupby(key_cols)[time_col].diff()

    dup_mask = df["_dt"].notna() & (df["_dt"].abs() <= tol)

    dropped = df.loc[dup_mask].drop(columns=["_dt", "_pri_rank"], errors="ignore")
    kept = df.loc[~dup_mask].drop(columns=["_dt", "_pri_rank"], errors="ignore")

    return kept.reset_index(drop=True), dropped.reset_index(drop=True)


# -----------------------------------------------------------------------------
# Core build
# -----------------------------------------------------------------------------

def build_event_catalog(
    df_wav: pd.DataFrame,
    df_picks: pd.DataFrame,
    df_map: pd.DataFrame,
    *,
    tol_s: float,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    tol = pd.Timedelta(seconds=float(tol_s))

    # -----------------------------
    # Normalize/validate waveform index
    # -----------------------------
    df_wav = df_wav.copy()
    for col in ("event_id", "starttime", "endtime"):
        if col not in df_wav.columns:
            raise ValueError(f"waveform-index missing required column: {col}")

    df_wav["starttime"] = to_dt(df_wav["starttime"])
    df_wav["endtime"] = to_dt(df_wav["endtime"])

    wav_bad = df_wav["starttime"].isna() | df_wav["endtime"].isna()
    if wav_bad.any():
        df_wav = df_wav.loc[~wav_bad].reset_index(drop=True)

    # -----------------------------
    # Normalize/validate merged picks (STEP 22)
    # -----------------------------
    df_picks = df_picks.copy()
    if "pick_id" not in df_picks.columns:
        raise ValueError("merged-picks (Step 22) missing required column: pick_id")
    if "event_id" not in df_picks.columns:
        raise ValueError("merged-picks (Step 22) missing required column: event_id (pick group id)")
    if "pick_time" not in df_picks.columns:
        raise ValueError("merged-picks (Step 22) missing required column: pick_time")

    df_picks["pick_time"] = to_dt(df_picks["pick_time"])
    # Keep NaT picks for QC, but they cannot be assigned by time-window logic
    df_picks["seed_norm"] = normalize_seed_id_df(df_picks)
    df_picks["pick_priority"] = infer_pick_priority(df_picks)

    # Convenience splits
    df_primary = df_picks[df_picks["pick_priority"].astype("string") == "primary"].copy()
    df_secondary = df_picks[df_picks["pick_priority"].astype("string") == "secondary"].copy()

    # -----------------------------
    # Normalize/validate mapping (STEP 30)
    # -----------------------------
    df_map = df_map.copy()
    # Validate STEP 30 mapping integrity
    validate_step30_map(df_map, df_picks)
    for col in ("pick_id", "waveform_id", "time_offset_from_start"):
        if col not in df_map.columns:
            raise ValueError(f"individual-pick-waveform-map (Step 30) missing required column: {col}")

    # Map can (and should) include only primary picks; we won't assume, we'll enforce later.
    # Ensure numeric offsets
    df_map["time_offset_from_start"] = pd.to_numeric(df_map["time_offset_from_start"], errors="coerce")

    # Join mapping -> pick attributes for authoritative individual picks
    ind_full = df_map.merge(
        df_primary,
        on="pick_id",
        how="left",
        validate="many_to_one",
        suffixes=("", "_pick"),
    )

    # Attach waveform start times for reconstruction
    ind_full = ind_full.merge(
        df_wav[["event_id", "starttime", "endtime", "wav_file"]],
        left_on="waveform_id",
        right_on="event_id",
        how="left",
        suffixes=("", "_wav"),
    )

    ind_full["pick_time_reconstructed"] = (
        ind_full["starttime"] + pd.to_timedelta(ind_full["time_offset_from_start"], unit="s")
    )

    # Keep only recon-good and waveform-known (if waveform missing, this mapping is unusable)
    ind_bad = (
        ind_full["pick_time_reconstructed"].isna()
        | ind_full["waveform_id"].isna()
        | ind_full["starttime"].isna()
        | ind_full["endtime"].isna()
    )
    ind_full_good = ind_full.loc[~ind_bad].copy().reset_index(drop=True)

    # Group authoritative picks by waveform_id
    ind_by_wav = (
        ind_full_good.groupby("waveform_id", sort=False)
        if not ind_full_good.empty else None
    )

    # -----------------------------
    # PASS 1: waveform-defined events
    # -----------------------------
    assigned_pick_ids = set()

    events_rows: List[dict] = []
    pick_rows: List[dict] = []
    qc_rows: List[dict] = []

    # Pre-filter secondary picks with valid times only (time-window matching)
    sec_valid_time = df_secondary["pick_time"].notna()
    df_secondary_time = df_secondary.loc[sec_valid_time].copy()

    for _, w in df_wav.iterrows():
        waveform_id = str(w["event_id"])
        start = w["starttime"]
        end = w["endtime"]
        wav_file = w.get("wav_file")

        ev_id = f"wav_{waveform_id}"

        # ---- authoritative picks for this waveform via STEP 30
        if ind_by_wav is not None and waveform_id in ind_by_wav.groups:
            ind_picks = ind_by_wav.get_group(waveform_id).copy()
        else:
            ind_picks = pd.DataFrame()

        # Build authoritative pick table for output mapping
        n_primary = 0
        n_primary_dedup = 0

        if not ind_picks.empty:
            # Ensure keys
            ind_picks = ensure_cols(ind_picks, ["seed_norm", "phase", "raw_line", "raw_lineno", "pha_file"])
            ind_picks["seed_norm"] = ind_picks.get("seed_norm", normalize_seed_id_df(ind_picks))
            ind_picks["pick_priority"] = "primary"

            # event-local dedupe (just in case mapping duplicates exist)
            ind_tmp = ind_picks.rename(columns={"pick_time_reconstructed": "pick_time_evt"}).copy()
            kept, dropped = dedupe_picks_within_event(ind_tmp, time_col="pick_time_evt", tol=tol)
            n_primary = int(len(ind_tmp))
            n_primary_dedup = int(len(dropped))

            ind_keep = kept.copy()
            ind_keep["pick_time_evt"] = ind_keep["pick_time_evt"]  # explicit
        else:
            ind_keep = pd.DataFrame()
            n_primary = 0
            n_primary_dedup = 0

        # Mark assigned primary picks
        if not ind_keep.empty:
            for pid in ind_keep["pick_id"].astype("string").fillna("").tolist():
                if pid:
                    assigned_pick_ids.add(pid)

        # ---- secondary picks in waveform window (± tol), not yet assigned
        mon = df_secondary_time[
            (df_secondary_time["pick_time"] >= (start - tol)) &
            (df_secondary_time["pick_time"] <= (end + tol))
        ].copy()

        # exclude already assigned pick_ids
        if not mon.empty:
            mon["pick_id"] = mon["pick_id"].astype("string")
            mon = mon[~mon["pick_id"].isin(assigned_pick_ids)].copy()

        # event-local dedupe for monthly picks (optional but highly stabilizing)
        n_secondary = int(len(mon))
        n_secondary_dedup = 0
        if not mon.empty:
            mon = ensure_cols(mon, ["seed_norm", "phase"])
            mon["seed_norm"] = mon.get("seed_norm", normalize_seed_id_df(mon))
            mon["pick_priority"] = "secondary"
            mon_tmp = mon.rename(columns={"pick_time": "pick_time_evt"}).copy()
            mon_keep, mon_drop = dedupe_picks_within_event(mon_tmp, time_col="pick_time_evt", tol=tol)
            n_secondary_dedup = int(len(mon_drop))
        else:
            mon_keep = pd.DataFrame()

        # mark assigned secondary picks
        if not mon_keep.empty:
            for pid in mon_keep["pick_id"].astype("string").fillna("").tolist():
                if pid:
                    assigned_pick_ids.add(pid)

        # ---- Emit pick map rows
        n_out_picks = 0

        if not ind_keep.empty:
            for _, p in ind_keep.iterrows():
                pt = p["pick_time_evt"]
                n_out_picks += 1
                pick_rows.append({
                    "event_id": ev_id,
                    "event_class": None,  # filled later
                    "waveform_id": waveform_id,
                    "wav_file": wav_file,
                    "pick_id": p.get("pick_id"),
                    "pick_source": "individual",
                    "pick_priority": "primary",
                    "pick_group_id": p.get("event_id"),  # STEP22 group id (the individual group id)
                    "event_id_source": p.get("event_id_source"),
                    "seed_id": p.get("seed_id"),
                    "seed_norm": p.get("seed_norm"),
                    "phase": p.get("phase"),
                    "pick_time": pd.Timestamp(pt).isoformat() if pd.notna(pt) else None,
                    "time_offset": float((pt - start).total_seconds()) if pd.notna(pt) else None,
                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),
                    "pha_file": p.get("pha_file"),
                })

        if not mon_keep.empty:
            for _, p in mon_keep.iterrows():
                pt = p["pick_time_evt"]
                n_out_picks += 1
                pick_rows.append({
                    "event_id": ev_id,
                    "event_class": None,  # filled later
                    "waveform_id": waveform_id,
                    "wav_file": wav_file,
                    "pick_id": p.get("pick_id"),
                    "pick_source": "monthly",
                    "pick_priority": "secondary",
                    "pick_group_id": p.get("event_id"),  # STEP22 group id (monthly block id)
                    "event_id_source": p.get("event_id_source"),
                    "seed_id": p.get("seed_id"),
                    "seed_norm": p.get("seed_norm"),
                    "phase": p.get("phase"),
                    "pick_time": pd.Timestamp(pt).isoformat() if pd.notna(pt) else None,
                    "time_offset": float((pt - start).total_seconds()) if pd.notna(pt) else None,
                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),
                    "pha_file": p.get("pha_file"),
                })

        # ---- Event-level row
        has_primary = not ind_keep.empty
        has_secondary = not mon_keep.empty
        has_waveform = True

        if has_primary or has_secondary:
            event_class = "WAV+PICKS"
        else:
            event_class = "WAV_ONLY"

        events_rows.append({
            "event_id": ev_id,
            "event_class": event_class,
            "waveform_id": waveform_id,
            "wav_file": wav_file,
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "has_waveform": True,
            "has_primary_picks": bool(has_primary),
            "has_secondary_picks": bool(has_secondary),
            "n_picks_total": int((len(ind_keep) if not ind_keep.empty else 0) + (len(mon_keep) if not mon_keep.empty else 0)),
            "n_primary_picks": int(len(ind_keep)) if not ind_keep.empty else 0,
            "n_secondary_picks": int(len(mon_keep)) if not mon_keep.empty else 0,
        })

        qc_rows.append({
            "event_id": ev_id,
            "event_class": event_class,
            "waveform_id": waveform_id,
            "has_waveform": True,
            "n_primary_picks_raw": n_primary,
            "n_primary_picks_dedup_dropped": n_primary_dedup,
            "n_secondary_picks_in_window_raw": n_secondary,
            "n_secondary_picks_dedup_dropped": n_secondary_dedup,
            "n_picks_assigned_total": int((len(ind_keep) if not ind_keep.empty else 0) + (len(mon_keep) if not mon_keep.empty else 0)),
            "notes": "",
        })

    # -----------------------------
    # PASS 2: pick-only events (all remaining picks)
    # -----------------------------
    df_picks_all = df_picks.copy()
    df_picks_all["pick_id"] = df_picks_all["pick_id"].astype("string")
    remaining = df_picks_all[~df_picks_all["pick_id"].isin(assigned_pick_ids)].copy()

    # Picks with NaT times cannot be window-matched; they still become PICKS_ONLY groups (QC them)
    # Group key for pick-only events is STEP22.event_id
    if not remaining.empty:
        remaining = ensure_cols(remaining, ["seed_norm", "phase", "raw_line", "raw_lineno", "pha_file"])
        remaining["seed_norm"] = remaining.get("seed_norm", normalize_seed_id_df(remaining))
        remaining["pick_priority"] = infer_pick_priority(remaining)

        for pick_group_id, g in remaining.groupby("event_id", sort=True):
            ev_id = f"pick_{pick_group_id}"

            # compute start/end from pick_time if possible
            g_times = g["pick_time"].dropna()
            if not g_times.empty:
                start = g_times.min()
                end = g_times.max()
                start_s = start.isoformat()
                end_s = end.isoformat()
            else:
                start_s = None
                end_s = None

            # event-level row
            has_primary = (g["pick_priority"].astype("string") == "primary").any()
            has_secondary = (g["pick_priority"].astype("string") == "secondary").any()

            events_rows.append({
                "event_id": ev_id,
                "event_class": "PICKS_ONLY",
                "waveform_id": pd.NA,
                "wav_file": pd.NA,
                "starttime": start_s,
                "endtime": end_s,
                "has_waveform": False,
                "has_primary_picks": bool(has_primary),
                "has_secondary_picks": bool(has_secondary),
                "n_picks_total": int(len(g)),
                "n_primary_picks": int((g["pick_priority"].astype("string") == "primary").sum()),
                "n_secondary_picks": int((g["pick_priority"].astype("string") == "secondary").sum()),
            })

            qc_rows.append({
                "event_id": ev_id,
                "event_class": "PICKS_ONLY",
                "waveform_id": pd.NA,
                "has_waveform": False,
                "n_primary_picks_raw": int((g["pick_priority"].astype("string") == "primary").sum()),
                "n_primary_picks_dedup_dropped": 0,
                "n_secondary_picks_in_window_raw": 0,
                "n_secondary_picks_dedup_dropped": 0,
                "n_picks_assigned_total": int(len(g)),
                "notes": "pick-only event (no waveform match/available)",
            })

            # pick-map rows
            for _, p in g.iterrows():
                pt = p.get("pick_time")
                pick_rows.append({
                    "event_id": ev_id,
                    "event_class": "PICKS_ONLY",
                    "waveform_id": pd.NA,
                    "wav_file": pd.NA,
                    "pick_id": p.get("pick_id"),
                    "pick_source": "individual" if str(p.get("pick_priority", "")).lower() == "primary" else "monthly",
                    "pick_priority": p.get("pick_priority"),
                    "pick_group_id": pick_group_id,
                    "event_id_source": p.get("event_id_source"),
                    "seed_id": p.get("seed_id"),
                    "seed_norm": p.get("seed_norm"),
                    "phase": p.get("phase"),
                    "pick_time": pd.Timestamp(pt).isoformat() if pd.notna(pt) else None,
                    "time_offset": None,
                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),
                    "pha_file": p.get("pha_file"),
                })

    # -----------------------------
    # Finalize pick_map event_class for waveform events
    # -----------------------------
    df_events = pd.DataFrame(events_rows)
    df_pickmap = pd.DataFrame(pick_rows)
    df_qc = pd.DataFrame(qc_rows)

    if not df_pickmap.empty and not df_events.empty:
        # Fill event_class for wav_* picks
        ev_class_map = dict(zip(df_events["event_id"].astype(str), df_events["event_class"].astype(str)))
        df_pickmap["event_class"] = df_pickmap["event_id"].astype(str).map(ev_class_map)

    # Sort for determinism
    if not df_events.empty:
        df_events = df_events.sort_values(["event_class", "event_id"]).reset_index(drop=True)
    if not df_pickmap.empty:
        df_pickmap = df_pickmap.sort_values(["event_id", "pick_priority", "seed_norm", "phase", "pick_time"]).reset_index(drop=True)
    if not df_qc.empty:
        df_qc = df_qc.sort_values(["event_class", "event_id"]).reset_index(drop=True)

    # Run-level QC summary row
    total_picks = int(len(df_picks))
    assigned_picks = int(df_pickmap["pick_id"].astype("string").nunique()) if not df_pickmap.empty else 0
    nat_picks = int(df_picks["pick_time"].isna().sum())

    df_qc = pd.concat(
        [
            df_qc,
            pd.DataFrame([{
                "event_id": "__RUN_SUMMARY__",
                "event_class": "",
                "waveform_id": "",
                "has_waveform": "",
                "n_primary_picks_raw": "",
                "n_primary_picks_dedup_dropped": "",
                "n_secondary_picks_in_window_raw": "",
                "n_secondary_picks_dedup_dropped": "",
                "n_picks_assigned_total": "",
                "notes": (
                    f"total_picks={total_picks}; "
                    f"assigned_unique_pick_ids={assigned_picks}; "
                    f"unparseable_pick_time_rows={nat_picks}; "
                    f"waveform_events={int((df_events['event_class'].str.startswith('WAV')).sum() if not df_events.empty else 0)}; "
                    f"pick_only_events={int((df_events['event_class'] == 'PICKS_ONLY').sum() if not df_events.empty else 0)}"
                ),
            }]),
        ],
        ignore_index=True,
    )

    return df_events, df_pickmap, df_qc


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="STEP 32: Build authoritative event catalog")
    ap.add_argument("--waveform-index", required=True, help="STEP 10 waveform index CSV")
    ap.add_argument("--merged-picks", required=True, help="STEP 22 merged picks CSV")
    ap.add_argument("--individual-pick-waveform-map", required=True, help="STEP 30 pick↔waveform map CSV")

    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-pick-map-csv", required=True)
    ap.add_argument("--out-qc-csv", required=True)

    ap.add_argument("--time-tolerance", type=float, default=0.5)

    args = ap.parse_args()

    df_wav = pd.read_csv(args.waveform_index)
    df_picks = pd.read_csv(args.merged_picks)
    df_map = pd.read_csv(args.individual_pick_waveform_map)

    df_events, df_pickmap, df_qc = build_event_catalog(
        df_wav, df_picks, df_map, tol_s=float(args.time_tolerance)
    )

    out_event = Path(args.out_event_csv)
    out_pick = Path(args.out_pick_map_csv)
    out_qc = Path(args.out_qc_csv)

    out_event.parent.mkdir(parents=True, exist_ok=True)
    out_pick.parent.mkdir(parents=True, exist_ok=True)
    out_qc.parent.mkdir(parents=True, exist_ok=True)

    df_events.to_csv(out_event, index=False)
    df_pickmap.to_csv(out_pick, index=False)
    df_qc.to_csv(out_qc, index=False)

    # Console summary
    wav_only = int((df_events["event_class"] == "WAV_ONLY").sum()) if not df_events.empty else 0
    wav_picks = int((df_events["event_class"] == "WAV+PICKS").sum()) if not df_events.empty else 0
    picks_only = int((df_events["event_class"] == "PICKS_ONLY").sum()) if not df_events.empty else 0

    print("\n=== STEP 32 COMPLETE ===")
    print(f"Time tolerance (s):     {float(args.time_tolerance)}")
    print(f"Events:                 {len(df_events)}")
    print(f"  WAV_ONLY:             {wav_only}")
    print(f"  WAV+PICKS:            {wav_picks}")
    print(f"  PICKS_ONLY:           {picks_only}")
    print(f"Pick mappings (rows):   {len(df_pickmap)}")
    print(f"Event CSV:              {out_event}")
    print(f"Pick map CSV:           {out_pick}")
    print(f"QC CSV:                 {out_qc}")


if __name__ == "__main__":
    main()