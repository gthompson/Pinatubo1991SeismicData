#!/usr/bin/env python3
"""
31_build_waveform_pick_events.py

STEP 31 of the Pinatubo FAIR pipeline.

Build the authoritative waveform-centered event catalog.

Inputs:
- Waveform index (STEP 10)
- Individual pick index (STEP 20)           [authoritative picks]
- Individual pick ↔ waveform map (STEP 30)  [authoritative mapping to waveform files]
- Monthly pick index (STEP 21)              [secondary picks]

Rules:
- Each waveform file defines ONE event: event_id = "wav_<waveform_event_id>"
- Individual picks are authoritative (as mapped by Step 30)
- Monthly picks are added only if:
    * they fall inside waveform window (± tolerance)
    * they do NOT duplicate authoritative picks (same seed_norm + phase within tolerance)

Outputs:
- Event CSV: one row per waveform event
- Pick map CSV: one row per pick assigned to a waveform event (individual + monthly)
- QC CSV: one row per waveform event with diagnostics + global counters
"""

from __future__ import annotations

from pathlib import Path
import argparse
import pandas as pd


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="mixed", utc=True, errors="coerce")


def normalize_seed_id(df: pd.DataFrame) -> pd.Series:
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


def safe_read_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Missing input: {p}")
    return pd.read_csv(p)


def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="STEP 31: Build waveform-centered event catalog")

    ap.add_argument("--waveform-index", required=True)
    ap.add_argument("--individual-pick-index", required=True)
    ap.add_argument("--individual-pick-waveform-map", required=True)  # Step 30 output
    ap.add_argument("--monthly-picks", required=True)

    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-pick-map-csv", required=True)
    ap.add_argument("--out-qc-csv", required=True)

    ap.add_argument("--time-tolerance", type=float, default=0.5)

    args = ap.parse_args()
    tol = pd.Timedelta(seconds=float(args.time_tolerance))

    print("=== STEP 31: Building waveform-centered event catalog ===")

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------
    df_wav = safe_read_csv(args.waveform_index)
    df_ind = safe_read_csv(args.individual_pick_index)
    df_map = safe_read_csv(args.individual_pick_waveform_map)
    df_mon = safe_read_csv(args.monthly_picks)

    # ------------------------------------------------------------------
    # Validate minimum schemas
    # ------------------------------------------------------------------
    for col in ("event_id", "starttime", "endtime"):
        if col not in df_wav.columns:
            raise ValueError(f"waveform-index missing required column: {col}")

    for col in ("pick_id", "event_id", "event_id_source", "pick_time"):
        if col not in df_ind.columns:
            raise ValueError(f"individual-pick-index (Step 20) missing required column: {col}")

    # Step 30 schema (from the regenerated Step 30)
    for col in ("pick_id", "source_event_id", "waveform_event_id", "time_offset_from_start"):
        if col not in df_map.columns:
            raise ValueError(f"individual-pick-waveform-map (Step 30) missing required column: {col}")

    for col in ("pick_time", "event_id", "event_id_source"):
        if col not in df_mon.columns:
            raise ValueError(f"monthly-picks (Step 21) missing required column: {col}")

    # ------------------------------------------------------------------
    # Parse times
    # ------------------------------------------------------------------
    df_wav = df_wav.copy()
    df_wav["starttime"] = to_dt(df_wav["starttime"])
    df_wav["endtime"] = to_dt(df_wav["endtime"])

    wav_bad = df_wav["starttime"].isna() | df_wav["endtime"].isna()
    if wav_bad.any():
        n_bad = int(wav_bad.sum())
        print(f"WARNING: Dropping {n_bad} waveform rows with unparseable start/end times")
        df_wav = df_wav.loc[~wav_bad].reset_index(drop=True)

    df_ind = df_ind.copy()
    df_ind["pick_time"] = to_dt(df_ind["pick_time"])
    if df_ind["pick_time"].isna().any():
        raise SystemExit("Unparseable pick_time values detected in Step 20 individual picks")

    df_mon = df_mon.copy()
    df_mon["pick_time"] = to_dt(df_mon["pick_time"])
    # monthly may include garbage; we'll drop NaT monthly picks and QC

    # ------------------------------------------------------------------
    # Ensure provenance columns exist (raw_line, raw_lineno, pha_file)
    # ------------------------------------------------------------------
    df_ind = ensure_cols(df_ind, ["raw_line", "raw_lineno", "pha_file", "station", "channel", "seed_id", "phase"])
    df_mon = ensure_cols(df_mon, ["raw_line", "raw_lineno", "pha_file", "station", "channel", "seed_id", "phase", "pick_id"])

    # Normalize seed ids for dedupe
    df_ind["seed_norm"] = normalize_seed_id(df_ind)
    df_mon["seed_norm"] = normalize_seed_id(df_mon)

    # ------------------------------------------------------------------
    # Build authoritative pick table per waveform using Step 30 mapping
    # ------------------------------------------------------------------
    # Join Step 30 mapping -> Step 20 pick attributes
    df_ind_full = df_map.merge(
        df_ind,
        on="pick_id",
        how="left",
        validate="many_to_one",
        indicator=True,
        suffixes=("", "_ind"),
    )

    n_unjoined_picks = int((df_ind_full["_merge"] != "both").sum())
    if n_unjoined_picks > 0:
        print(f"WARNING: Step 30 mapping rows that did not join to Step 20 pick table: {n_unjoined_picks}")
    df_ind_full = df_ind_full.drop(columns=["_merge"])

    # Attach waveform starttime for reconstruction
    df_ind_full = df_ind_full.merge(
        df_wav[["event_id", "starttime"]],
        left_on="waveform_event_id",
        right_on="event_id",
        how="left",
        suffixes=("", "_wav"),
    )

    # Reconstruct absolute pick times (authoritative)
    df_ind_full["pick_time_reconstructed"] = (
        df_ind_full["starttime"] +
        pd.to_timedelta(df_ind_full["time_offset_from_start"], unit="s")
    )

    bad_recon = df_ind_full["pick_time_reconstructed"].isna()
    if bad_recon.any():
        print(f"WARNING: Dropping {int(bad_recon.sum())} authoritative mappings with bad reconstruction (missing waveform start?)")
        df_ind_full = df_ind_full.loc[~bad_recon].reset_index(drop=True)

    # Ensure seed_norm exists after join (if Step 20 row was missing, seed_norm could be NA)
    if "seed_norm" not in df_ind_full.columns:
        df_ind_full["seed_norm"] = normalize_seed_id(df_ind_full)

    ind_by_waveform = df_ind_full.groupby("waveform_event_id") if not df_ind_full.empty else None

    # ------------------------------------------------------------------
    # Monthly: drop NaT times (QC counts)
    # ------------------------------------------------------------------
    monthly_nat = df_mon["pick_time"].isna()
    n_monthly_nat = int(monthly_nat.sum())
    if n_monthly_nat > 0:
        print(f"WARNING: Monthly picks with unparseable pick_time will be ignored: {n_monthly_nat}")
        df_mon_valid = df_mon.loc[~monthly_nat].copy()
    else:
        df_mon_valid = df_mon

    # ------------------------------------------------------------------
    # Iterate waveform events
    # ------------------------------------------------------------------
    events: list[dict] = []
    pick_map: list[dict] = []
    qc_rows: list[dict] = []

    for _, w in df_wav.iterrows():
        waveform_event_id = str(w["event_id"])
        start = w["starttime"]
        end = w["endtime"]
        ev_id = f"wav_{waveform_event_id}"

        # Authoritative picks for this waveform
        if ind_by_waveform is not None and waveform_event_id in ind_by_waveform.groups:
            ind_picks = ind_by_waveform.get_group(waveform_event_id).copy()
        else:
            ind_picks = pd.DataFrame()

        # Monthly picks inside window (± tol)
        mon_in = df_mon_valid[
            (df_mon_valid["pick_time"] >= (start - tol)) &
            (df_mon_valid["pick_time"] <= (end + tol))
        ].copy()

        # Deduplicate monthly vs individual (seed_norm, phase, time within tol)
        monthly_missing_keys = 0
        monthly_suppressed = 0

        if not mon_in.empty:
            mon_keyed_mask = mon_in["seed_norm"].notna() & mon_in["phase"].notna()

            mon_keyed = mon_in.loc[mon_keyed_mask].copy()
            mon_unkeyed = mon_in.loc[~mon_keyed_mask].copy()
            monthly_missing_keys = int((~mon_keyed_mask).sum())

            if not ind_picks.empty and not mon_keyed.empty:
                ind_keyed = ind_picks[ind_picks["seed_norm"].notna() & ind_picks["phase"].notna()].copy()

                if ind_keyed.empty:
                    mon_final = pd.concat([mon_keyed, mon_unkeyed], ignore_index=True)
                else:
                    ind_block = (
                        ind_keyed.rename(columns={"pick_time_reconstructed": "pick_time"})
                        [["seed_norm", "phase", "pick_time"]]
                        .sort_values(["seed_norm", "phase", "pick_time"])
                        .reset_index(drop=True)
                    )

                    mon_block = mon_keyed.sort_values(["seed_norm", "phase", "pick_time"]).reset_index(drop=True)

                    keep_parts = []
                    supp_parts = []

                    for (sid, ph), mon_grp in mon_block.groupby(["seed_norm", "phase"], sort=False):
                        ind_grp = ind_block[(ind_block["seed_norm"] == sid) & (ind_block["phase"] == ph)]
                        if ind_grp.empty:
                            keep_parts.append(mon_grp)
                            continue

                        m = pd.merge_asof(
                            mon_grp.sort_values("pick_time"),
                            ind_grp.sort_values("pick_time"),
                            on="pick_time",
                            direction="nearest",
                            tolerance=tol,
                            suffixes=("", "_ind"),
                        )
                        matched = m["pick_time_ind"].notna()
                        supp_parts.append(mon_grp.loc[matched.values])
                        keep_parts.append(mon_grp.loc[~matched.values])

                    mon_supp = pd.concat(supp_parts, ignore_index=True) if supp_parts else mon_block.iloc[0:0].copy()
                    mon_keep = pd.concat(keep_parts, ignore_index=True) if keep_parts else mon_block.iloc[0:0].copy()

                    monthly_suppressed = int(len(mon_supp))
                    mon_final = pd.concat([mon_keep, mon_unkeyed], ignore_index=True)
            else:
                mon_final = mon_in
        else:
            mon_final = mon_in

        # Event-level outputs
        events.append({
            "event_id": ev_id,
            "waveform_event_id": waveform_event_id,
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "wav_file": w.get("wav_file"),
            "n_individual_picks": int(len(ind_picks)),
            "n_monthly_picks_in_window": int(len(mon_in)),
            "n_monthly_suppressed_vs_individual": int(monthly_suppressed),
            "n_monthly_missing_keys": int(monthly_missing_keys),
            "n_monthly_picks_kept": int(len(mon_final)),
        })

        qc_rows.append({
            "event_id": ev_id,
            "waveform_event_id": waveform_event_id,
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "n_individual_picks": int(len(ind_picks)),
            "n_monthly_in_window": int(len(mon_in)),
            "n_monthly_suppressed_vs_individual": int(monthly_suppressed),
            "n_monthly_missing_keys": int(monthly_missing_keys),
            "n_monthly_kept": int(len(mon_final)),
        })

        # Pick-map: authoritative picks (reconstructed times)
        if not ind_picks.empty:
            for _, p in ind_picks.iterrows():
                pt = p.get("pick_time_reconstructed")
                pick_map.append({
                    "event_id": ev_id,
                    "waveform_event_id": waveform_event_id,
                    "pick_id": p.get("pick_id"),
                    "pick_source": "individual",
                    "source_event_id": p.get("source_event_id"),               # from Step 30
                    "source_event_id_source": "individual_pha_filename",      # stable semantic
                    "seed_id": p.get("seed_id"),
                    "seed_norm": p.get("seed_norm"),
                    "phase": p.get("phase"),
                    "pick_time": pd.Timestamp(pt).isoformat() if pd.notna(pt) else None,
                    "time_offset": float((pt - start).total_seconds()) if pd.notna(pt) else None,
                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),
                    "pha_file": p.get("pha_file"),
                })

        # Pick-map: monthly picks (direct times)
        if not mon_final.empty:
            for _, p in mon_final.iterrows():
                pt = p.get("pick_time")
                pick_map.append({
                    "event_id": ev_id,
                    "waveform_event_id": waveform_event_id,
                    "pick_id": p.get("pick_id"),
                    "pick_source": "monthly",
                    "source_event_id": p.get("event_id"),                      # monthly block id
                    "source_event_id_source": p.get("event_id_source"),        # e.g., "monthly_pha_block"
                    "seed_id": p.get("seed_id"),
                    "seed_norm": p.get("seed_norm"),
                    "phase": p.get("phase"),
                    "pick_time": pd.Timestamp(pt).isoformat() if pd.notna(pt) else None,
                    "time_offset": float((pt - start).total_seconds()) if pd.notna(pt) else None,
                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),
                    "pha_file": p.get("pha_file"),
                })

    # Add global QC counters (single row) if you want them in the QC CSV
    qc_rows.append({
        "event_id": "__GLOBAL__",
        "waveform_event_id": pd.NA,
        "starttime": pd.NA,
        "endtime": pd.NA,
        "n_individual_picks": pd.NA,
        "n_monthly_in_window": pd.NA,
        "n_monthly_suppressed_vs_individual": pd.NA,
        "n_monthly_missing_keys": pd.NA,
        "n_monthly_kept": pd.NA,
        "n_monthly_nat_dropped": int(n_monthly_nat),
        "n_step30_unjoined_picks": int(n_unjoined_picks),
    })

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    out_event = Path(args.out_event_csv)
    out_pick = Path(args.out_pick_map_csv)
    out_qc = Path(args.out_qc_csv)

    out_event.parent.mkdir(parents=True, exist_ok=True)
    out_pick.parent.mkdir(parents=True, exist_ok=True)
    out_qc.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(events).to_csv(out_event, index=False)
    pd.DataFrame(pick_map).to_csv(out_pick, index=False)
    pd.DataFrame(qc_rows).to_csv(out_qc, index=False)

    print("\nSTEP 31 COMPLETE")
    print("----------------")
    print(f"Waveform events:      {len(events)}")
    print(f"Total pick mappings:  {len(pick_map)}")
    print(f"Event CSV:            {out_event}")
    print(f"Pick map CSV:         {out_pick}")
    print(f"QC CSV:               {out_qc}")
    print(f"Time tolerance (s):   {float(args.time_tolerance)}")


if __name__ == "__main__":
    main()