#!/usr/bin/env python3
"""
31_build_waveform_pick_events.py

Build the authoritative waveform-centered event catalog.

Inputs:
- Waveform index (STEP 10)
- Individual pick index (STEP 20)
- Individual pick ↔ waveform map (STEP 30)
- Monthly pick index (STEP 21)

Rules:
- Each waveform file defines ONE event (event_id = "wav_<waveform_event_id>")
- Individual picks are authoritative
- Monthly picks are added only if:
    * they fall inside waveform window (with ± tolerance)
    * they do NOT duplicate authoritative picks (same seed_norm + phase within tolerance)
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
    Prefer seed_id if present; otherwise construct XB.<station>.. <channel>.
    """
    if "seed_id" in df.columns:
        sid = df["seed_id"].astype("string")
        sid = sid.fillna("").str.strip()
        has = sid.ne("")
    else:
        sid = pd.Series([""] * len(df), index=df.index, dtype="string")
        has = pd.Series([False] * len(df), index=df.index)

    sta = df.get("station", pd.Series([""] * len(df), index=df.index)).astype("string").fillna("").str.strip()
    cha = df.get("channel", pd.Series([""] * len(df), index=df.index)).astype("string").fillna("").str.strip()

    fallback = "XB." + sta + ".." + cha
    out = sid.where(has, fallback)
    return out.astype("string")


def safe_read_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Missing input: {p}")
    return pd.read_csv(p)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="STEP 31: Build waveform-centered event catalog")
    ap.add_argument("--waveform-index", required=True)
    ap.add_argument("--individual-pick-index", required=True)
    ap.add_argument("--individual-pick-waveform-map", required=True)
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

    # Required columns sanity (fail early with helpful messages)
    for col in ("event_id", "starttime", "endtime"):
        if col not in df_wav.columns:
            raise ValueError(f"waveform-index missing required column: {col}")

    if "pick_id" not in df_ind.columns:
        raise ValueError("individual-pick-index missing required column: pick_id")
    if "pick_id" not in df_map.columns:
        raise ValueError("individual-pick-waveform-map missing required column: pick_id")
    if "waveform_event_id" not in df_map.columns:
        raise ValueError("individual-pick-waveform-map missing required column: waveform_event_id")
    if "time_offset_from_start" not in df_map.columns:
        raise ValueError("individual-pick-waveform-map missing required column: time_offset_from_start")

    if "pick_time" not in df_mon.columns:
        raise ValueError("monthly-picks missing required column: pick_time")

    # Parse times
    df_wav = df_wav.copy()
    df_wav["starttime"] = to_dt(df_wav["starttime"])
    df_wav["endtime"] = to_dt(df_wav["endtime"])

    df_ind = df_ind.copy()
    df_ind["pick_time"] = to_dt(df_ind["pick_time"]) if "pick_time" in df_ind.columns else pd.NaT

    df_mon = df_mon.copy()
    df_mon["pick_time"] = to_dt(df_mon["pick_time"])

    # Drop waveform rows with bad times
    wav_bad_time = df_wav["starttime"].isna() | df_wav["endtime"].isna()
    if wav_bad_time.any():
        n_bad = int(wav_bad_time.sum())
        print(f"WARNING: Dropping {n_bad} waveform rows with unparseable start/end times")
        df_wav = df_wav.loc[~wav_bad_time].reset_index(drop=True)

    # Add seed_norm for dedupe
    df_ind["seed_norm"] = normalize_seed_id(df_ind)
    df_mon["seed_norm"] = normalize_seed_id(df_mon)

    # ------------------------------------------------------------------
    # Prepare authoritative individual picks with waveform association
    # ------------------------------------------------------------------

    # Join STEP 20 + STEP 30
    # df_map is many-to-one with df_ind on pick_id (one pick_id -> one pick row)
    df_ind_full = df_map.merge(
        df_ind,
        on="pick_id",
        how="left",
        validate="many_to_one",
        suffixes=("", "_ind"),
    )

    # Bring waveform starttime for reconstruction
    df_ind_full = df_ind_full.merge(
        df_wav[["event_id", "starttime"]],
        left_on="waveform_event_id",
        right_on="event_id",
        how="left",
        suffixes=("", "_wav"),
    )

    # Reconstruct absolute pick time from waveform start + offset
    df_ind_full["pick_time_reconstructed"] = (
        df_ind_full["starttime"] +
        pd.to_timedelta(df_ind_full["time_offset_from_start"], unit="s")
    )

    # Build seed_norm for ind_full (prefer existing from df_ind; otherwise normalize)
    if "seed_norm" not in df_ind_full.columns:
        df_ind_full["seed_norm"] = normalize_seed_id(df_ind_full)

    # Drop ind rows that couldn't reconstruct (missing waveform starttime etc.)
    ind_recon_bad = df_ind_full["pick_time_reconstructed"].isna()
    if ind_recon_bad.any():
        print(f"WARNING: Dropping {int(ind_recon_bad.sum())} individual pick mappings with bad reconstruction")
        df_ind_full = df_ind_full.loc[~ind_recon_bad].reset_index(drop=True)

    # Group for per-waveform lookup
    ind_by_waveform = df_ind_full.groupby("waveform_event_id") if not df_ind_full.empty else None

    # ------------------------------------------------------------------
    # Build waveform-centered events
    # ------------------------------------------------------------------
    events: list[dict] = []
    pick_map: list[dict] = []
    qc_rows: list[dict] = []

    for _, w in df_wav.iterrows():
        wav_id = str(w["event_id"])
        start = w["starttime"]
        end = w["endtime"]
        ev_id = f"wav_{wav_id}"

        # authoritative picks for this waveform
        if ind_by_waveform is not None and wav_id in ind_by_waveform.groups:
            ind_picks = ind_by_waveform.get_group(wav_id).copy()
        else:
            ind_picks = pd.DataFrame()

        # monthly picks inside waveform window (± tol)
        mon_in = df_mon[
            (df_mon["pick_time"] >= (start - tol)) &
            (df_mon["pick_time"] <= (end + tol))
        ].copy()

        # Deduplicate monthly vs individual using tolerance on (seed_norm, phase)
        # Note: monthly picks may lack phase/seed_norm; we can only dedupe when those exist.
        monthly_missing_keys = 0
        monthly_suppressed = 0

        if not mon_in.empty:
            # Ensure required columns exist for dedupe
            for col in ("phase", "seed_norm"):
                if col not in mon_in.columns:
                    mon_in[col] = pd.NA
            if not ind_picks.empty:
                for col in ("phase", "seed_norm"):
                    if col not in ind_picks.columns:
                        ind_picks[col] = pd.NA

            # Partition monthly picks into those that can be deduped vs those that cannot
            can_key = mon_in["seed_norm"].notna() & mon_in["phase"].notna()
            mon_keyed = mon_in.loc[can_key].copy()
            mon_unkeyed = mon_in.loc[~can_key].copy()
            monthly_missing_keys = int((~can_key).sum())

            if not ind_picks.empty and not mon_keyed.empty:
                ind_keyed = ind_picks.copy()
                ind_keyed = ind_keyed[ind_keyed["seed_norm"].notna() & ind_keyed["phase"].notna()].copy()

                if ind_keyed.empty:
                    mon_final = pd.concat([mon_keyed, mon_unkeyed], ignore_index=True)
                else:
                    ind_block = (
                        ind_keyed
                        .rename(columns={"pick_time_reconstructed": "pick_time"})
                        [["seed_norm", "phase", "pick_time"]]
                        .sort_values(["seed_norm", "phase", "pick_time"])
                        .reset_index(drop=True)
                    )
                    mon_block = (
                        mon_keyed
                        .sort_values(["seed_norm", "phase", "pick_time"])
                        .reset_index(drop=True)
                    )

                    # merge_asof needs global sort on the 'on' key within each 'by' group
                    # Doing it per group avoids the classic merge_asof sorting pitfalls.
                    keep_parts = []
                    suppressed_parts = []

                    for (seed_norm, phase), mon_grp in mon_block.groupby(["seed_norm", "phase"], sort=False):
                        ind_grp = ind_block[(ind_block["seed_norm"] == seed_norm) & (ind_block["phase"] == phase)]
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
                        suppressed_parts.append(mon_grp.loc[matched.values])
                        keep_parts.append(mon_grp.loc[~matched.values])

                    mon_supp = pd.concat(suppressed_parts, ignore_index=True) if suppressed_parts else mon_block.iloc[0:0].copy()
                    mon_keep = pd.concat(keep_parts, ignore_index=True) if keep_parts else mon_block.iloc[0:0].copy()

                    monthly_suppressed = int(len(mon_supp))
                    mon_final = pd.concat([mon_keep, mon_unkeyed], ignore_index=True)
            else:
                mon_final = mon_in
        else:
            mon_final = mon_in

        # Record event row
        events.append({
            "event_id": ev_id,
            "waveform_event_id": wav_id,
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
            "waveform_event_id": wav_id,
            "event_id": ev_id,
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "n_individual_picks": int(len(ind_picks)),
            "n_monthly_in_window": int(len(mon_in)),
            "n_monthly_suppressed_vs_individual": int(monthly_suppressed),
            "n_monthly_missing_keys": int(monthly_missing_keys),
            "n_monthly_kept": int(len(mon_final)),
        })

        # Pick mappings (store offsets relative to waveform start)
        if not ind_picks.empty:
            for _, p in ind_picks.iterrows():
                pick_map.append({
                    "event_id": ev_id,
                    "waveform_event_id": wav_id,
                    "pick_id": p.get("pick_id"),
                    "pick_source": "individual",
                    "seed_norm": p.get("seed_norm"),
                    "phase": p.get("phase"),
                    "pick_time": pd.Timestamp(p["pick_time_reconstructed"]).isoformat(),
                    "time_offset": float((p["pick_time_reconstructed"] - start).total_seconds()),
                })

        if not mon_final.empty:
            for _, p in mon_final.iterrows():
                pick_map.append({
                    "event_id": ev_id,
                    "waveform_event_id": wav_id,
                    "pick_id": p.get("pick_id"),
                    "pick_source": "monthly",
                    "seed_norm": p.get("seed_norm"),
                    "phase": p.get("phase"),
                    "pick_time": pd.Timestamp(p["pick_time"]).isoformat() if pd.notna(p.get("pick_time")) else None,
                    "time_offset": float((p["pick_time"] - start).total_seconds()) if pd.notna(p.get("pick_time")) else None,
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