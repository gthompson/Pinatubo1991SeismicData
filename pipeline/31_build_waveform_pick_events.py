#!/usr/bin/env python3
"""
05c_build_waveform_events.py

STEP 05c of the Pinatubo FAIR pipeline.

Build the authoritative waveform-centered event catalog.

Inputs:
- Waveform index (STEP 01)
- Individual pick index (STEP 02)
- Individual pick ↔ waveform map (STEP 02b)
- Monthly pick index (STEP 03)

Rules:
- Each waveform file defines ONE event
- Individual picks are authoritative
- Monthly picks are added only if:
    * they fall inside waveform window
    * they do not duplicate authoritative picks
"""

from pathlib import Path
import argparse
import pandas as pd


def to_dt(s):
    return pd.to_datetime(s, format="mixed", utc=True, errors="coerce")


def main():
    ap = argparse.ArgumentParser(
        description="STEP 05c: Build waveform-centered event catalog"
    )
    ap.add_argument("--waveform-index", required=True)
    ap.add_argument("--individual-pick-index", required=True)
    ap.add_argument("--individual-pick-waveform-map", required=True)
    ap.add_argument("--monthly-picks", required=True)
    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-pick-map-csv", required=True)
    ap.add_argument("--out-qc-csv", required=True)
    ap.add_argument("--time-tolerance", type=float, default=0.5)

    args = ap.parse_args()

    print("=== STEP 05c: Building waveform-centered event catalog ===")

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_wav = pd.read_csv(args.waveform_index)
    df_ind = pd.read_csv(args.individual_pick_index)
    df_map = pd.read_csv(args.individual_pick_waveform_map)
    df_mon = pd.read_csv(args.monthly_picks)

    df_wav["starttime"] = to_dt(df_wav["starttime"])
    df_wav["endtime"] = to_dt(df_wav["endtime"])

    df_ind["pick_time"] = to_dt(df_ind["pick_time"])
    df_mon["pick_time"] = to_dt(df_mon["pick_time"])

    tol = pd.Timedelta(seconds=args.time_tolerance)

    # ------------------------------------------------------------------
    # Prepare authoritative individual picks with waveform association
    # ------------------------------------------------------------------

    # Join STEP 02 + STEP 02b
    df_ind_full = df_map.merge(
        df_ind,
        on="pick_id",
        how="left",
        validate="many_to_one"
    )

    # Reconstruct absolute pick_time from waveform start + offset
    df_ind_full = df_ind_full.merge(
        df_wav[["event_id", "starttime"]],
        left_on="waveform_event_id",
        right_on="event_id",
        how="left",
        suffixes=("", "_wav"),
    )

    df_ind_full["pick_time_reconstructed"] = (
        df_ind_full["starttime"] +
        pd.to_timedelta(df_ind_full["time_offset_from_start"], unit="s")
    )

    # ------------------------------------------------------------------
    # Index for fast lookup
    # ------------------------------------------------------------------

    ind_by_waveform = df_ind_full.groupby("waveform_event_id")

    events = []
    pick_map = []
    qc_rows = []

    # ------------------------------------------------------------------
    # Build waveform-centered events
    # ------------------------------------------------------------------

    for _, w in df_wav.iterrows():
        wav_id = str(w["event_id"])
        start = w["starttime"]
        end = w["endtime"]
        ev_id = f"wav_{wav_id}"

        # --- authoritative picks ---
        ind_picks = (
            ind_by_waveform.get_group(wav_id)
            if wav_id in ind_by_waveform.groups
            else pd.DataFrame()
        )

        # --- monthly picks inside window ---
        mon_in = df_mon[
            (df_mon["pick_time"] >= start - tol) &
            (df_mon["pick_time"] <= end + tol)
        ]

        # --- deduplicate monthly vs individual ---
        if not ind_picks.empty:
            key_cols = ["station", "phase"]
            merged = mon_in.merge(
                ind_picks[key_cols + ["pick_time_reconstructed"]],
                left_on=key_cols + ["pick_time"],
                right_on=key_cols + ["pick_time_reconstructed"],
                how="left",
                indicator=True,
            )
            mon_picks = merged[merged["_merge"] == "left_only"].drop(columns="_merge")
        else:
            mon_picks = mon_in

        # --- record event ---
        events.append({
            "event_id": ev_id,
            "waveform_event_id": wav_id,
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "wav_file": w.get("wav_file"),
            "n_individual_picks": len(ind_picks),
            "n_monthly_picks": len(mon_picks),
        })

        # --- pick mappings ---
        for _, p in ind_picks.iterrows():
            pick_map.append({
                "event_id": ev_id,
                "pick_id": p["pick_id"],
                "pick_source": "individual",
                "time_offset": (p["pick_time_reconstructed"] - start).total_seconds(),
            })

        for _, p in mon_picks.iterrows():
            pick_map.append({
                "event_id": ev_id,
                "pick_id": p.get("pick_id"),
                "pick_source": "monthly",
                "time_offset": (p["pick_time"] - start).total_seconds(),
            })

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out_event = Path(args.out_event_csv)
    out_pick = Path(args.out_pick_map_csv)
    out_qc = Path(args.out_qc_csv)

    out_event.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(events).to_csv(out_event, index=False)
    pd.DataFrame(pick_map).to_csv(out_pick, index=False)
    pd.DataFrame(qc_rows).to_csv(out_qc, index=False)

    print("\nSTEP 05c COMPLETE")
    print("-----------------")
    print(f"Waveform events:      {len(events)}")
    print(f"Total pick mappings:  {len(pick_map)}")
    print(f"Event CSV:            {out_event}")
    print(f"Pick map CSV:         {out_pick}")
    print(f"QC CSV:               {out_qc}")


if __name__ == "__main__":
    main()