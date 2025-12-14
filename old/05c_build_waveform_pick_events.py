#!/usr/bin/env python3
"""
05c_build_waveform_events.py

STEP 05c of the Pinatubo FAIR pipeline.

Build authoritative waveform-based events and attach:
  • individual (authoritative) picks from STEP 02b
  • monthly (secondary) picks from STEP 03

Waveforms are the primary event objects.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def to_dt(s):
    return pd.to_datetime(s, format="mixed", utc=True, errors="coerce")


def main():
    ap = argparse.ArgumentParser(
        description="STEP 05c: Build waveform-based event catalog"
    )
    ap.add_argument("--waveform-index", required=True)
    ap.add_argument("--individual-pick-map", required=True)
    ap.add_argument("--monthly-picks", required=True)

    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-pick-map-csv", required=True)
    ap.add_argument("--out-unmatched-monthly", required=True)
    ap.add_argument("--out-waveform-qc", required=True)

    ap.add_argument("--time-tolerance", type=float, default=0.5)

    args = ap.parse_args()

    tol = pd.Timedelta(seconds=args.time_tolerance)

    print("=== STEP 05c: Building waveform-based events ===")

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_wav = pd.read_csv(args.waveform_index)
    df_indiv = pd.read_csv(args.individual_pick_map)
    df_month = pd.read_csv(args.monthly_picks)

    # Parse times
    df_wav["starttime"] = to_dt(df_wav["starttime"])
    df_wav["endtime"] = to_dt(df_wav["endtime"])

    df_indiv["pick_time"] = to_dt(df_indiv["pick_time"])
    df_month["pick_time"] = to_dt(df_month["pick_time"])

    # ------------------------------------------------------------------
    # Basic validation
    # ------------------------------------------------------------------

    required_wav_cols = {"event_id", "wav_file", "starttime", "endtime"}
    if not required_wav_cols.issubset(df_wav.columns):
        raise SystemExit("Waveform index missing required columns")

    if "waveform_event_id" not in df_indiv.columns:
        raise SystemExit("02b output must include waveform_event_id")

    # ------------------------------------------------------------------
    # Prepare outputs
    # ------------------------------------------------------------------

    events = []
    pick_map = []
    unmatched_monthly = []
    waveform_qc = []

    # Index waveform rows by event_id
    wav_by_id = df_wav.set_index("event_id", drop=False)

    # ------------------------------------------------------------------
    # Build events waveform-by-waveform
    # ------------------------------------------------------------------

    for wid, w in wav_by_id.iterrows():
        wstart = w["starttime"]
        wend = w["endtime"]

        # Individual picks already mapped (authoritative)
        indiv = df_indiv[df_indiv["waveform_event_id"].astype(str) == str(wid)]

        # Monthly picks that fall inside waveform window
        month = df_month[
            (df_month["pick_time"] >= (wstart - tol)) &
            (df_month["pick_time"] <= (wend + tol))
        ]

        # Build event row
        events.append({
            "event_id": f"wav_{wid}",
            "event_type": "waveform",
            "waveform_event_id": wid,
            "wav_file": w["wav_file"],
            "starttime": wstart.isoformat(),
            "endtime": wend.isoformat(),
            "n_individual_picks": len(indiv),
            "n_monthly_picks": len(month),
        })

        # Map picks
        for _, p in indiv.iterrows():
            pick_map.append({
                "pick_id": p["pick_id"],
                "event_id": f"wav_{wid}",
                "pick_source": "individual",
                "association_method": "02b_direct",
                "time_offset_from_start":
                    (p["pick_time"] - wstart).total_seconds(),
            })

        for _, p in month.iterrows():
            pick_map.append({
                "pick_id": p.get("pick_id"),
                "event_id": f"wav_{wid}",
                "pick_source": "monthly",
                "association_method": "window_enclosure",
                "time_offset_from_start":
                    (p["pick_time"] - wstart).total_seconds(),
            })

        waveform_qc.append({
            "waveform_event_id": wid,
            "wav_file": w["wav_file"],
            "individual_picks": len(indiv),
            "monthly_picks": len(month),
            "has_any_picks": len(indiv) + len(month) > 0,
        })

    # ------------------------------------------------------------------
    # Monthly picks that never matched any waveform
    # ------------------------------------------------------------------

    used_monthly_ids = set(pm["pick_id"] for pm in pick_map if pm["pick_source"] == "monthly")

    for _, p in df_month.iterrows():
        pid = p.get("pick_id")
        if pid not in used_monthly_ids:
            r = p.to_dict()
            r["reason"] = "no_enclosing_waveform"
            unmatched_monthly.append(r)

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out_event = Path(args.out_event_csv)
    out_pick = Path(args.out_pick_map_csv)
    out_event.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(events).to_csv(out_event, index=False)
    pd.DataFrame(pick_map).to_csv(out_pick, index=False)
    pd.DataFrame(unmatched_monthly).to_csv(args.out_unmatched_monthly, index=False)
    pd.DataFrame(waveform_qc).to_csv(args.out_waveform_qc, index=False)

    print("\nSTEP 05c COMPLETE")
    print("-----------------")
    print(f"Waveform events:        {len(events)}")
    print(f"Total pick mappings:    {len(pick_map)}")
    print(f"Unmatched monthly picks:{len(unmatched_monthly)}")
    print(f"Event CSV:              {out_event}")
    print(f"Pick map CSV:           {out_pick}")


if __name__ == "__main__":
    main()