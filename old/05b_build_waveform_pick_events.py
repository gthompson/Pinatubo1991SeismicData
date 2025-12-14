#!/usr/bin/env python3
"""
05b_build_waveform_pick_events.py

STEP 05b of the Pinatubo FAIR pipeline.

Associate merged phase-pick groups with waveform event files.

AUTHORITATIVE LOGIC
-------------------
• Individual PHA picks were made directly on waveform files
• Therefore:
    - Individual pick groups MUST associate by waveform event_id
    - Time enclosure is only a sanity check
• Monthly picks associate by time enclosure only
• Exactly ONE waveform file per event
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="mixed", utc=True, errors="coerce")


def pick_group_column(df: pd.DataFrame) -> str:
    if "merged_pick_group_id" in df.columns:
        return "merged_pick_group_id"
    if "pick_group_id" in df.columns:
        return "pick_group_id"
    raise SystemExit("No pick-group column found")


def group_has_individual_picks(g: pd.DataFrame) -> bool:
    return (
        "merged_pick_group_type" in g.columns
        and (g["merged_pick_group_type"] == "authoritative").any()
    )


def event_hint(g: pd.DataFrame) -> str | None:
    for col in ("merged_event_hint", "event_id"):
        if col in g.columns:
            vals = g[col].dropna().astype(str).unique()
            if len(vals) == 1:
                return vals[0]
    return None


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="STEP 05b: Associate waveform files with pick events"
    )
    ap.add_argument("--waveform-index", required=True)
    ap.add_argument("--pick-index", required=True)
    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-pick-map-csv", required=True)
    ap.add_argument("--out-unmatched-picks", default=None)
    ap.add_argument("--out-unmatched-waveforms", default=None)
    ap.add_argument("--window-tol-seconds", type=float, default=0.5)

    args = ap.parse_args()

    print("=== STEP 05b: Building waveform ↔ pick-event association ===")

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_wav = pd.read_csv(args.waveform_index)
    df_picks = pd.read_csv(args.pick_index, low_memory=False)

    df_wav["starttime"] = to_dt(df_wav["starttime"])
    df_wav["endtime"] = to_dt(df_wav["endtime"])
    df_picks["pick_time"] = to_dt(df_picks["pick_time"])

    if df_wav["starttime"].isna().any() or df_wav["endtime"].isna().any():
        raise SystemExit("Waveform index contains unparseable times")

    if df_picks["pick_time"].isna().any():
        raise SystemExit("Pick index contains unparseable pick_time")

    df_wav["event_id"] = df_wav["event_id"].astype(str)

    if "pick_id" not in df_picks.columns:
        df_picks["pick_id"] = [f"pick_{i}" for i in range(len(df_picks))]

    group_col = pick_group_column(df_picks)
    groups = df_picks.groupby(group_col, sort=False)

    wav_by_id = df_wav.set_index("event_id", drop=False)
    tol = pd.Timedelta(seconds=args.window_tol_seconds)

    # ------------------------------------------------------------------
    # Outputs
    # ------------------------------------------------------------------

    events = []
    pick_map = []
    unmatched_picks = []
    unmatched_waveforms = []

    used_waveforms = set()
    event_counter = 0

    # Diagnostics
    n_groups = 0
    n_id_match = 0
    n_time_match = 0
    n_pick_only = 0
    n_id_outside = 0

    # ------------------------------------------------------------------
    # Associate pick groups → waveform
    # ------------------------------------------------------------------

    for group_id, g in groups:
        n_groups += 1
        event_counter += 1
        wp_event_id = f"wp_{event_counter:06d}"

        g = g.copy()
        tmin = g["pick_time"].min()
        tmax = g["pick_time"].max()

        is_individual = group_has_individual_picks(g)
        hint = event_hint(g)

        w = None
        assoc_method = None

        # --------------------------------------------------------------
        # 1) Individual picks → MUST match by event_id
        # --------------------------------------------------------------

        if is_individual:
            if hint is None or hint not in wav_by_id.index:
                n_pick_only += 1
                assoc_method = "individual_missing_waveform"
            else:
                w = wav_by_id.loc[hint]
                assoc_method = "event_id"
                n_id_match += 1

                outside = g[
                    (g["pick_time"] < w["starttime"] - tol) |
                    (g["pick_time"] > w["endtime"] + tol)
                ]
                if len(outside) > 0:
                    n_id_outside += 1
                    for _, p in outside.iterrows():
                        r = p.to_dict()
                        r["why_unmatched"] = "id_match_but_pick_outside_window"
                        r["waveform_event_id"] = w["event_id"]
                        unmatched_picks.append(r)

        # --------------------------------------------------------------
        # 2) Monthly-only picks → time enclosure
        # --------------------------------------------------------------

        if w is None and not is_individual:
            matches = df_wav[
                (df_wav["starttime"] <= tmin + tol) &
                (df_wav["endtime"] >= tmax - tol)
            ]

            if len(matches) == 1:
                w = matches.iloc[0]
                assoc_method = "waveform_enclosure"
                n_time_match += 1
            elif len(matches) > 1:
                matches = matches.copy()
                matches["dur"] = (matches["endtime"] - matches["starttime"]).dt.total_seconds()
                w = matches.sort_values("dur").iloc[0]
                assoc_method = "waveform_enclosure_multiple"
                n_time_match += 1
            else:
                n_pick_only += 1
                assoc_method = "no_enclosing_waveform"

        # --------------------------------------------------------------
        # Build outputs
        # --------------------------------------------------------------

        if w is None:
            events.append({
                "event_id": wp_event_id,
                "event_type": "pick_only",
                "origin_time_estimate": tmin.isoformat(),
                "waveform_event_id": None,
                "pick_group_id": group_id,
                "starttime": tmin.isoformat(),
                "endtime": tmax.isoformat(),
                "wav_file": None,
                "n_picks": len(g),
                "association_method": assoc_method,
                "event_hint": hint,
            })

            for _, p in g.iterrows():
                pick_map.append({
                    "pick_id": p["pick_id"],
                    "event_id": wp_event_id,
                    "association_method": "pick_only",
                    "time_offset_from_start": (p["pick_time"] - tmin).total_seconds(),
                })
                r = p.to_dict()
                r["why_unmatched"] = assoc_method
                unmatched_picks.append(r)

            continue

        # --------------------------------------------------------------
        # Waveform-associated event
        # --------------------------------------------------------------

        used_waveforms.add(w["event_id"])

        events.append({
            "event_id": wp_event_id,
            "event_type": "waveform",
            "origin_time_estimate": w["starttime"].isoformat(),
            "waveform_event_id": w["event_id"],
            "pick_group_id": group_id,
            "starttime": w["starttime"].isoformat(),
            "endtime": w["endtime"].isoformat(),
            "wav_file": w.get("wav_file"),
            "n_picks": len(g),
            "association_method": assoc_method,
            "event_hint": hint,
        })

        for _, p in g.iterrows():
            pick_map.append({
                "pick_id": p["pick_id"],
                "event_id": wp_event_id,
                "association_method": assoc_method,
                "time_offset_from_start": (p["pick_time"] - w["starttime"]).total_seconds(),
            })

    # ------------------------------------------------------------------
    # Waveform-only events
    # ------------------------------------------------------------------

    for _, w in df_wav.iterrows():
        if w["event_id"] in used_waveforms:
            continue

        event_counter += 1
        wp_event_id = f"wp_{event_counter:06d}"

        events.append({
            "event_id": wp_event_id,
            "event_type": "waveform_only",
            "origin_time_estimate": w["starttime"].isoformat(),
            "waveform_event_id": w["event_id"],
            "pick_group_id": None,
            "starttime": w["starttime"].isoformat(),
            "endtime": w["endtime"].isoformat(),
            "wav_file": w.get("wav_file"),
            "n_picks": 0,
            "association_method": "no_picks",
            "event_hint": None,
        })

        r = w.to_dict()
        r["why_unmatched"] = "no_picks_associated"
        unmatched_waveforms.append(r)

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out_event = Path(args.out_event_csv)
    out_pick = Path(args.out_pick_map_csv)
    out_event.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(events).to_csv(out_event, index=False)
    pd.DataFrame(pick_map).to_csv(out_pick, index=False)

    if args.out_unmatched_picks:
        pd.DataFrame(unmatched_picks).to_csv(args.out_unmatched_picks, index=False)

    if args.out_unmatched_waveforms:
        pd.DataFrame(unmatched_waveforms).to_csv(args.out_unmatched_waveforms, index=False)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    print("\nSTEP 05b COMPLETE")
    print("-----------------")
    print(f"Pick groups processed:           {n_groups}")
    print(f"Matched by event_id:             {n_id_match}")
    print(f"Matched by time enclosure:       {n_time_match}")
    print(f"Pick-only groups:                {n_pick_only}")
    print(f"ID match but picks outside win:  {n_id_outside}")
    print(f"Waveform-only events:            {len(unmatched_waveforms)}")
    print(f"Events created:                  {len(events)}")
    print(f"Pick mappings:                   {len(pick_map)}")
    print(f"Event CSV:                       {out_event}")
    print(f"Pick map CSV:                    {out_pick}")


if __name__ == "__main__":
    main()