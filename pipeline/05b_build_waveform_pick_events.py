#!/usr/bin/env python3
"""
05b_build_waveform_pick_events.py

Associate merged phase-pick groups with waveform-triggered events.

This step creates the authoritative waveform–pick event catalog.

Key rules / assumptions:
- Exactly ONE waveform file per event
- Individual PHA picks are authoritative and should match waveform by event_id
- ALL picks in a group must fall inside the waveform time window (within tolerance)
- Pick-only events are preserved if waveform is missing (i.e., missing waveform file)
"""

from __future__ import annotations

from pathlib import Path
import argparse
import pandas as pd


def _to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="mixed", utc=True, errors="coerce")


def _pick_group_key(df_picks: pd.DataFrame) -> str:
    # Prefer the merged grouping from step 04 if present.
    if "merged_pick_group_id" in df_picks.columns:
        return "merged_pick_group_id"
    if "pick_group_id" in df_picks.columns:
        return "pick_group_id"
    if "monthly_block_id" in df_picks.columns:
        return "monthly_block_id"
    raise SystemExit("No pick grouping column found (expected merged_pick_group_id/pick_group_id/monthly_block_id)")


def _event_hint_for_group(g: pd.DataFrame) -> str | None:
    """
    Try to recover the authoritative waveform event id.
    Priority:
      1) merged_event_hint (from step 04 for authoritative picks)
      2) event_id (from step 02 authoritative picks)
    """
    for col in ("merged_event_hint", "event_id"):
        if col in g.columns:
            vals = g[col].dropna().astype(str).unique().tolist()
            if len(vals) == 1:
                return vals[0]
            if len(vals) > 1:
                # Should not happen for authoritative picks; keep None and let fallback handle
                return None
    return None


def _group_type(g: pd.DataFrame) -> str:
    # Best-effort labelling for diagnostics
    if "merged_pick_group_type" in g.columns:
        vals = g["merged_pick_group_type"].dropna().astype(str).unique().tolist()
        if len(vals) == 1:
            return vals[0]
    if "pick_group_type" in g.columns:
        vals = g["pick_group_type"].dropna().astype(str).unique().tolist()
        if len(vals) == 1:
            return vals[0]
    if "event_source" in g.columns:
        vals = g["event_source"].dropna().astype(str).unique().tolist()
        if len(vals) == 1:
            return vals[0]
    return "unknown"


def main():
    ap = argparse.ArgumentParser(description="STEP 05b: Build waveform–pick event catalog")
    ap.add_argument("--waveform-index", required=True)
    ap.add_argument("--pick-index", required=True)
    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-pick-map-csv", required=True)
    ap.add_argument("--out-unmatched-picks", default=None)
    ap.add_argument("--out-unmatched-waveforms", default=None)

    # small leeway for edge cases (e.g., rounding to nearest sample / serialization quirks)
    ap.add_argument("--window-tol-seconds", type=float, default=0.5)

    args = ap.parse_args()

    print("=== STEP 05b: Building waveform↔pick-event association ===")

    df_wav = pd.read_csv(args.waveform_index)
    df_picks = pd.read_csv(args.pick_index)

    # Parse times robustly
    df_wav["starttime"] = _to_dt(df_wav["starttime"])
    df_wav["endtime"] = _to_dt(df_wav["endtime"])
    df_picks["pick_time"] = _to_dt(df_picks["pick_time"])

    # Basic validation
    if df_wav["starttime"].isna().any() or df_wav["endtime"].isna().any():
        bad = df_wav[df_wav["starttime"].isna() | df_wav["endtime"].isna()]
        raise SystemExit(
            f"Waveform index has unparseable start/end times for {len(bad)} rows. "
            f"Fix upstream or inspect {args.waveform_index}."
        )
    if df_picks["pick_time"].isna().any():
        bad = df_picks[df_picks["pick_time"].isna()]
        raise SystemExit(
            f"Pick index has unparseable pick_time for {len(bad)} rows. "
            f"Fix upstream or inspect {args.pick_index}."
        )

    # Ensure waveform event_id is string for joining
    if "event_id" not in df_wav.columns:
        raise SystemExit("Waveform index must have column 'event_id'.")
    df_wav["event_id"] = df_wav["event_id"].astype(str)

    # Ensure pick_id exists
    if "pick_id" not in df_picks.columns:
        df_picks["pick_id"] = [f"pick_{i}" for i in range(len(df_picks))]

    group_col = _pick_group_key(df_picks)
    pick_groups = df_picks.groupby(group_col, sort=False)

    tol = pd.Timedelta(seconds=float(args.window_tol_seconds))

    # Quick lookup table by waveform event_id
    wav_by_id = df_wav.set_index("event_id", drop=False)

    events = []
    pick_map = []

    unmatched_picks_rows = []
    unmatched_waveforms_rows = []

    used_waveform_ids: set[str] = set()
    event_counter = 0

    # Diagnostics counters
    n_groups = 0
    matched_by_id = 0
    matched_by_time = 0
    pick_only_groups = 0
    id_match_but_outside = 0

    for group_id, g in pick_groups:
        n_groups += 1
        event_counter += 1
        ev_id = f"wp_{event_counter:06d}"

        g = g.copy()
        gtype = _group_type(g)

        tmin = g["pick_time"].min()
        tmax = g["pick_time"].max()

        # 1) Try ID-based match for authoritative picks
        hint = _event_hint_for_group(g)
        w = None
        association_method = None

        if hint is not None and hint in wav_by_id.index:
            w = wav_by_id.loc[hint]
            association_method = "event_id"
            matched_by_id += 1

            # sanity check: all picks inside waveform window
            start = w["starttime"]
            end = w["endtime"]

            outside = g[(g["pick_time"] < (start - tol)) | (g["pick_time"] > (end + tol))]
            if len(outside) > 0:
                # This is the case you said "should not happen" unless waveform is wrong/mismatched or corrupted picks
                id_match_but_outside += 1

                # Record these as unmatched picks for debugging, but STILL build the event
                # so you can inspect what’s going on downstream.
                for _, prow in outside.iterrows():
                    r = prow.to_dict()
                    r.update({
                        "why_unmatched": "id_match_but_pick_outside_window",
                        "waveform_event_id": str(w["event_id"]),
                        "waveform_starttime": start.isoformat(),
                        "waveform_endtime": end.isoformat(),
                        "group_id": group_id,
                        "group_type": gtype,
                    })
                    unmatched_picks_rows.append(r)

        # 2) If no ID match, fall back to time enclosure
        if w is None:
            matches = df_wav[
                (df_wav["starttime"] <= (tmin + tol)) &
                (df_wav["endtime"] >= (tmax - tol))
            ]

            if len(matches) == 1:
                w = matches.iloc[0]
                association_method = "waveform_enclosure"
                matched_by_time += 1
            elif len(matches) > 1:
                # Shouldn't happen if waveform files are truly 1-per-event and non-overlapping.
                # Choose the shortest enclosing window as safest fallback, but also surface it.
                matches = matches.copy()
                matches["dur"] = (matches["endtime"] - matches["starttime"]).dt.total_seconds()
                w = matches.sort_values("dur").iloc[0]
                association_method = "waveform_enclosure_multiple_choose_shortest"
            else:
                # Pick-only event (waveform missing or waveform index incomplete)
                pick_only_groups += 1
                events.append({
                    "event_id": ev_id,
                    "event_type": "pick_only",
                    "origin_time_estimate": tmin.isoformat(),
                    "waveform_event_id": None,
                    "pick_group_id": group_id,
                    "pick_group_type": gtype,
                    "starttime": tmin.isoformat(),
                    "endtime": tmax.isoformat(),
                    "wav_file": None,
                    "n_picks": len(g),
                    "association_method": "none",
                    "event_hint": hint,
                })
                for _, p in g.iterrows():
                    pick_map.append({
                        "pick_id": p["pick_id"],
                        "event_id": ev_id,
                        "association_method": "pick_only",
                        "time_offset_from_start": (p["pick_time"] - tmin).total_seconds(),
                    })
                    r = p.to_dict()
                    r.update({
                        "why_unmatched": "no_enclosing_waveform",
                        "group_id": group_id,
                        "group_type": gtype,
                        "event_hint": hint,
                    })
                    unmatched_picks_rows.append(r)
                continue  # done with this group

        # If we got here, we have a waveform row w
        used_waveform_ids.add(str(w["event_id"]))

        events.append({
            "event_id": ev_id,
            "event_type": "waveform",
            "origin_time_estimate": w["starttime"].isoformat(),
            "waveform_event_id": str(w["event_id"]),
            "pick_group_id": group_id,
            "pick_group_type": gtype,
            "starttime": w["starttime"].isoformat(),
            "endtime": w["endtime"].isoformat(),
            "wav_file": w.get("wav_file"),
            "n_picks": len(g),
            "association_method": association_method,
            "event_hint": hint,
        })

        for _, p in g.iterrows():
            pick_map.append({
                "pick_id": p["pick_id"],
                "event_id": ev_id,
                "association_method": association_method,
                "time_offset_from_start": (p["pick_time"] - w["starttime"]).total_seconds(),
            })

    # Waveform-only events: anything never used
    for _, w in df_wav.iterrows():
        wid = str(w["event_id"])
        if wid in used_waveform_ids:
            continue

        event_counter += 1
        ev_id = f"wp_{event_counter:06d}"
        events.append({
            "event_id": ev_id,
            "event_type": "waveform_only",
            "origin_time_estimate": w["starttime"].isoformat(),
            "waveform_event_id": wid,
            "pick_group_id": None,
            "pick_group_type": None,
            "starttime": w["starttime"].isoformat(),
            "endtime": w["endtime"].isoformat(),
            "wav_file": w.get("wav_file"),
            "n_picks": 0,
            "association_method": "none",
            "event_hint": None,
        })
        r = w.to_dict()
        r.update({"why_unmatched": "no_picks_associated"})
        unmatched_waveforms_rows.append(r)

    # Write outputs
    out_event = Path(args.out_event_csv)
    out_pick = Path(args.out_pick_map_csv)
    out_event.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(events).to_csv(out_event, index=False)
    pd.DataFrame(pick_map).to_csv(out_pick, index=False)

    if args.out_unmatched_picks:
        Path(args.out_unmatched_picks).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(unmatched_picks_rows).to_csv(args.out_unmatched_picks, index=False)

    if args.out_unmatched_waveforms:
        Path(args.out_unmatched_waveforms).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(unmatched_waveforms_rows).to_csv(args.out_unmatched_waveforms, index=False)

    print("\nSTEP 05b COMPLETE")
    print("-----------------")
    print(f"Pick groups processed: {n_groups}")
    print(f"Matched by event_id:   {matched_by_id}")
    print(f"Matched by time:       {matched_by_time}")
    print(f"Pick-only groups:      {pick_only_groups}")
    print(f"Waveform-only events:  {sum(e['event_type']=='waveform_only' for e in events)}")
    print("")
    print("Sanity checks:")
    print(f"  ID match but picks outside window: {id_match_but_outside}")
    print("")
    print(f"Events created:        {len(events)}")
    print(f"Pick mappings:         {len(pick_map)}")
    print(f"Event CSV:             {out_event}")
    print(f"Pick map CSV:          {out_pick}")
    if args.out_unmatched_picks:
        print(f"Unmatched picks CSV:   {args.out_unmatched_picks}")
    if args.out_unmatched_waveforms:
        print(f"Unmatched wav CSV:     {args.out_unmatched_waveforms}")


if __name__ == "__main__":
    main()