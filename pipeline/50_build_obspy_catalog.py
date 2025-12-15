#!/usr/bin/env python3
"""
50_build_obspy_catalog.py

STEP 50 — Build ObsPy QuakeML Catalog

Authoritative event spine:
  • Step 32 waveform-centered event catalog

Hypocenters:
  • Associated from Step 43 by nearest-time match
  • One-to-one, within --origin-time-tol
  • Unmatched hypocenters become hypocenter-only events

Outputs:
  • ObsPy Catalog written as QuakeML
"""

from __future__ import annotations

from pathlib import Path
import argparse
import pandas as pd

from obspy import UTCDateTime
from obspy.core.event import (
    Catalog, Event, Origin, Pick,
    ResourceIdentifier, WaveformStreamID, Comment
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def parse_seed_id(seed_id: str):
    if not isinstance(seed_id, str) or not seed_id.strip():
        return (None, None, None, None)
    parts = seed_id.split(".")
    if len(parts) != 4:
        return (None, None, None, None)
    return tuple(parts)

def safe_series_get(row, key, default=None):
    v = row.get(key, default)
    return default if pd.isna(v) else v

def build_origin(row):
    return Origin(
        time=UTCDateTime(row["origin_time"]),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        depth=float(row["depth_km"]) * 1000.0,
        resource_id=ResourceIdentifier(f"origin/{row['origin_id']}"),
    )

def build_pick(row, default_net="XB"):
    seed_id = safe_series_get(row, "seed_id")
    net, sta, loc, chan = parse_seed_id(seed_id)

    wid = WaveformStreamID(
        network_code=net or default_net,
        station_code=sta,
        location_code=loc or "",
        channel_code=chan,
    )

    pid = safe_series_get(row, "pick_id")
    if pid is None:
        return None

    return Pick(
        time=UTCDateTime(row["pick_time"]),
        phase_hint=safe_series_get(row, "phase"),
        waveform_id=wid,
        resource_id=ResourceIdentifier(f"pick/{pid}"),
    )

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 50: Build ObsPy Catalog")
    ap.add_argument("--waveform-event-index", required=True)
    ap.add_argument("--waveform-pick-map", required=True)
    ap.add_argument("--pick-index", required=True)
    ap.add_argument("--hypo-event-index", required=True)
    ap.add_argument("--hypo-origin-index", required=True)
    ap.add_argument("--origin-time-tol", type=float, default=10.0,
                    help="Seconds for waveform↔hypocenter association")
    ap.add_argument("--out-quakeml", required=True)
    ap.add_argument("--default-net", default="XB")
    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_evt = pd.read_csv(args.waveform_event_index)
    df_pm  = pd.read_csv(args.waveform_pick_map, low_memory=False, dtype={"pick_id": str})
    df_pk  = pd.read_csv(args.pick_index, low_memory=False)
    df_he  = pd.read_csv(args.hypo_event_index)
    df_ho  = pd.read_csv(args.hypo_origin_index)

    # ------------------------------------------------------------------
    # Parse times
    # ------------------------------------------------------------------

    if "starttime" not in df_evt.columns:
        raise SystemExit("Step 50 requires 'starttime' column from Step 32")

    df_evt["event_time"] = pd.to_datetime(
        df_evt["starttime"], format="mixed", utc=True, errors="coerce"
    )
    if df_evt["event_time"].isna().any():
        raise SystemExit("Unparseable starttime values in Step 32 catalog")

    df_pk["pick_time"] = pd.to_datetime(df_pk["pick_time"], format="mixed", utc=True)
    df_he["preferred_origin_time"] = pd.to_datetime(
        df_he["preferred_origin_time"], format="mixed", utc=True
    )
    df_ho["origin_time"] = pd.to_datetime(
        df_ho["origin_time"], format="mixed", utc=True
    )

    picks_by_id = df_pk.set_index("pick_id", drop=False)
    pm_by_event = df_pm.groupby("event_id")

    # ------------------------------------------------------------------
    # GLOBAL waveform ↔ hypocenter association
    # ------------------------------------------------------------------

    w_times = (
        df_evt[["event_id", "event_time"]]
        .dropna()
        .sort_values("event_time")
        .reset_index(drop=True)
    )

    h_times = (
        df_he[["event_id", "preferred_origin_time"]]
        .dropna()
        .sort_values("preferred_origin_time")
        .reset_index(drop=True)
    )

    matches = pd.merge_asof(
        w_times,
        h_times,
        left_on="event_time",
        right_on="preferred_origin_time",
        tolerance=pd.Timedelta(seconds=args.origin_time_tol),
        direction="nearest",
        suffixes=("_w", "_h"),
    ).dropna(subset=["event_id_h"])

    matches["event_id_h"] = matches["event_id_h"].astype(int)

    used_hypo_ids = set(matches["event_id_h"])
    hypo_for_waveform = dict(zip(matches["event_id_w"], matches["event_id_h"]))

    # ------------------------------------------------------------------
    # Build ObsPy Catalog
    # ------------------------------------------------------------------

    catalog = Catalog()
    comp = {"W+P+H": 0, "W+P": 0, "W+H": 0, "W only": 0, "H only": 0}

    # ---- waveform-centered events ----
    for _, erow in df_evt.iterrows():
        eid = erow["event_id"]
        ev = Event(resource_id=ResourceIdentifier(f"event/{eid}"))
        ev.comments = []

        # --- waveform starttime provenance ---
        ev.comments.append(
            Comment(text=f"waveform_starttime:{erow['event_time'].isoformat()}")
        )

        # --- waveform filename (basename only) ---
        if "waveform_file" in erow and pd.notna(erow["waveform_file"]):
            wavname = Path(str(erow["waveform_file"])).name
            ev.comments.append(Comment(text=f"wavfile:{wavname}"))

        # Picks
        if eid in pm_by_event.groups:
            for pid in pm_by_event.get_group(eid)["pick_id"]:
                if pid in picks_by_id.index:
                    p = build_pick(picks_by_id.loc[pid], args.default_net)
                    if p:
                        ev.picks.append(p)

        # Hypocenters
        hid = hypo_for_waveform.get(eid)
        if hid is not None:
            for _, orow in df_ho[df_ho["event_id"] == hid].iterrows():
                ev.origins.append(build_origin(orow))
            if ev.origins:
                ev.preferred_origin_id = ev.origins[0].resource_id

        # Composition stats
        has_p = bool(ev.picks)
        has_h = bool(ev.origins)
        if has_p and has_h:
            comp["W+P+H"] += 1
        elif has_p:
            comp["W+P"] += 1
        elif has_h:
            comp["W+H"] += 1
        else:
            comp["W only"] += 1

        catalog.events.append(ev)

    # ---- hypocenter-only events ----
    orphan_hypo_ids = set(df_he["event_id"]) - used_hypo_ids
    for hid in sorted(orphan_hypo_ids):
        ev = Event(resource_id=ResourceIdentifier(f"hypocenter/{hid}"))
        for _, orow in df_ho[df_ho["event_id"] == hid].iterrows():
            ev.origins.append(build_origin(orow))
        if ev.origins:
            ev.preferred_origin_id = ev.origins[0].resource_id
        comp["H only"] += 1
        catalog.events.append(ev)

    # ------------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------------

    out = Path(args.out_quakeml)
    out.parent.mkdir(parents=True, exist_ok=True)
    catalog.write(str(out), format="QUAKEML")

    print("\nSTEP 50 COMPLETE")
    print("----------------")
    print(f"Total ObsPy Events: {len(catalog)}")
    for k, v in comp.items():
        print(f"{k:22s}: {v}")
    print(f"\nQuakeML written: {out}")

if __name__ == "__main__":
    main()