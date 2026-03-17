#!/usr/bin/env python3
"""
50_build_obspy_catalog.py

STEP 50 — Build ObsPy Event Catalogs (AUTHORITATIVE)

This step lifts the waveform–pick event spine from Step 32 into ObsPy,
then associates hypocenters via a time-based outer join.

Authoritative inputs:
  • 32_waveform_pick_event_index.csv  (event spine + event_class)
  • 32_waveform_pick_event_map.csv    (pick → event mapping)
  • Hypo71 event + origin tables

Event ontology (FINAL, EXPLICIT):
  W_P_H   : waveform + picks + hypocenter
  W_P     : waveform + picks
  W_H     : waveform + hypocenter
  W_ONLY  : waveform only
  P_ONLY  : picks only
  H_ONLY  : hypocenter only

Outputs:
  • catalog_all        (QuakeML + pickle)
  • catalog_waveform   (subset with waveform-related events)
"""

from __future__ import annotations

from pathlib import Path
import argparse
import pickle
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
    if not isinstance(seed_id, str):
        return None, None, None, None
    parts = seed_id.split(".")
    if len(parts) != 4:
        return None, None, None, None
    return parts

def build_pick(row, default_net="XB"):
    net, sta, loc, chan = parse_seed_id(row.get("seed_id"))
    wid = WaveformStreamID(
        network_code=net or default_net,
        station_code=sta,
        location_code=loc or "",
        channel_code=chan,
    )
    return Pick(
        time=UTCDateTime(row["pick_time"]),
        phase_hint=row.get("phase"),
        waveform_id=wid,
        resource_id=ResourceIdentifier(f"pick/{row['pick_id']}")
    )

def build_origin(row):
    return Origin(
        time=UTCDateTime(row["origin_time"]),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        depth=float(row["depth_km"]) * 1000.0,
        resource_id=ResourceIdentifier(f"origin/{row['origin_id']}")
    )

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 50: Build ObsPy Catalogs")
    ap.add_argument("--waveform-event-index", required=True)
    ap.add_argument("--waveform-pick-map", required=True)
    ap.add_argument("--pick-index", required=True)
    ap.add_argument("--hypo-event-index", required=True)
    ap.add_argument("--hypo-origin-index", required=True)
    ap.add_argument("--origin-time-tol", type=float, default=10.0)
    ap.add_argument("--out-prefix", required=True)
    ap.add_argument("--default-net", default="XB")
    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_evt = pd.read_csv(args.waveform_event_index)
    df_map = pd.read_csv(args.waveform_pick_map, dtype={"pick_id": str})
    df_pk  = pd.read_csv(args.pick_index, dtype={"pick_id": str})
    df_he  = pd.read_csv(args.hypo_event_index)
    df_ho  = pd.read_csv(args.hypo_origin_index)

    df_evt["event_time"] = pd.to_datetime(
        df_evt["starttime"], format="mixed", utc=True
    )

    df_pk["pick_time"] = pd.to_datetime(
        df_pk["pick_time"], format="mixed", utc=True
    )

    df_he["preferred_origin_time"] = pd.to_datetime(
        df_he["preferred_origin_time"], format="mixed", utc=True
    )

    df_ho["origin_time"] = pd.to_datetime(
        df_ho["origin_time"], format="mixed", utc=True
    )

    # Index helpers
    picks_by_id = df_pk.set_index("pick_id", drop=False)
    map_by_event = df_map.groupby("event_id")

    # ------------------------------------------------------------------
    # Associate waveform events ↔ hypocenters (time-based outer join)
    # ------------------------------------------------------------------

    w_times = df_evt[["event_id", "event_time"]].sort_values("event_time")
    h_times = df_he[["event_id", "preferred_origin_time"]].sort_values(
        "preferred_origin_time"
    )

    matches = pd.merge_asof(
        w_times, h_times,
        left_on="event_time",
        right_on="preferred_origin_time",
        tolerance=pd.Timedelta(seconds=args.origin_time_tol),
        direction="nearest",
        suffixes=("_w", "_h"),
    ).dropna()

    hypo_for_event = dict(zip(matches["event_id_w"], matches["event_id_h"]))
    used_hypo_ids = set(hypo_for_event.values())

    print(f"Hypocenters associated to waveform/pick events: {len(used_hypo_ids)}")

    # ------------------------------------------------------------------
    # Build catalog_all
    # ------------------------------------------------------------------

    catalog_all = Catalog()
    class_counts = {
        "W_P_H": 0, "W_P": 0, "W_H": 0,
        "W_ONLY": 0, "P_ONLY": 0, "H_ONLY": 0
    }

    for _, erow in df_evt.iterrows():
        eid = erow["event_id"]
        spine_class = erow["event_class"]  # WAV_ONLY, WAV+PICKS, PICKS_ONLY

        has_w = spine_class in ("WAV_ONLY", "WAV+PICKS")
        has_p = spine_class in ("PICKS_ONLY", "WAV+PICKS")

        ev = Event(resource_id=ResourceIdentifier(f"event/{eid}"))
        ev.comments = []

        # Waveform starttime (AUTHORITATIVE from Step 32)
        if has_w:
            ev.comments.append(
                Comment(text=f"waveform_starttime:{erow['event_time'].isoformat()}")
            )

            # Waveform metadata
            if pd.notna(erow.get("wav_file")):
                ev.comments.append(
                    Comment(text=f"wavfile:{Path(erow['wav_file']).name}")
                )

        # Picks
        if eid in map_by_event.groups:
            for _, prow in map_by_event.get_group(eid).iterrows():
                pid = prow["pick_id"]
                if pid in picks_by_id.index:
                    ev.picks.append(
                        build_pick(picks_by_id.loc[pid], args.default_net)
                    )

        # Hypocenters
        has_h = False
        hid = hypo_for_event.get(eid)
        if hid is not None:
            for _, orow in df_ho[df_ho["event_id"] == hid].iterrows():
                ev.origins.append(build_origin(orow))
            if ev.origins:
                ev.preferred_origin_id = ev.origins[0].resource_id
                has_h = True

        # Final EVENT_CLASS
        if has_w and has_p and has_h:
            cls = "W_P_H"
        elif has_w and has_p:
            cls = "W_P"
        elif has_w and has_h:
            cls = "W_H"
        elif has_w:
            cls = "W_ONLY"
        elif has_p:
            cls = "P_ONLY"
        else:
            continue  # should never happen

        ev.comments.append(Comment(text=f"event_class:{cls}"))
        class_counts[cls] += 1
        catalog_all.events.append(ev)

    # ------------------------------------------------------------------
    # Hypocenter-only events
    # ------------------------------------------------------------------

    orphan_hypo_ids = set(df_he["event_id"]) - used_hypo_ids
    for hid in orphan_hypo_ids:
        ev = Event(resource_id=ResourceIdentifier(f"hypocenter/{hid}"))
        ev.comments = [Comment(text="event_class:H_ONLY")]

        for _, orow in df_ho[df_ho["event_id"] == hid].iterrows():
            ev.origins.append(build_origin(orow))

        if ev.origins:
            ev.preferred_origin_id = ev.origins[0].resource_id

        class_counts["H_ONLY"] += 1
        catalog_all.events.append(ev)

    # ------------------------------------------------------------------
    # catalog_waveform (authoritative)
    # ------------------------------------------------------------------

    catalog_waveform = Catalog(
        events=[
            ev for ev in catalog_all
            if any(
                c.text.startswith("event_class:W")
                for c in ev.comments
            )
        ]
    )

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out = Path(args.out_prefix)
    out.parent.mkdir(parents=True, exist_ok=True)

    catalog_all.write(out.with_suffix(".xml"), format="QUAKEML")
    catalog_waveform.write(out.with_name(out.name + "_waveform.xml"), format="QUAKEML")

    with open(out.with_suffix(".pkl"), "wb") as f:
        pickle.dump(catalog_all, f)

    with open(out.with_name(out.name + "_waveform.pkl"), "wb") as f:
        pickle.dump(catalog_waveform, f)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    print("\nSTEP 50 COMPLETE")
    print("----------------")
    print(f"Catalog ALL events:        {len(catalog_all)}")
    print(f"Catalog WAVEFORM events:   {len(catalog_waveform)}")
    print("EVENT_CLASS breakdown (catalog_all):")
    for k in ["W_P_H", "W_P", "W_H", "W_ONLY", "P_ONLY", "H_ONLY"]:
        print(f"  {k:7s}: {class_counts[k]}")
    print(f"Written:")
    print(f"  {out.with_suffix('.xml')}")
    print(f"  {out.with_suffix('.pkl')}")
    print(f"  {out.with_name(out.name + '_waveform.xml')}")
    print(f"  {out.with_name(out.name + '_waveform.pkl')}")

if __name__ == "__main__":
    main()