#!/usr/bin/env python3
"""
09_build_obspy_catalog.py

STEP 09 — Build ObsPy Catalog from:
  • Step 08 event index
  • Step 08 origin index
  • Step 04 merged pick index

Picks are associated to events by nearest preferred-origin time
within a configurable tolerance.
"""

from __future__ import annotations

from pathlib import Path
import argparse

import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import (
    Catalog, Event, Origin, Pick, Arrival, ResourceIdentifier,
    CreationInfo, WaveformStreamID
)


# -----------------------------------------------------------------------------
# Builders
# -----------------------------------------------------------------------------

def build_origin(row: pd.Series) -> Origin:
    origin = Origin(
        time=UTCDateTime(row["origin_time"]),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        depth=float(row["depth_km"]) * 1000.0,
        resource_id=ResourceIdentifier(f"origin/{row['origin_id']}"),
        creation_info=CreationInfo(
            agency_id="PHIVOLCS/USGS",
            author=str(row["source"]) if pd.notna(row.get("source")) else None,
        )
    )
    origin.method_id = ResourceIdentifier(f"method/{row['source']}")
    return origin


def build_pick(row: pd.Series) -> Pick:
    wid = WaveformStreamID(
        network_code=str(row.get("network")) if pd.notna(row.get("network")) else None,
        station_code=str(row.get("station")) if pd.notna(row.get("station")) else None,
        channel_code=str(row.get("channel")) if pd.notna(row.get("channel")) else None,
        location_code=str(row.get("location")) if pd.notna(row.get("location")) else None,
    )

    return Pick(
        time=UTCDateTime(row["pick_time"]),
        phase_hint=str(row["phase"]) if pd.notna(row.get("phase")) else None,
        resource_id=ResourceIdentifier(f"pick/{row['pick_id']}"),
        waveform_id=wid,
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 09: Build ObsPy Catalog")
    ap.add_argument("--event-index", required=True)
    ap.add_argument("--origin-index", required=True)
    ap.add_argument("--pick-index", required=True)
    ap.add_argument("--pick-time-tol", type=float, default=10.0)
    ap.add_argument("--out-quakeml", required=True)
    ap.add_argument("--out-unassigned-picks", default=None)
    args = ap.parse_args()

    # -------------------------------------------------------------------------
    # Load inputs
    # -------------------------------------------------------------------------
    df_events = pd.read_csv(args.event_index)
    df_origins = pd.read_csv(args.origin_index)
    df_picks = pd.read_csv(args.pick_index)

    # Parse times
    df_events["preferred_origin_time"] = pd.to_datetime(
        df_events["preferred_origin_time"], format="mixed", utc=True
    )
    df_origins["origin_time"] = pd.to_datetime(
        df_origins["origin_time"], format="mixed", utc=True
    )
    df_picks["pick_time"] = pd.to_datetime(
        df_picks["pick_time"], format="mixed", utc=True
    )

    # Ensure pick_id exists
    if "pick_id" not in df_picks.columns:
        df_picks["pick_id"] = df_picks.index.map(lambda i: f"pick_{i}")

    # -------------------------------------------------------------------------
    # Assign picks -> events (nearest origin time)
    # -------------------------------------------------------------------------

    ev_time = (
        df_events[["event_id", "preferred_origin_time"]]
        .rename(columns={"event_id": "event_id_match"})
        .sort_values("preferred_origin_time")
    )

    pk_time = df_picks.sort_values("pick_time").copy()

    assigned = pd.merge_asof(
        pk_time,
        ev_time,
        left_on="pick_time",
        right_on="preferred_origin_time",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=args.pick_time_tol),
    )

    if "event_id_match" not in assigned.columns:
        raise RuntimeError("merge_asof failed: event_id_match missing")

    assigned["event_id"] = assigned["event_id_match"]
    assigned.drop(columns=["event_id_match", "preferred_origin_time"], inplace=True)

    unassigned = assigned[assigned["event_id"].isna()].copy()
    assigned_ok = assigned[assigned["event_id"].notna()].copy()
    assigned_ok["event_id"] = assigned_ok["event_id"].astype(int)

    # -------------------------------------------------------------------------
    # Build Catalog
    # -------------------------------------------------------------------------

    catalog = Catalog()

    origins_by_event = {
        int(eid): g for eid, g in df_origins.groupby("event_id")
    }
    picks_by_event = {
        int(eid): g for eid, g in assigned_ok.groupby("event_id")
    }

    for _, ev in df_events.iterrows():
        eid = int(ev["event_id"])
        event = Event(resource_id=ResourceIdentifier(f"event/{eid}"))

        # Origins
        origin_map = {}
        for _, orow in origins_by_event.get(eid, pd.DataFrame()).iterrows():
            origin = build_origin(orow)
            event.origins.append(origin)
            origin_map[str(orow["origin_id"])] = origin.resource_id

        # Preferred origin
        pref_id = str(ev.get("preferred_origin_id", ""))
        if pref_id in origin_map:
            event.preferred_origin_id = origin_map[pref_id]

        # Picks + Arrivals
        pref_origin = next(
            (o for o in event.origins if o.resource_id == event.preferred_origin_id),
            None,
        )

        for _, prow in picks_by_event.get(eid, pd.DataFrame()).iterrows():
            pick = build_pick(prow)
            event.picks.append(pick)

            if pref_origin is not None:
                pref_origin.arrivals.append(
                    Arrival(pick_id=pick.resource_id, phase=pick.phase_hint)
                )

        catalog.events.append(event)

    # -------------------------------------------------------------------------
    # Output
    # -------------------------------------------------------------------------
    out = Path(args.out_quakeml)
    out.parent.mkdir(parents=True, exist_ok=True)
    catalog.write(str(out), format="QUAKEML")

    if args.out_unassigned_picks:
        p = Path(args.out_unassigned_picks)
        p.parent.mkdir(parents=True, exist_ok=True)
        unassigned.to_csv(p, index=False)

    print("\nSTEP 09 COMPLETE")
    print("----------------")
    print(f"Events:           {len(catalog)}")
    print(f"Picks assigned:   {len(assigned_ok)}")
    print(f"Picks unassigned: {len(unassigned)}")
    print(f"QuakeML:          {out}")


if __name__ == "__main__":
    main()