#!/usr/bin/env python3
"""
08_associate_hypocenters.py

STEP 08 of the Pinatubo FAIR pipeline

Associate hypocenters from multiple sources into seismic events
based on time and spatial proximity.

Uses exact origin values from STEP 05 and STEP 06 and links them
into event groups using:
  • time tolerance (seconds)
  • distance tolerance (kilometers)

Outputs:
--------
• Event-level CSV
• Origin-level CSV
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict

import pandas as pd
from obspy.geodetics import locations2degrees, degrees2kilometers


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def km_distance(lat1, lon1, lat2, lon2) -> float:
    deg = locations2degrees(lat1, lon1, lat2, lon2)
    return degrees2kilometers(deg)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="STEP 08: Associate hypocenters into events"
    )
    ap.add_argument("--hypo05", required=True, help="STEP 05 hypocenter CSV")
    ap.add_argument("--hypo06", required=True, help="STEP 06 hypocenter CSV")
    ap.add_argument("--time-tol", type=float, required=True,
                    help="Time tolerance in seconds")
    ap.add_argument("--dist-tol", type=float, required=True,
                    help="Distance tolerance in kilometers")
    ap.add_argument("--preferred-source",
                    choices=["hypo05", "pinaall"],
                    default="hypo05",
                    help="Preferred origin source")
    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-origin-csv", required=True)

    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Load + normalize inputs
    # ------------------------------------------------------------------

    df05 = pd.read_csv(args.hypo05)
    df06 = pd.read_csv(args.hypo06)

    df05_norm = pd.DataFrame({
        "origin_id": df05.index.map(lambda i: f"hypo05_{i}"),
        "origin_time": pd.to_datetime(df05["origin_time"], format="mixed", utc=True),
        "latitude": df05["latitude"].astype(float),
        "longitude": df05["longitude"].astype(float),
        "depth_km": df05["depth_km"].astype(float),
        "magnitude": df05["magnitude"].astype(float),
        "source": "hypo05",
        "source_file": df05["source_file"],
        "source_line": df05["source_line"],
    })

    df06_norm = pd.DataFrame({
        "origin_id": df06.index.map(lambda i: f"pinaall_{i}"),
        "origin_time": pd.to_datetime(df06["origin_time"], format="mixed", utc=True),
        "latitude": df06["latitude"].astype(float),
        "longitude": df06["longitude"].astype(float),
        "depth_km": df06["depth_km"].astype(float),
        "magnitude": df06["magnitude"].astype(float),
        "source": "pinaall",
        "source_file": df06["source_file"],
        "source_line": df06["source_line"],
    })

    all_origins = (
        pd.concat([df05_norm, df06_norm], ignore_index=True)
        .sort_values("origin_time")
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # Event association
    # ------------------------------------------------------------------

    events: List[Dict] = []
    origins_out: List[Dict] = []

    event_id = 0
    assigned = [False] * len(all_origins)

    for i, row in all_origins.iterrows():
        if assigned[i]:
            continue

        event_id += 1
        assigned[i] = True

        members = [i]

        for j in range(i + 1, len(all_origins)):
            if assigned[j]:
                continue

            dt = abs(
                (all_origins.loc[j, "origin_time"] - row["origin_time"])
                .total_seconds()
            )
            if dt > args.time_tol:
                break

            dist = km_distance(
                row["latitude"], row["longitude"],
                all_origins.loc[j, "latitude"],
                all_origins.loc[j, "longitude"]
            )

            if dist <= args.dist_tol:
                assigned[j] = True
                members.append(j)

        group = all_origins.loc[members]

        # Preferred origin selection
        preferred = group[group["source"] == args.preferred_source]
        if preferred.empty:
            preferred = group.iloc[[0]]
        else:
            preferred = preferred.iloc[[0]]

        events.append({
            "event_id": event_id,
            "preferred_origin_time": preferred.iloc[0]["origin_time"].isoformat(),
            "preferred_latitude": preferred.iloc[0]["latitude"],
            "preferred_longitude": preferred.iloc[0]["longitude"],
            "preferred_depth_km": preferred.iloc[0]["depth_km"],
            "preferred_magnitude": preferred.iloc[0]["magnitude"],
            "n_origins": len(group),
        })

        for _, o in group.iterrows():
            origins_out.append({
                "event_id": event_id,
                "origin_id": o["origin_id"],       
                "origin_time": o["origin_time"].isoformat(),
                "latitude": o["latitude"],
                "longitude": o["longitude"],
                "depth_km": o["depth_km"],
                "magnitude": o["magnitude"],
                "source": o["source"],
                "source_file": o["source_file"],
                "source_line": o["source_line"],
            })
    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out_event = Path(args.out_event_csv)
    out_origin = Path(args.out_origin_csv)

    out_event.parent.mkdir(parents=True, exist_ok=True)
    out_origin.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(events).to_csv(out_event, index=False)
    pd.DataFrame(origins_out).to_csv(out_origin, index=False)

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    print("\nSTEP 08 — HYPOCENTER ASSOCIATION COMPLETE")
    print("----------------------------------------")
    print(f"Total events:   {len(events)}")
    print(f"Total origins:  {len(origins_out)}")
    print(f"Event CSV:      {out_event}")
    print(f"Origin CSV:     {out_origin}")


if __name__ == "__main__":
    main()