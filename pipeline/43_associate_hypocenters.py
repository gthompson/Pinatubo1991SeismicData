#!/usr/bin/env python3
"""
43_associate_hypocenters.py

STEP 43 of the Pinatubo FAIR pipeline

Associate hypocenters from multiple sources into seismic events
based on time and spatial proximity.

Enhancements:
-------------
• Step-42-style reconciliation diagnostics (fuzzy match)
• Explicit reporting of hypo40 vs pinaall contribution
• Optional CSV diagnostics for publication / QA

Association logic is UNCHANGED.
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
        description="STEP 43: Associate hypocenters into events"
    )
    ap.add_argument("--hypo40", required=True, help="STEP 40 hypocenter CSV")
    ap.add_argument("--hypo41", required=True, help="STEP 41 hypocenter CSV")
    ap.add_argument("--time-tol", type=float, required=True,
                    help="Time tolerance in seconds")
    ap.add_argument("--dist-tol", type=float, required=True,
                    help="Distance tolerance in kilometers")
    ap.add_argument("--preferred-source",
                    choices=["hypo40", "pinaall"],
                    default="hypo40",
                    help="Preferred origin source")
    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-origin-csv", required=True)

    # NEW (optional)
    ap.add_argument("--emit-diagnostics", action="store_true",
                    help="Write Step-42-style fuzzy reconciliation CSVs")

    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Load + normalize inputs
    # ------------------------------------------------------------------

    df40 = pd.read_csv(args.hypo40)
    df41 = pd.read_csv(args.hypo41)

    df40_norm = pd.DataFrame({
        "origin_id": df40.index.map(lambda i: f"hypo40_{i}"),
        "origin_time": pd.to_datetime(df40["origin_time"], format="mixed", utc=True),
        "latitude": df40["latitude"].astype(float),
        "longitude": df40["longitude"].astype(float),
        "depth_km": df40["depth_km"].astype(float),
        "magnitude": df40["magnitude"].astype(float),
        "source": "hypo40",
        "source_file": df40["source_file"],
        "source_line": df40["source_line"],
    })

    df41_norm = pd.DataFrame({
        "origin_id": df41.index.map(lambda i: f"pinaall_{i}"),
        "origin_time": pd.to_datetime(df41["origin_time"], format="mixed", utc=True),
        "latitude": df41["latitude"].astype(float),
        "longitude": df41["longitude"].astype(float),
        "depth_km": df41["depth_km"].astype(float),
        "magnitude": df41["magnitude"].astype(float),
        "source": "pinaall",
        "source_file": df41["source_file"],
        "source_line": df41["source_line"],
    })

    all_origins = (
        pd.concat([df40_norm, df41_norm], ignore_index=True)
        .sort_values("origin_time")
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # Event association (UNCHANGED)
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

        preferred = group[group["source"] == args.preferred_source]
        preferred = preferred.iloc[[0]] if not preferred.empty else group.iloc[[0]]

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
    # Write primary outputs
    # ------------------------------------------------------------------

    out_event = Path(args.out_event_csv)
    out_origin = Path(args.out_origin_csv)
    out_event.parent.mkdir(parents=True, exist_ok=True)

    df_events = pd.DataFrame(events)
    df_origins = pd.DataFrame(origins_out)

    df_events.to_csv(out_event, index=False)
    df_origins.to_csv(out_origin, index=False)

    # ------------------------------------------------------------------
    # Diagnostics (NEW)
    # ------------------------------------------------------------------

    comp = (
        df_origins
        .groupby(["event_id", "source"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    comp["has_hypo40"] = comp.get("hypo40", 0) > 0
    comp["has_pinaall"] = comp.get("pinaall", 0) > 0

    both = comp[comp["has_hypo40"] & comp["has_pinaall"]]
    hypo40_only = comp[comp["has_hypo40"] & ~comp["has_pinaall"]]
    pinaall_only = comp[~comp["has_hypo40"] & comp["has_pinaall"]]

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    print("\nSTEP 43 — HYPOCENTER ASSOCIATION COMPLETE")
    print("----------------------------------------")
    print(f"Total events:            {len(df_events)}")
    print(f"Total origins:           {len(df_origins)}")
    print("")
    print("FUZZY RECONCILIATION SUMMARY")
    print("============================")
    print(f"Events with both sources: {len(both)}")
    print(f"Events with hypo40 only:  {len(hypo40_only)}")
    print(f"Events with pinaall only: {len(pinaall_only)}")
    print("")
    print("ORIGIN COUNTS")
    print("-------------")
    print(f"Hypo40 origins:  {(df_origins['source'] == 'hypo40').sum()}")
    print(f"Pinaall origins: {(df_origins['source'] == 'pinaall').sum()}")
    print("")
    print(f"Event CSV:  {out_event}")
    print(f"Origin CSV: {out_origin}")

    # ------------------------------------------------------------------
    # Optional diagnostic CSVs
    # ------------------------------------------------------------------

    if args.emit_diagnostics:
        diag_dir = out_event.parent / "diagnostics"
        diag_dir.mkdir(parents=True, exist_ok=True)

        comp.to_csv(diag_dir / "43_event_source_composition.csv", index=False)
        both.to_csv(diag_dir / "43_events_with_both_sources.csv", index=False)
        hypo40_only.to_csv(diag_dir / "43_events_hypo40_only.csv", index=False)
        pinaall_only.to_csv(diag_dir / "43_events_pinaall_only.csv", index=False)

        print("\nDiagnostic CSVs written to:")
        print(f"  {diag_dir}")

if __name__ == "__main__":
    main()