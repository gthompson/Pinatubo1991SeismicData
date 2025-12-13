#!/usr/bin/env python3
"""
09_build_obspy_catalog.py

STEP 09 — Build ObsPy Catalog from:
  • Step 05b waveform event index (event spine)
  • Step 05b waveform ↔ pick map (event↔pick linkage)
  • Step 04 merged pick index (authoritative pick metadata)
  • Step 08 hypocenter event + origin indexes

This is a TRUE OUTER JOIN:
  • waveform-only events → kept
  • pick-only events → kept (even if missing from waveform-event index)
  • hypocenter-only events → kept

Hypocenters are associated to waveform/pick events by nearest time match
against preferred_origin_time within --origin-time-tol seconds.

Notes:
- "has waveform" is defined by waveform_event_id being present (not by resource_id prefix).
- pick metadata comes from the Step 04 pick index (df_picks) keyed by pick_id.
"""

from __future__ import annotations

from pathlib import Path
import argparse
import pandas as pd

from obspy import UTCDateTime
from obspy.core.event import (
    Catalog, Event, Origin, Pick, Arrival,
    ResourceIdentifier, CreationInfo, WaveformStreamID
)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def parse_seed_id(seed_id: str):
    """
    Parse SEED id like 'XB.ABC..EHZ' -> (net, sta, loc, chan)
    Returns (None, None, None, None) if parse fails.
    """
    if not isinstance(seed_id, str) or not seed_id.strip():
        return (None, None, None, None)
    parts = seed_id.split(".")
    if len(parts) != 4:
        return (None, None, None, None)
    net, sta, loc, chan = parts
    return (net or None, sta or None, loc or None, chan or None)


def safe_series_get(row: pd.Series, key: str, default=None):
    v = row.get(key, default)
    if pd.isna(v):
        return default
    return v


# -----------------------------------------------------------------------------
# Builders
# -----------------------------------------------------------------------------

def build_origin(row: pd.Series) -> Origin:
    return Origin(
        time=UTCDateTime(row["origin_time"]),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        depth=float(row["depth_km"]) * 1000.0,
        resource_id=ResourceIdentifier(f"origin/{row['origin_id']}"),
        creation_info=CreationInfo(
            agency_id="PHIVOLCS/USGS",
            author=str(row.get("source")) if pd.notna(row.get("source")) else None,
        ),
    )


def build_pick(row: pd.Series, default_net: str = "XB") -> Pick:
    # Prefer seed_id if available
    seed_id = safe_series_get(row, "seed_id", None)
    net, sta, loc, chan = parse_seed_id(seed_id) if seed_id else (None, None, None, None)

    # Fall back to station/channel columns if needed
    if net is None:
        net = safe_series_get(row, "network", default_net)
    if sta is None:
        sta = safe_series_get(row, "station", None)
    if chan is None:
        chan = safe_series_get(row, "channel", None)
    if loc is None:
        loc = safe_series_get(row, "location", "")

    wid = WaveformStreamID(
        network_code=str(net) if net is not None else None,
        station_code=str(sta) if sta is not None else None,
        location_code=str(loc) if loc is not None else None,
        channel_code=str(chan) if chan is not None else None,
    )

    pid = safe_series_get(row, "pick_id", None)
    if pid is None or pd.isna(pid):
        return None

    return Pick(
        time=UTCDateTime(row["pick_time"]),
        phase_hint=str(safe_series_get(row, "phase", None)) if safe_series_get(row, "phase", None) is not None else None,
        waveform_id=wid,
        resource_id=ResourceIdentifier(f"pick/{pid}"),
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 09: Build ObsPy Catalog")
    ap.add_argument("--waveform-event-index", required=True)
    ap.add_argument("--waveform-pick-map", required=True)
    ap.add_argument("--pick-index", required=True)
    ap.add_argument("--hypo-event-index", required=True)
    ap.add_argument("--hypo-origin-index", required=True)
    ap.add_argument("--origin-time-tol", type=float, default=10.0)
    ap.add_argument("--out-quakeml", required=True)
    ap.add_argument("--default-net", default="XB")
    args = ap.parse_args()

    # -------------------------------------------------------------------------
    # Load inputs
    # -------------------------------------------------------------------------
    df_wfe = pd.read_csv(args.waveform_event_index)
    df_pickmap = pd.read_csv(args.waveform_pick_map)
    df_picks = pd.read_csv(args.pick_index, low_memory=False)
    df_hyp_evt = pd.read_csv(args.hypo_event_index)
    df_hyp_org = pd.read_csv(args.hypo_origin_index)

    # Times (robust ISO handling)
    df_wfe["event_time"] = pd.to_datetime(df_wfe["origin_time_estimate"], format="mixed", utc=True, errors="coerce")

    if "pick_time" in df_picks.columns:
        df_picks["pick_time"] = pd.to_datetime(df_picks["pick_time"], format="mixed", utc=True, errors="coerce")
    else:
        raise SystemExit("pick-index missing required column: pick_time")

    df_hyp_evt["preferred_origin_time"] = pd.to_datetime(df_hyp_evt["preferred_origin_time"], format="mixed", utc=True, errors="coerce")
    df_hyp_org["origin_time"] = pd.to_datetime(df_hyp_org["origin_time"], format="mixed", utc=True, errors="coerce")

    # Ensure pick_id exists
    if "pick_id" not in df_picks.columns:
        df_picks["pick_id"] = df_picks.index.map(lambda i: f"pick_{i}")

    # Pick lookup by pick_id
    picks_by_id = df_picks.set_index("pick_id", drop=False)

    # -------------------------------------------------------------------------
    # Hypocenter lookup table (time-sorted)
    # -------------------------------------------------------------------------
    hypo_times = (
        df_hyp_evt[["event_id", "preferred_origin_time"]]
        .dropna(subset=["preferred_origin_time"])
        .sort_values("preferred_origin_time")
        .reset_index(drop=True)
    )

    # -------------------------------------------------------------------------
    # TRUE OUTER JOIN EVENT SPINE:
    #   include all event_ids from waveform-event-index AND from pick-map
    # -------------------------------------------------------------------------
    wfe_event_ids = set(df_wfe["event_id"].dropna().astype(str))
    pm_event_ids = set(df_pickmap["event_id"].dropna().astype(str)) if "event_id" in df_pickmap.columns else set()

    all_event_ids = sorted(wfe_event_ids | pm_event_ids)

    # Pre-index waveform-event rows by event_id
    wfe_by_event = df_wfe.set_index("event_id", drop=False)

    # Pre-index pick-map rows by event_id
    if "event_id" not in df_pickmap.columns:
        raise SystemExit("waveform-pick-map missing required column: event_id")
    if "pick_id" not in df_pickmap.columns:
        raise SystemExit("waveform-pick-map missing required column: pick_id")

    pickmap_by_event = df_pickmap.groupby("event_id")

    catalog = Catalog()
    used_hypo_ids: set[int] = set()

    # For composition stats (don’t infer from resource_id prefix)
    comp = {
        "W+P+H": 0,
        "W+P": 0,
        "W+H": 0,
        "W only": 0,
        "P+H": 0,
        "P only": 0,
        "H only": 0,
    }

    # -------------------------------------------------------------------------
    # Build events for the union spine
    # -------------------------------------------------------------------------
    for eid in all_event_ids:
        # Get waveform-event row if present
        wfe_row = wfe_by_event.loc[eid] if eid in wfe_by_event.index else None

        # Determine event_time
        if wfe_row is not None and pd.notna(wfe_row.get("event_time")):
            event_time = wfe_row["event_time"]
        else:
            # If the event isn't in df_wfe (or time is missing), derive time from picks
            # (minimum pick_time across this event_id in pickmap)
            if eid in pickmap_by_event.groups:
                pids = pickmap_by_event.get_group(eid)["pick_id"].tolist()
                times = [picks_by_id.loc[pid]["pick_time"] for pid in pids if pid in picks_by_id.index]
                times = [t for t in times if pd.notna(t)]
                event_time = min(times) if times else pd.NaT
            else:
                event_time = pd.NaT

        ev = Event(resource_id=ResourceIdentifier(f"event/{eid}"))

        # -----------------------------
        # Picks (via pick-map -> pick-index)
        # -----------------------------
        if eid in pickmap_by_event.groups:
            pm = pickmap_by_event.get_group(eid)
            for pid in pm["pick_id"].tolist():
                if pid not in picks_by_id.index:
                    continue

                rows = picks_by_id.loc[[pid]]  # ALWAYS a DataFrame
                for _, prow in rows.iterrows():
                    if pd.notna(prow.get("pick_time")):
                        #ev.picks.append(build_pick(prow, default_net=args.default_net))
                        p = build_pick(prow, default_net=args.default_net)
                        if p is not None:
                            ev.picks.append(p)
        # -----------------------------
        # Hypocenter association (nearest time)
        # -----------------------------
        if pd.notna(event_time) and not hypo_times.empty:
            match = pd.merge_asof(
                pd.DataFrame({"t": [event_time]}),
                hypo_times,
                left_on="t",
                right_on="preferred_origin_time",
                tolerance=pd.Timedelta(seconds=args.origin_time_tol),
                direction="nearest",
            )
            if not match.empty and pd.notna(match.iloc[0]["event_id"]):
                hid = int(match.iloc[0]["event_id"])
                used_hypo_ids.add(hid)

                origins = df_hyp_org[df_hyp_org["event_id"] == hid]
                for _, orow in origins.iterrows():
                    if pd.notna(orow.get("origin_time")):
                        ev.origins.append(build_origin(orow))

                if ev.origins:
                    ev.preferred_origin_id = ev.origins[0].resource_id

        # -----------------------------
        # Composition stats (waveform presence based on waveform_event_id)
        # -----------------------------
        has_waveform = False
        if wfe_row is not None:
            wav_eid = wfe_row.get("waveform_event_id")
            has_waveform = pd.notna(wav_eid) and str(wav_eid).strip() not in ("", "None", "nan")

        has_picks = len(ev.picks) > 0
        has_hypo = len(ev.origins) > 0

        if has_waveform and has_picks and has_hypo:
            comp["W+P+H"] += 1
        elif has_waveform and has_picks:
            comp["W+P"] += 1
        elif has_waveform and has_hypo:
            comp["W+H"] += 1
        elif has_waveform:
            comp["W only"] += 1
        elif has_picks and has_hypo:
            comp["P+H"] += 1
        elif has_picks:
            comp["P only"] += 1
        elif has_hypo:
            comp["H only"] += 1

        catalog.events.append(ev)

    # -------------------------------------------------------------------------
    # Add hypocenter-only events (not used above)
    # -------------------------------------------------------------------------
    all_hypo_ids = set(df_hyp_evt["event_id"].dropna().astype(int))
    orphan_ids = all_hypo_ids - used_hypo_ids

    for hid in sorted(orphan_ids):
        ev = Event(resource_id=ResourceIdentifier(f"hypocenter/{hid}"))
        origins = df_hyp_org[df_hyp_org["event_id"] == hid]
        for _, orow in origins.iterrows():
            if pd.notna(orow.get("origin_time")):
                ev.origins.append(build_origin(orow))
        if ev.origins:
            ev.preferred_origin_id = ev.origins[0].resource_id

        # hypocenter-only composition
        comp["H only"] += 1
        catalog.events.append(ev)

    # -------------------------------------------------------------------------
    # Write output
    # -------------------------------------------------------------------------
    out = Path(args.out_quakeml)
    out.parent.mkdir(parents=True, exist_ok=True)
    catalog.write(str(out), format="QUAKEML")

    print("\nSTEP 09 COMPLETE")
    print("----------------")
    print(f"Total ObsPy Events: {len(catalog)}")
    print("")
    print("Event composition:")
    print(f"  Waveform + Picks + Hypocenter : {comp['W+P+H']}")
    print(f"  Waveform + Picks              : {comp['W+P']}")
    print(f"  Waveform + Hypocenter         : {comp['W+H']}")
    print(f"  Waveform only                 : {comp['W only']}")
    print(f"  Picks + Hypocenter            : {comp['P+H']}")
    print(f"  Picks only                    : {comp['P only']}")
    print(f"  Hypocenter only               : {comp['H only']}")
    print("")
    print(f"QuakeML written: {out}")


if __name__ == "__main__":
    main()