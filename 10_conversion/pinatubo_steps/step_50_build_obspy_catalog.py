"""
50_build_obspy_catalog.py

STEP 50 — Build ObsPy Event Catalogs (AUTHORITATIVE)

Authoritative event spine:
  • Step 32 waveform-centered event catalog

This step lifts the waveform–pick event spine from Step 32 into ObsPy,
then associates hypocenters via a time-based outer join.

Hypocenters:
  • Associated from Step 43 by nearest-time match
  • Greedy one-to-one by default, or explicit nearest-time reuse
  • Both policies respect origin_time_tol
  • Unmatched hypocenters become hypocenter-only events

Authoritative inputs:
  • 32_waveform_pick_event_index.csv  (event spine + event_class)
  • 32_waveform_pick_event_map.csv    (pick → event mapping)
  • pick_index.csv                    (pick metadata)
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
import pickle
import math

import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import (
    Catalog,
    Comment,
    Event,
    Origin,
    Pick,
    ResourceIdentifier,
    WaveformStreamID,
)

from .shared import (parse_seed_id, safe_series_get, associate_one_to_one_by_time, associate_nearest_by_time)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def build_pick(row, default_net="XB"):
    """
    Build an ObsPy Pick from a pick-index row.
    Returns None if no valid pick_id is present.
    """
    pid = safe_series_get(row, "pick_id")
    if pid is None:
        return None

    seed_id = safe_series_get(row, "seed_id")
    net, sta, loc, chan = parse_seed_id(seed_id)

    wid = WaveformStreamID(
        network_code=net or default_net,
        station_code=sta,
        location_code=loc or "",
        channel_code=chan,
    )

    return Pick(
        time=UTCDateTime(row["pick_time"]),
        phase_hint=safe_series_get(row, "phase"),
        waveform_id=wid,
        resource_id=ResourceIdentifier(f"pick/{pid}"),
    )


def build_origin(row):
    """
    Build an ObsPy Origin from a hypo-origin row.
    Assumes depth_km is in km and converts to meters.
    """
    return Origin(
        time=UTCDateTime(row["origin_time"]),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        depth=float(row["depth_km"]) * 1000.0,
        resource_id=ResourceIdentifier(f"origin/{row['origin_id']}"),
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def run(
    *,
    waveform_event_index,
    waveform_pick_map,
    pick_index,
    hypo_event_index,
    hypo_origin_index,
    origin_time_tol=10.0,
    out_prefix,
    default_net='XB',
    association_policy='one_to_one',
):
    """Build both catalog subsets using one shared assembly/export path.

    association_policy='one_to_one' uses deterministic greedy matching.
    'nearest' retains the original reuse-permitting nearest-time policy.
    This applies to the entire Step-32 spine, including pick-only events.
    Outputs keep the canonical Step-50 prefix consumed by steps 51/52.
    Return catalogs, association table, class counts and chosen policy.
    """
    if association_policy not in ('one_to_one', 'nearest'):
        raise ValueError("association_policy must be 'one_to_one' or 'nearest'")
    if not math.isfinite(origin_time_tol) or origin_time_tol < 0:
        raise ValueError("origin_time_tol must be finite and nonnegative")
    from types import SimpleNamespace
    args = SimpleNamespace(
        waveform_event_index=waveform_event_index,
        waveform_pick_map=waveform_pick_map,
        pick_index=pick_index,
        hypo_event_index=hypo_event_index,
        hypo_origin_index=hypo_origin_index,
        origin_time_tol=origin_time_tol,
        out_prefix=out_prefix,
        default_net=default_net,
    )

    # ------------------------------------------------------------------
    # Read inputs
    # ------------------------------------------------------------------

    df_evt = pd.read_csv(args.waveform_event_index, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})
    df_map = pd.read_csv(args.waveform_pick_map, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})
    df_pk = pd.read_csv(args.pick_index, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})
    df_he = pd.read_csv(args.hypo_event_index, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})
    df_ho = pd.read_csv(args.hypo_origin_index, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})

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

    df_pk["pick_time"] = pd.to_datetime(
        df_pk["pick_time"], format="mixed", utc=True, errors="coerce"
    )
    if df_pk["pick_time"].isna().any():
        raise SystemExit("Unparseable pick_time values in pick index")

    df_he["preferred_origin_time"] = pd.to_datetime(
        df_he["preferred_origin_time"], format="mixed", utc=True, errors="coerce"
    )
    df_ho["origin_time"] = pd.to_datetime(
        df_ho["origin_time"], format="mixed", utc=True, errors="coerce"
    )

    # ------------------------------------------------------------------
    # Index helpers
    # ------------------------------------------------------------------

    picks_by_id = df_pk.set_index("pick_id", drop=False)
    map_by_event = df_map.groupby("event_id")

    # ------------------------------------------------------------------
    # Associate waveform events ↔ hypocenters
    # ------------------------------------------------------------------

    if association_policy == 'one_to_one':
        matches = associate_one_to_one_by_time(
            df_wave=df_evt, df_hypo=df_he,
            tolerance_seconds=args.origin_time_tol,
        )
    else:
        matches = associate_nearest_by_time(
            df_evt, df_he, tolerance_seconds=args.origin_time_tol,
        )

    hypo_for_waveform = dict(zip(matches["event_id_w"], matches["event_id_h"])) if not matches.empty else {}
    used_hypo_ids = set(hypo_for_waveform.values())

    print(f"Hypocenters associated to waveform/pick events: {len(used_hypo_ids)}")

    # ------------------------------------------------------------------
    # Build catalog_all
    # ------------------------------------------------------------------

    catalog_all = Catalog()
    class_counts = {
        "W_P_H": 0,
        "W_P": 0,
        "W_H": 0,
        "W_ONLY": 0,
        "P_ONLY": 0,
        "H_ONLY": 0,
    }

    # ---- Step 32 authoritative event spine ----
    for _, erow in df_evt.iterrows():
        eid = erow["event_id"]
        spine_class = safe_series_get(erow, "event_class", "")

        # Expected Step 32 classes:
        #   WAV_ONLY, WAV+PICKS, PICKS_ONLY
        has_w = spine_class in ("WAV_ONLY", "WAV+PICKS")
        has_p_spine = spine_class in ("PICKS_ONLY", "WAV+PICKS")

        ev = Event(resource_id=ResourceIdentifier(f"event/{eid}"))
        ev.comments = []

        # Provenance / metadata
        if has_w:
            ev.comments.append(
                Comment(text=f"waveform_starttime:{erow['event_time'].isoformat()}")
            )

        wav_file = safe_series_get(erow, "wav_file")
        if wav_file is None:
            wav_file = safe_series_get(erow, "waveform_file")

        if wav_file is not None:
            ev.comments.append(Comment(text=f"wavfile:{Path(str(wav_file)).name}"))

        # Picks
        if eid in map_by_event.groups:
            for _, prow in map_by_event.get_group(eid).iterrows():
                pid = prow["pick_id"]
                if pid in picks_by_id.index:
                    p = build_pick(picks_by_id.loc[pid], args.default_net)
                    if p is not None:
                        ev.picks.append(p)

        # Hypocenters
        hid = hypo_for_waveform.get(eid)
        if hid is not None:
            for _, orow in df_ho[df_ho["event_id"] == hid].iterrows():
                ev.origins.append(build_origin(orow))
            if ev.origins:
                ev.preferred_origin_id = ev.origins[0].resource_id

        # Final observed composition
        has_p = bool(ev.picks) or has_p_spine
        has_h = bool(ev.origins)

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
            # A Step 32 event with neither waveform nor picks should not exist.
            continue

        ev.comments.append(Comment(text=f"event_class:{cls}"))
        class_counts[cls] += 1
        catalog_all.events.append(ev)

    # ------------------------------------------------------------------
    # Hypocenter-only events
    # ------------------------------------------------------------------

    orphan_hypo_ids = set(df_he["event_id"]) - used_hypo_ids

    for hid in sorted(orphan_hypo_ids):
        ev = Event(resource_id=ResourceIdentifier(f"hypocenter/{hid}"))
        ev.comments = [Comment(text="event_class:H_ONLY")]

        for _, orow in df_ho[df_ho["event_id"] == hid].iterrows():
            ev.origins.append(build_origin(orow))

        if ev.origins:
            ev.preferred_origin_id = ev.origins[0].resource_id

        class_counts["H_ONLY"] += 1
        catalog_all.events.append(ev)

    # ------------------------------------------------------------------
    # catalog_waveform (authoritative waveform-related subset)
    # ------------------------------------------------------------------

    waveform_classes = {"W_P_H", "W_P", "W_H", "W_ONLY"}

    catalog_waveform = Catalog(
        events=[
            ev
            for ev in catalog_all
            if any(
                c.text == f"event_class:{cls}"
                for c in ev.comments
                for cls in waveform_classes
            )
        ]
    )

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out = Path(args.out_prefix)
    out.parent.mkdir(parents=True, exist_ok=True)

    all_xml = out.with_suffix(".xml")
    all_pkl = out.with_suffix(".pkl")
    wav_xml = out.with_name(out.name + "_waveform.xml")
    wav_pkl = out.with_name(out.name + "_waveform.pkl")

    catalog_all.write(str(all_xml), format="QUAKEML")
    catalog_waveform.write(str(wav_xml), format="QUAKEML")

    with open(all_pkl, "wb") as f:
        pickle.dump(catalog_all, f)

    with open(wav_pkl, "wb") as f:
        pickle.dump(catalog_waveform, f)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    print("\nSTEP 50 COMPLETE")
    print("----------------")
    print(f"Catalog ALL events:      {len(catalog_all)}")
    print(f"Catalog WAVEFORM events: {len(catalog_waveform)}")
    print("EVENT_CLASS breakdown (catalog_all):")
    for k in ["W_P_H", "W_P", "W_H", "W_ONLY", "P_ONLY", "H_ONLY"]:
        print(f"  {k:7s}: {class_counts[k]}")

    if not matches.empty:
        print(f"{association_policy} spine↔hypocenter matches: {len(matches)}")
        print(f"Max accepted |Δt|: {matches['abs_dt_s'].max():.3f} s")
    else:
        print(f"{association_policy} spine↔hypocenter matches: 0")

    print("Written:")
    print(f"  {all_xml}")
    print(f"  {all_pkl}")
    print(f"  {wav_xml}")
    print(f"  {wav_pkl}")
    return {
        'catalog_all': catalog_all,
        'catalog_waveform': catalog_waveform,
        'matches': matches,
        'class_counts': class_counts,
        'association_policy': association_policy,
        'output_paths': {
            'all_xml': all_xml, 'all_pkl': all_pkl,
            'waveform_xml': wav_xml, 'waveform_pkl': wav_pkl,
        },
    }


def main(argv=None):
    """Compatibility CLI shared by the old script entry points."""
    import argparse
    parser = argparse.ArgumentParser(description='STEP 50: Build ObsPy catalogs')
    for name in ('waveform-event-index', 'waveform-pick-map', 'pick-index',
                 'hypo-event-index', 'hypo-origin-index', 'out-prefix'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--origin-time-tol', type=float, default=10.0)
    parser.add_argument('--default-net', default='XB')
    parser.add_argument('--association-policy', choices=('one_to_one', 'nearest'),
                        default='one_to_one')
    return run(**vars(parser.parse_args(argv)))
