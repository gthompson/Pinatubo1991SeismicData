#!/usr/bin/env python3
"""
30_associate_individual_picks_with_waveforms.py

STEP 30 of the Pinatubo FAIR pipeline.

Associate individual-event PHA pick groups with their corresponding
waveform event files.

CRITICAL ASSUMPTION:
• Individual PHA picks were made by humans directly on waveform files
• Therefore ALL pick times for an event must fall within ONE waveform window
"""

from pathlib import Path
import argparse
import pandas as pd


def main():
    ap = argparse.ArgumentParser(
        description="STEP 30: Associate individual PHA events with waveform files"
    )

    ap.add_argument("--individual-picks", required=True)
    ap.add_argument("--waveform-index", required=True)

    ap.add_argument("--out-event-csv", required=True)
    ap.add_argument("--out-pick-map-csv", required=True)
    ap.add_argument("--out-qc-csv", required=True)

    args = ap.parse_args()

    print("=== STEP 30: Associating individual PHA events with waveform files ===")

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_picks = pd.read_csv(args.individual_picks)
    df_wav = pd.read_csv(args.waveform_index)

    df_picks["pick_time"] = pd.to_datetime(
        df_picks["pick_time"], format="mixed", utc=True
    )
    df_wav["starttime"] = pd.to_datetime(
        df_wav["starttime"], format="mixed", utc=True
    )
    df_wav["endtime"] = pd.to_datetime(
        df_wav["endtime"], format="mixed", utc=True
    )

    if "event_id" not in df_picks.columns:
        raise SystemExit("Expected event_id column in individual picks CSV")

    event_rows = []
    pick_map_rows = []
    qc_rows = []

    grouped = df_picks.groupby("event_id")

    # ------------------------------------------------------------------
    # Associate each individual event → waveform
    # ------------------------------------------------------------------

    for event_id, g in grouped:
        tmin = g["pick_time"].min()
        tmax = g["pick_time"].max()

        matches = df_wav[
            (df_wav["starttime"] <= tmin) &
            (df_wav["endtime"] >= tmax)
        ]

        if len(matches) == 1:
            w = matches.iloc[0]

            # --- Event-level row ---
            event_rows.append({
                "event_id": event_id,
                "waveform_event_id": w["event_id"],
                "wav_file": w["wav_file"],
                "starttime": w["starttime"].isoformat(),
                "endtime": w["endtime"].isoformat(),
                "n_picks": len(g),
                "association_method": "individual_pha_enclosure",
            })

            # --- Pick-level mapping ---
            for _, p in g.iterrows():
                pick_map_rows.append({
                    "pick_id": p["pick_id"],
                    "event_id": event_id,
                    "waveform_event_id": w["event_id"],
                    "time_offset_from_start":
                        (p["pick_time"] - w["starttime"]).total_seconds(),
                })

        elif len(matches) == 0:
            qc_rows.append({
                "event_id": event_id,
                "issue": "no_enclosing_waveform",
                "pick_time_min": tmin.isoformat(),
                "pick_time_max": tmax.isoformat(),
            })

        else:
            qc_rows.append({
                "event_id": event_id,
                "issue": "multiple_enclosing_waveforms",
                "n_matches": len(matches),
            })

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out_event = Path(args.out_event_csv)
    out_pick_map = Path(args.out_pick_map_csv)
    out_qc = Path(args.out_qc_csv)

    out_event.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(event_rows).to_csv(out_event, index=False)
    pd.DataFrame(pick_map_rows).to_csv(out_pick_map, index=False)
    pd.DataFrame(qc_rows).to_csv(out_qc, index=False)

    print("\nSTEP 30 COMPLETE")
    print("-----------------")
    print(f"Individual events processed: {len(grouped)}")
    print(f"Associated events:           {len(event_rows)}")
    print(f"QC issues:                  {len(qc_rows)}")
    print(f"Event CSV:                  {out_event}")
    print(f"Pick map CSV:               {out_pick_map}")
    print(f"QC CSV:                     {out_qc}")


if __name__ == "__main__":
    main()