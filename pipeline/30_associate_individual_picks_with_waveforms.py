#!/usr/bin/env python3
"""
30_associate_individual_picks_with_waveforms.py

STEP 30 of the Pinatubo FAIR pipeline.

Associate authoritative individual-event PHA pick groups
(STEP 20 output) with their corresponding waveform files
(STEP 10 output).

CRITICAL ASSUMPTION
-------------------
• Individual PHA picks were made by analysts directly on waveform files
• Therefore ALL picks for a given individual PHA event must fall
  within ONE waveform window

This step:
• operates ONLY on STEP 20 output
• groups picks by event_id
• maps each individual PHA event to exactly one waveform window
• preserves full pick-level provenance
• does NOT create physical events (that happens in STEP 31)
"""

from pathlib import Path
import argparse
import pandas as pd


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="STEP 30: Associate individual PHA events with waveform files"
    )

    ap.add_argument(
        "--individual-picks",
        required=True,
        help="STEP 20 output CSV (individual PHA picks)",
    )
    ap.add_argument(
        "--waveform-index",
        required=True,
        help="STEP 10 waveform index CSV",
    )

    ap.add_argument(
        "--out-event-csv",
        required=True,
        help="Event-level association output CSV",
    )
    ap.add_argument(
        "--out-pick-map-csv",
        required=True,
        help="Pick-to-waveform mapping CSV",
    )
    ap.add_argument(
        "--out-qc-csv",
        required=True,
        help="QC / diagnostic output CSV",
    )

    args = ap.parse_args()

    print("=== STEP 30: Associating individual PHA events with waveform files ===")

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_picks = pd.read_csv(args.individual_picks)
    df_wav = pd.read_csv(args.waveform_index)

    # ------------------------------------------------------------------
    # Validate inputs
    # ------------------------------------------------------------------

    required_pick_cols = {
        "event_id",
        "event_id_source",
        "pick_id",
        "pick_time",
    }
    missing = required_pick_cols - set(df_picks.columns)
    if missing:
        raise SystemExit(f"Missing required columns in individual picks CSV: {missing}")

    required_wav_cols = {"event_id", "starttime", "endtime"}
    missing = required_wav_cols - set(df_wav.columns)
    if missing:
        raise SystemExit(f"Missing required columns in waveform index CSV: {missing}")

    # ------------------------------------------------------------------
    # Filter to authoritative individual PHA picks
    # ------------------------------------------------------------------

    df_picks = df_picks[
        df_picks["event_id_source"] == "individual_pha_filename"
    ].copy()

    if df_picks.empty:
        raise SystemExit("No individual PHA picks found — nothing to associate")

    # ------------------------------------------------------------------
    # Parse times
    # ------------------------------------------------------------------

    df_picks["pick_time"] = pd.to_datetime(
        df_picks["pick_time"], format="mixed", utc=True, errors="coerce"
    )
    df_wav["starttime"] = pd.to_datetime(
        df_wav["starttime"], format="mixed", utc=True, errors="coerce"
    )
    df_wav["endtime"] = pd.to_datetime(
        df_wav["endtime"], format="mixed", utc=True, errors="coerce"
    )

    if df_picks["pick_time"].isna().any():
        raise SystemExit("Unparseable pick_time values detected in individual picks")

    if df_wav[["starttime", "endtime"]].isna().any().any():
        raise SystemExit("Unparseable starttime/endtime values detected in waveform index")

    # ------------------------------------------------------------------
    # Group by individual PHA event_id
    # ------------------------------------------------------------------

    event_rows = []
    pick_map_rows = []
    qc_rows = []

    grouped = df_picks.groupby("event_id", sort=True)

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
            waveform_event_id = w["event_id"]

            # ----------------------------------------------------------
            # Event-level association
            # ----------------------------------------------------------
            event_rows.append({
                "source_event_id": event_id,
                "source_event_id_source": "individual_pha_filename",
                "waveform_event_id": waveform_event_id,
                "wav_file": w.get("wav_file"),
                "starttime": w["starttime"].isoformat(),
                "endtime": w["endtime"].isoformat(),
                "n_picks": int(len(g)),
                "association_method": "enclosing_waveform",
            })

            # ----------------------------------------------------------
            # Pick-level mapping
            # ----------------------------------------------------------
            for _, p in g.iterrows():
                pick_map_rows.append({
                    "pick_id": p["pick_id"],
                    "source_event_id": event_id,
                    "waveform_event_id": waveform_event_id,
                    "time_offset_from_start":
                        (p["pick_time"] - w["starttime"]).total_seconds(),
                    # raw provenance
                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),
                    "pha_file": p.get("pha_file"),
                })

        elif len(matches) == 0:
            qc_rows.append({
                "source_event_id": event_id,
                "issue": "no_enclosing_waveform",
                "pick_time_min": tmin.isoformat(),
                "pick_time_max": tmax.isoformat(),
            })

        else:
            qc_rows.append({
                "source_event_id": event_id,
                "issue": "multiple_enclosing_waveforms",
                "n_matches": int(len(matches)),
                "pick_time_min": tmin.isoformat(),
                "pick_time_max": tmax.isoformat(),
            })

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    out_event = Path(args.out_event_csv)
    out_pick_map = Path(args.out_pick_map_csv)
    out_qc = Path(args.out_qc_csv)

    out_event.parent.mkdir(parents=True, exist_ok=True)
    out_pick_map.parent.mkdir(parents=True, exist_ok=True)
    out_qc.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(event_rows).to_csv(out_event, index=False)
    pd.DataFrame(pick_map_rows).to_csv(out_pick_map, index=False)
    pd.DataFrame(qc_rows).to_csv(out_qc, index=False)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    print("\nSTEP 30 COMPLETE")
    print("----------------")
    print(f"Individual PHA events processed: {len(grouped)}")
    print(f"Associated waveform events:      {len(event_rows)}")
    print(f"QC issues:                      {len(qc_rows)}")
    print(f"Event CSV:                      {out_event}")
    print(f"Pick map CSV:                   {out_pick_map}")
    print(f"QC CSV:                         {out_qc}")


if __name__ == "__main__":
    main()