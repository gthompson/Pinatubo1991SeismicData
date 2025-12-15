#!/usr/bin/env python3
"""
30_associate_individual_picks_with_waveforms.py

STEP 30 of the Pinatubo FAIR pipeline.

Associate authoritative individual-event picks
(from STEP 22 merged pick table)
with their corresponding waveform files (STEP 10).

IMPORTANT
---------
• STEP 22 defines the authoritative pick universe
• This step MUST NOT resurrect suppressed picks
"""

from pathlib import Path
import argparse
import pandas as pd


def main():
    ap = argparse.ArgumentParser(
        description="STEP 30: Associate individual picks with waveform files"
    )

    ap.add_argument(
        "--merged-picks",
        required=True,
        help="STEP 22 merged pick CSV (authoritative)",
    )
    ap.add_argument(
        "--waveform-index",
        required=True,
        help="STEP 10 waveform index CSV",
    )
    ap.add_argument(
        "--out-event-csv",
        required=True,
    )
    ap.add_argument(
        "--out-pick-map-csv",
        required=True,
    )
    ap.add_argument(
        "--out-qc-csv",
        required=True,
    )

    args = ap.parse_args()

    print("=== STEP 30: Associating individual picks with waveform files ===")

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------

    df_picks = pd.read_csv(args.merged_picks)
    df_wav = pd.read_csv(args.waveform_index)

    # ------------------------------------------------------------------
    # Validate inputs
    # ------------------------------------------------------------------

    required_pick_cols = {
        "event_id",
        "pick_id",
        "pick_time",
        "pick_priority",
    }
    missing = required_pick_cols - set(df_picks.columns)
    if missing:
        raise SystemExit(f"Missing required columns in merged picks CSV: {missing}")

    required_wav_cols = {"event_id", "starttime", "endtime"}
    missing = required_wav_cols - set(df_wav.columns)
    if missing:
        raise SystemExit(f"Missing required columns in waveform index CSV: {missing}")

    # ------------------------------------------------------------------
    # Filter to authoritative individual picks ONLY
    # ------------------------------------------------------------------

    df_picks = df_picks[df_picks["pick_priority"] == "primary"].copy()

    if df_picks.empty:
        raise SystemExit("No authoritative individual picks found in STEP 22")

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
        raise SystemExit("Unparseable pick_time values in merged picks")

    # ------------------------------------------------------------------
    # Group by authoritative individual event_id (from STEP 22)
    # ------------------------------------------------------------------

    event_rows = []
    pick_map_rows = []
    qc_rows = []

    grouped = df_picks.groupby("event_id", sort=True)

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

            event_rows.append({
                "source_event_id": event_id,
                "waveform_event_id": waveform_event_id,
                "wav_file": w.get("wav_file"),
                "starttime": w["starttime"].isoformat(),
                "endtime": w["endtime"].isoformat(),
                "n_picks": int(len(g)),
                "association_method": "enclosing_waveform",
            })

            for _, p in g.iterrows():
                pick_map_rows.append({
                    "pick_id": p["pick_id"],

                    # Canonical interface for Step 32
                    "waveform_id": waveform_event_id,

                    # Preserve original semantics / provenance
                    "waveform_event_id": waveform_event_id,

                    "time_offset_from_start":
                        (p["pick_time"] - w["starttime"]).total_seconds(),

                    "raw_line": p.get("raw_line"),
                    "raw_lineno": p.get("raw_lineno"),
                    "pha_file": p.get("pha_file"),
                })

        elif len(matches) == 0:
            qc_rows.append({
                "source_event_id": event_id,
                "issue": "no_enclosing_waveform",
            })
        else:
            qc_rows.append({
                "source_event_id": event_id,
                "issue": "multiple_enclosing_waveforms",
                "n_matches": int(len(matches)),
            })

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------

    Path(args.out_event_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_pick_map_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_qc_csv).parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(event_rows).to_csv(args.out_event_csv, index=False)
    pd.DataFrame(pick_map_rows).to_csv(args.out_pick_map_csv, index=False)
    pd.DataFrame(qc_rows).to_csv(args.out_qc_csv, index=False)

    print("\nSTEP 30 COMPLETE")
    print("----------------")
    print(f"Authoritative individual events processed: {len(grouped)}")
    print(f"Associated waveform events:              {len(event_rows)}")
    print(f"QC issues:                              {len(qc_rows)}")


if __name__ == "__main__":
    main()