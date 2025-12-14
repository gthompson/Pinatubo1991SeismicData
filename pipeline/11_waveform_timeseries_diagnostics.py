#!/usr/bin/env python3
"""
11_waveform_timeseries_diagnostics.py

Generate time-series and availability diagnostics from
the Step 10 waveform index.

Outputs:
- waveform_files_per_day.png
- stations_per_day.png
- seed_ids_per_day.png
- station_availability.png
- CSV summaries for all of the above
"""

from pathlib import Path
import argparse
import pandas as pd
import matplotlib.pyplot as plt


def main():
    ap = argparse.ArgumentParser(
        description="STEP 11: Waveform archive time-series diagnostics"
    )
    ap.add_argument("--waveform-index", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--net", default="XB")
    ap.add_argument("--fig-dpi", type=int, default=150)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Load waveform index
    # ------------------------------------------------------------------
    df = pd.read_csv(args.waveform_index)

    df["starttime"] = pd.to_datetime(df["starttime"], utc=True, errors="coerce")
    df["date"] = df["starttime"].dt.date

    # Drop rows with bad times
    df = df.dropna(subset=["date"])

    # ------------------------------------------------------------------
    # 1) Waveform files per day
    # ------------------------------------------------------------------
    wf_per_day = df.groupby("date").size().rename("n_waveforms")
    wf_per_day.to_csv(outdir / "waveform_files_per_day.csv")

    plt.figure()
    wf_per_day.plot()
    plt.xlabel("Date")
    plt.ylabel("Number of waveform files")
    plt.title("Waveform files per day")
    plt.tight_layout()
    plt.savefig(outdir / "waveform_files_per_day.png", dpi=args.fig_dpi)
    plt.close()

    # ------------------------------------------------------------------
    # Explode stations & channels
    # ------------------------------------------------------------------
    df["stations_list"] = df["stations"].fillna("").str.split(",")
    df["channels_list"] = df["channels"].fillna("").str.split(",")

    df_sta = df.explode("stations_list").rename(columns={"stations_list": "station"})
    df_sta["station"] = df_sta["station"].str.strip()
    df_sta = df_sta[df_sta["station"] != ""]

    df_chan = df.explode("channels_list").rename(columns={"channels_list": "channel"})
    df_chan["channel"] = df_chan["channel"].str.strip()
    df_chan = df_chan[df_chan["channel"] != ""]

    # ------------------------------------------------------------------
    # 2) Unique stations per day
    # ------------------------------------------------------------------
    stations_per_day = (
        df_sta.groupby("date")["station"]
        .nunique()
        .rename("n_stations")
    )
    stations_per_day.to_csv(outdir / "stations_per_day.csv")

    plt.figure()
    stations_per_day.plot()
    plt.xlabel("Date")
    plt.ylabel("Number of stations")
    plt.title("Stations reporting per day")
    plt.tight_layout()
    plt.savefig(outdir / "stations_per_day.png", dpi=args.fig_dpi)
    plt.close()

    # ------------------------------------------------------------------
    # 3) Unique SEED IDs per day
    # ------------------------------------------------------------------
    df_seed = df.merge(
        df_sta[["event_id", "station"]],
        on="event_id",
        how="left"
    ).merge(
        df_chan[["event_id", "channel"]],
        on="event_id",
        how="left"
    )

    df_seed["seed_id"] = (
        args.net + "." +
        df_seed["station"].astype(str) +
        ".." +
        df_seed["channel"].astype(str)
    )

    seed_per_day = (
        df_seed.groupby("date")["seed_id"]
        .nunique()
        .rename("n_seed_ids")
    )
    seed_per_day.to_csv(outdir / "seed_ids_per_day.csv")

    plt.figure()
    seed_per_day.plot()
    plt.xlabel("Date")
    plt.ylabel("Number of SEED IDs")
    plt.title("SEED IDs reporting per day")
    plt.tight_layout()
    plt.savefig(outdir / "seed_ids_per_day.png", dpi=args.fig_dpi)
    plt.close()

    # ------------------------------------------------------------------
    # 4) Station availability matrix (date × station)
    # ------------------------------------------------------------------
    avail = (
        df_sta.groupby(["date", "station"])
        .size()
        .unstack(fill_value=0)
        .astype(bool)
    )

    avail.to_csv(outdir / "station_availability_matrix.csv")

    plt.figure(figsize=(12, max(4, 0.25 * len(avail.columns))))
    plt.imshow(avail.T, aspect="auto", interpolation="nearest")
    plt.yticks(range(len(avail.columns)), avail.columns)
    plt.xticks(range(0, len(avail.index), max(1, len(avail.index) // 10)),
               avail.index[::max(1, len(avail.index) // 10)],
               rotation=45)
    plt.xlabel("Date")
    plt.ylabel("Station")
    plt.title("Station data availability (by day)")
    plt.tight_layout()
    plt.savefig(outdir / "station_availability.png", dpi=args.fig_dpi)
    plt.close()

    print("\nSTEP 11 COMPLETE")
    print("----------------")
    print(f"Output directory: {outdir}")
    print("Generated plots:")
    print(" - waveform_files_per_day.png")
    print(" - stations_per_day.png")
    print(" - seed_ids_per_day.png")
    print(" - station_availability.png")


if __name__ == "__main__":
    main()