#!/usr/bin/env python3
"""
Enhanced plot_picks_all_mseed.py

Associates picks with MiniSEED files using wfdisc_catalog.csv, then produces
one plot per MiniSEED file, stacking all channels and placing P/S picks from
two CSV files in different colors.

Author: Glenn Thompson + ChatGPT-assisted rewrite
"""

import argparse
from pathlib import Path
import pandas as pd
from obspy import read, UTCDateTime
import matplotlib.pyplot as plt
import numpy as np
from datetime import timezone
import sys


# ---------------------------------------------------------
# Load CSV and convert pick_time → timezone-aware UTC datetime
# ---------------------------------------------------------
def load_pick_csv(csv_path):
    df = pd.read_csv(csv_path)
    df["pick_time_ts"] = pd.to_datetime(df["pick_time"], utc=True)
    df["mseed_file"] = ""   # association target
    return df


# ---------------------------------------------------------
# Load wfdisc and normalize types
# ---------------------------------------------------------
def load_wfdisc(wfdisc_path):
    df = pd.read_csv(wfdisc_path)
    df["starttime"] = pd.to_datetime(df["starttime"], utc=True)
    df["endtime"] = pd.to_datetime(df["endtime"], utc=True)
    df["pick_count"] = 0
    return df


# ---------------------------------------------------------
# Associate picks to MiniSEED files based on time window
# ---------------------------------------------------------
def associate_picks(df_picks, df_wfd, label, log):
    multi_hits = 0
    no_hits = 0

    for idx, row in df_picks.iterrows():
        t = row["pick_time_ts"]

        # Find wfdisc rows whose window includes this pick
        mask = (df_wfd["starttime"] <= t) & (df_wfd["endtime"] >= t)
        matches = df_wfd[mask]

        if len(matches) == 1:
            mseed_file = matches.iloc[0]["file"]
            df_picks.at[idx, "mseed_file"] = mseed_file
            df_wfd.loc[df_wfd["file"] == mseed_file, "pick_count"] += 1

        elif len(matches) > 1:
            multi_hits += 1
            log.append(f"[{label}] MULTI-HIT pick {t} matches {len(matches)} mseed files.")
            df_picks.at[idx, "mseed_file"] = matches.iloc[0]["file"]  # choose earliest
        else:
            no_hits += 1
            log.append(f"[{label}] NO-HIT pick {t} (no MiniSEED window matched).")

    return multi_hits, no_hits


# ---------------------------------------------------------
# Plot one MiniSEED file, all channels stacked
# ---------------------------------------------------------
def plot_one_mseed(mseed_file, df_monthly, df_ind, outdir):
    try:
        st = read(mseed_file)
    except Exception as e:
        print(f"ERROR reading {mseed_file}: {e}")
        return

    # Sort channels for consistent panel layout
    st.sort(keys=["channel"])

    t0 = st[0].stats.starttime
    t0_aware = t0.datetime.replace(tzinfo=timezone.utc)

    nchan = len(st)
    fig, axes = plt.subplots(nchan, 1, figsize=(14, 3 + nchan * 1.5),
                             sharex=True)

    if nchan == 1:
        axes = [axes]

    # Colors
    colors = {
        ("monthly", "P"): "darkred",
        ("monthly", "S"): "darkgreen",
        ("individual", "P"): "lightcoral",
        ("individual", "S"): "lightgreen",
    }

    for ax, tr in zip(axes, st):
        sta = tr.stats.station
        cha = tr.stats.channel
        t = tr.times()
        data = tr.data.astype(float)

        ax.plot(t, data, "k-", lw=0.6)
        ax.set_ylabel(f"{sta}.{cha}")

        # Picks that map to THIS MiniSEED file
        picks_m = df_monthly[df_monthly["mseed_file"] == mseed_file]
        picks_i = df_ind[df_ind["mseed_file"] == mseed_file]

        # Plot them ONLY on matching station+channel
        for df_src, label in [(picks_m, "monthly"), (picks_i, "individual")]:
            for _, row in df_src.iterrows():
                if row["station"] != sta:
                    continue

                # channel match optional — many legacy datasets do not provide channel
                ph = row["phase"].strip().upper()[0]  # P or S
                color = colors.get((label, ph), "blue")

                pt = (UTCDateTime(row["pick_time"]) - t0)
                ax.axvline(pt, color=color, lw=1.2)

    axes[-1].set_xlabel("Time (s from start)")
    fig.suptitle(Path(mseed_file).name)

    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / (Path(mseed_file).stem + ".png")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved {out}")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wav-root", required=True)
    ap.add_argument("--monthly-csv", required=True)
    ap.add_argument("--individual-csv", required=True)
    ap.add_argument("--wfdisc", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    wav_root = Path(args.wav_root)
    outdir = Path(args.outdir)

    # --- Load everything ---
    df_monthly = load_pick_csv(args.monthly_csv)
    df_ind = load_pick_csv(args.individual_csv)
    df_wfd = load_wfdisc(args.wfdisc)

    # --- Associate picks with MiniSEED files ---
    logs = []

    print("Associating monthly picks…")
    m_multi, m_none = associate_picks(df_monthly, df_wfd, "monthly", logs)

    print("Associating individual picks…")
    i_multi, i_none = associate_picks(df_ind, df_wfd, "individual", logs)

    print(f"Monthly:    {m_multi} multi-hit, {m_none} no-hit")
    print(f"Individual: {i_multi} multi-hit, {i_none} no-hit")

    # Save updated CSVs with associations
    df_monthly.to_csv(Path(args.monthly_csv).with_suffix(".associated.csv"), index=False)
    df_ind.to_csv(Path(args.individual_csv).with_suffix(".associated.csv"), index=False)

    # --- Walk WAV root ---
    mseed_files = sorted(wav_root.rglob("*M.*_*"))
    print(f"Found {len(mseed_files)} MiniSEED files.")

    # --- Plot each MiniSEED file ---
    for mseed_path in mseed_files:
        plot_one_mseed(str(mseed_path), df_monthly, df_ind, outdir)

    # Write association log
    with open(outdir / "association_warnings.log", "w") as f:
        for L in logs:
            f.write(L + "\n")

    print("DONE.")


if __name__ == "__main__":
    main()