#!/usr/bin/env python3
"""
23_plot_pick_event_diagnostics.py

STEP 23 of the Pinatubo FAIR pipeline.

Plot and export diagnostics for:
- STEP 20: individual PHA picks
- STEP 21: monthly PHA picks
- STEP 22: merged picks

Produces:
- timeseries plots: picks/day, events/day, median picks/event/day, median duration/day
- distribution plots: picks/event, duration, P-S delay
- station/phase health plots
- CSV summary tables
- optional QC flags JSON

Assumptions:
- Input tables contain at least: event_id, pick_time
- pick_time is parseable by pandas.to_datetime(..., utc=True)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

REQUIRED_COLS = {"event_id", "pick_time"}


def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="mixed", utc=True, errors="coerce")


def ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def safe_read_csv(path: Optional[str]) -> Optional[pd.DataFrame]:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Missing input CSV: {p}")
    return pd.read_csv(p)


def normalize_phase(series: pd.Series) -> pd.Series:
    s = series.astype("string").fillna("").str.strip().str.upper()
    # Keep common canonical labels; map blanks/unknowns to "UNK"
    s = s.replace({"": "UNK", "NONE": "UNK", "NAN": "UNK"})
    return s


def day_floor_utc(ts: pd.Series) -> pd.Series:
    # pandas UTC timestamps → daily bins
    return ts.dt.floor("D")


def savefig(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()


# -----------------------------------------------------------------------------
# Core computations
# -----------------------------------------------------------------------------

def load_pick_table(path: str, source: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"{source} CSV missing required columns: {sorted(missing)}")

    df = ensure_cols(df, ["station", "channel", "seed_id", "phase", "pick_id"])
    df = df.copy()
    df["pick_time"] = to_dt(df["pick_time"])

    bad = df["pick_time"].isna()
    if bad.any():
        # Keep a clean table for plotting; caller can QC counts from this
        df = df.loc[~bad].reset_index(drop=True)

    df["source"] = source
    df["phase"] = normalize_phase(df["phase"])
    df["date"] = day_floor_utc(df["pick_time"])
    return df


def compute_daily_pick_counts(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["date", "source"], as_index=False)
        .size()
        .rename(columns={"size": "n_picks"})
        .sort_values(["date", "source"])
    )
    return out


def compute_event_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build event-level table from pick table:
    - event_time = min pick_time in event
    - duration_s = max-min
    - n_picks
    """
    g = df.groupby(["source", "event_id"], as_index=False)
    ev = g.agg(
        event_time=("pick_time", "min"),
        end_time=("pick_time", "max"),
        n_picks=("pick_time", "size"),
    )
    ev["duration_s"] = (ev["end_time"] - ev["event_time"]).dt.total_seconds()
    ev["date"] = day_floor_utc(ev["event_time"])
    return ev.sort_values(["source", "event_time", "event_id"]).reset_index(drop=True)


def compute_daily_event_counts(ev: pd.DataFrame) -> pd.DataFrame:
    out = (
        ev.groupby(["date", "source"], as_index=False)
        .size()
        .rename(columns={"size": "n_events"})
        .sort_values(["date", "source"])
    )
    return out


def compute_event_size_stats(ev: pd.DataFrame) -> pd.DataFrame:
    def q(x, p):
        return float(np.nanquantile(x, p)) if len(x) else np.nan

    rows = []
    for (date, source), sub in ev.groupby(["date", "source"]):
        x = sub["n_picks"].astype(float).to_numpy()
        rows.append(
            {
                "date": date,
                "source": source,
                "median_picks": float(np.nanmedian(x)) if len(x) else np.nan,
                "p10": q(x, 0.10),
                "p90": q(x, 0.90),
                "max": float(np.nanmax(x)) if len(x) else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values(["date", "source"])


def compute_station_pick_rates(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["date", "source", "station"], as_index=False)
        .size()
        .rename(columns={"size": "n_picks"})
        .sort_values(["date", "source", "station"])
    )
    return out


def compute_phase_counts(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["date", "source", "phase"], as_index=False)
        .size()
        .rename(columns={"size": "n_picks"})
        .sort_values(["date", "source", "phase"])
    )
    return out


def compute_ps_delays(
    df: pd.DataFrame,
    ps_delay_max: float = 60.0,
) -> pd.DataFrame:
    """
    Compute P-S delay (seconds) within each (source,event_id,station),
    using earliest P and earliest S for that station/event.

    Returns rows with:
    - source, event_id, station, p_time, s_time, ps_delay_s
    """
    needed = df[["source", "event_id", "station", "phase", "pick_time"]].copy()
    needed["station"] = needed["station"].astype("string").fillna("").str.strip()
    needed = needed[needed["station"].ne("")].copy()

    # Keep only P and S
    needed = needed[needed["phase"].isin(["P", "S"])].copy()
    if needed.empty:
        return needed.assign(ps_delay_s=pd.Series(dtype=float))

    # Earliest P/S per group
    pmin = (
        needed[needed["phase"] == "P"]
        .groupby(["source", "event_id", "station"], as_index=False)["pick_time"]
        .min()
        .rename(columns={"pick_time": "p_time"})
    )
    smin = (
        needed[needed["phase"] == "S"]
        .groupby(["source", "event_id", "station"], as_index=False)["pick_time"]
        .min()
        .rename(columns={"pick_time": "s_time"})
    )

    merged = pmin.merge(smin, on=["source", "event_id", "station"], how="inner")
    merged["ps_delay_s"] = (merged["s_time"] - merged["p_time"]).dt.total_seconds()

    # Keep reasonable range (still export full, but plotting uses filtered)
    merged["ps_delay_s"] = merged["ps_delay_s"].astype(float)
    merged["ps_delay_ok"] = merged["ps_delay_s"].between(0.0, ps_delay_max, inclusive="both")
    return merged


# -----------------------------------------------------------------------------
# Plotting
# -----------------------------------------------------------------------------

def plot_timeseries_multi(daily: pd.DataFrame, value_col: str, title: str, outpath: Path) -> None:
    plt.figure(figsize=(11, 4))
    for source, sub in daily.groupby("source"):
        plt.plot(sub["date"], sub[value_col], marker="o", linewidth=1, markersize=2, label=source)
    plt.title(title)
    plt.xlabel("Date (UTC)")
    plt.ylabel(value_col)
    plt.grid(True, alpha=0.3)
    plt.legend()
    savefig(outpath)


def plot_timeseries_stat(ev: pd.DataFrame, stat_col: str, title: str, outpath: Path) -> None:
    # ev here is per-event; aggregate per-day per-source
    agg = (
        ev.groupby(["date", "source"], as_index=False)[stat_col]
        .median()
        .rename(columns={stat_col: f"median_{stat_col}"})
        .sort_values(["date", "source"])
    )
    plt.figure(figsize=(11, 4))
    for source, sub in agg.groupby("source"):
        plt.plot(sub["date"], sub[f"median_{stat_col}"], marker="o", linewidth=1, markersize=2, label=source)
    plt.title(title)
    plt.xlabel("Date (UTC)")
    plt.ylabel(f"median_{stat_col}")
    plt.grid(True, alpha=0.3)
    plt.legend()
    savefig(outpath)


def plot_hist_by_source(ev_or_df: pd.DataFrame, col: str, title: str, outpath: Path, bins: int = 60) -> None:
    plt.figure(figsize=(9, 4))
    for source, sub in ev_or_df.groupby("source"):
        x = sub[col].dropna().astype(float).to_numpy()
        if len(x) == 0:
            continue
        plt.hist(x, bins=bins, alpha=0.4, label=source)
    plt.title(title)
    plt.xlabel(col)
    plt.ylabel("count")
    plt.grid(True, alpha=0.3)
    plt.legend()
    savefig(outpath)


def plot_station_stacked_area(
    station_rates: pd.DataFrame,
    top_stations: int,
    outpath: Path,
    title: str,
) -> None:
    # Choose global top stations by total picks
    totals = (
        station_rates.groupby("station", as_index=False)["n_picks"].sum()
        .sort_values("n_picks", ascending=False)
    )
    keep = totals["station"].head(top_stations).tolist()

    df = station_rates[station_rates["station"].isin(keep)].copy()
    if df.empty:
        return

    # Plot per source separately (stacked area gets messy across sources)
    for source, sub in df.groupby("source"):
        pivot = (
            sub.pivot_table(index="date", columns="station", values="n_picks", aggfunc="sum", fill_value=0)
            .sort_index()
        )
        plt.figure(figsize=(11, 4))
        plt.stackplot(pivot.index, pivot.T.values, labels=pivot.columns)
        plt.title(f"{title} — {source} (top {top_stations})")
        plt.xlabel("Date (UTC)")
        plt.ylabel("n_picks")
        plt.grid(True, alpha=0.3)
        plt.legend(loc="upper left", ncol=2, fontsize=8)
        savefig(outpath.parent / f"{outpath.stem}_{source}{outpath.suffix}")


def plot_phase_fraction(
    phase_counts: pd.DataFrame,
    outpath: Path,
    title: str,
    phases: Optional[List[str]] = None,
) -> None:
    if phases is None:
        phases = ["P", "S", "UNK"]

    # Ensure all phases present
    df = phase_counts.copy()
    df["phase"] = df["phase"].astype("string").fillna("UNK")
    df.loc[~df["phase"].isin(phases), "phase"] = "OTHER"

    for source, sub in df.groupby("source"):
        pivot = (
            sub.pivot_table(index="date", columns="phase", values="n_picks", aggfunc="sum", fill_value=0)
            .sort_index()
        )

        # Convert to fractions
        tot = pivot.sum(axis=1).replace(0, np.nan)
        frac = pivot.div(tot, axis=0).fillna(0.0)

        # Plot stacked
        plt.figure(figsize=(11, 4))
        cols = [c for c in ["P", "S", "UNK", "OTHER"] if c in frac.columns]
        plt.stackplot(frac.index, frac[cols].T.values, labels=cols)
        plt.title(f"{title} — {source}")
        plt.xlabel("Date (UTC)")
        plt.ylabel("fraction")
        plt.ylim(0, 1)
        plt.grid(True, alpha=0.3)
        plt.legend(loc="upper left", ncol=4, fontsize=9)
        savefig(outpath.parent / f"{outpath.stem}_{source}{outpath.suffix}")


def plot_ps_delay_box_by_station(ps: pd.DataFrame, top_stations: int, outpath: Path, title: str) -> None:
    if ps.empty:
        return

    # Restrict to OK delays only for the boxplot
    ps_ok = ps[ps["ps_delay_ok"]].copy()
    if ps_ok.empty:
        return

    # Global top stations by number of P-S pairs
    st = (
        ps_ok.groupby("station", as_index=False)
        .size()
        .rename(columns={"size": "n_pairs"})
        .sort_values("n_pairs", ascending=False)
    )
    keep = st["station"].head(top_stations).tolist()

    for source, sub in ps_ok[ps_ok["station"].isin(keep)].groupby("source"):
        data = []
        labels = []
        for station in keep:
            vals = sub.loc[sub["station"] == station, "ps_delay_s"].dropna().astype(float).to_numpy()
            if len(vals) == 0:
                continue
            data.append(vals)
            labels.append(station)

        if not data:
            continue

        plt.figure(figsize=(11, 4))
        plt.boxplot(data, tick_labels=labels, showfliers=False)
        plt.title(f"{title} — {source} (top {top_stations} stations)")
        plt.xlabel("Station")
        plt.ylabel("P–S delay (s)")
        plt.grid(True, alpha=0.3)
        savefig(outpath.parent / f"{outpath.stem}_{source}{outpath.suffix}")


# -----------------------------------------------------------------------------
# QC Flags
# -----------------------------------------------------------------------------

def build_qc_flags(
    df_all: pd.DataFrame,
    ev_all: pd.DataFrame,
    ps: pd.DataFrame,
    ps_delay_max: float,
) -> Dict[str, object]:
    flags: Dict[str, object] = {}

    # 1) P-S delay outliers (monthly in particular)
    if not ps.empty:
        for src in ps["source"].unique():
            sub = ps[ps["source"] == src].copy()
            if len(sub) == 0:
                continue
            frac_bad = float((~sub["ps_delay_ok"]).sum()) / float(len(sub))
            flags[f"{src}_ps_delay_outlier_fraction"] = frac_bad
            flags[f"{src}_ps_delay_outliers"] = bool(frac_bad > 0.02)  # 2% threshold
    else:
        flags["ps_delay_available"] = False

    # 2) Median event duration too large (per source, any day)
    dur_daily = (
        ev_all.groupby(["date", "source"], as_index=False)["duration_s"]
        .median()
        .rename(columns={"duration_s": "median_duration_s"})
    )
    for src, sub in dur_daily.groupby("source"):
        flags[f"{src}_median_event_duration_exceeds_120s"] = bool((sub["median_duration_s"] > 120).any())

    # 3) Basic parse health
    # (if events/day is zero for long stretches, something broke)
    events_daily = compute_daily_event_counts(ev_all)
    for src, sub in events_daily.groupby("source"):
        flags[f"{src}_has_zero_event_days"] = bool((sub["n_events"] == 0).any())

    # 4) Missing S picks for stations (coarse check)
    if "station" in df_all.columns:
        # For each source, fraction of stations that have P picks but never S
        for src, sub in df_all.groupby("source"):
            sub = sub.copy()
            sub["station"] = sub["station"].astype("string").fillna("").str.strip()
            sub = sub[sub["station"].ne("")]

            # Stations with P
            st_p = set(sub.loc[sub["phase"] == "P", "station"].unique().tolist())
            st_s = set(sub.loc[sub["phase"] == "S", "station"].unique().tolist())
            if st_p:
                frac = float(len(st_p - st_s)) / float(len(st_p))
                flags[f"{src}_station_missing_S_fraction"] = frac
                flags[f"{src}_stations_missing_S_flag"] = bool(frac > 0.25)
            else:
                flags[f"{src}_station_missing_S_fraction"] = None

    flags["ps_delay_max_s"] = float(ps_delay_max)
    return flags


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="STEP 23: Plot pick/event diagnostics for Step 20/21/22 tables")

    ap.add_argument("--individual", required=False, help="Step 20 output CSV")
    ap.add_argument("--monthly", required=False, help="Step 21 output CSV")
    ap.add_argument("--merged", required=False, help="Step 22 output CSV")
    ap.add_argument("--outdir", required=True, help="Output directory for plots/CSVs")

    ap.add_argument("--top-stations", type=int, default=10)
    ap.add_argument("--ps-delay-max", type=float, default=60.0)

    ap.add_argument("--emit-csv", action="store_true", help="Write CSV summary tables")
    ap.add_argument("--emit-qc", action="store_true", help="Write QC flags JSON")

    args = ap.parse_args()

    outdir = Path(args.outdir)

    sources: List[Tuple[str, Optional[str]]] = [
        ("individual", args.individual),
        ("monthly", args.monthly),
        ("merged", args.merged),
    ]
    sources = [(name, path) for (name, path) in sources if path]

    if not sources:
        raise SystemExit("No inputs provided. Use at least one of --individual/--monthly/--merged")

    # Load and concatenate
    dfs = []
    for name, path in sources:
        print(f"Loading {name}: {path}")
        dfs.append(load_pick_table(path, source=name))

    df_all = pd.concat(dfs, ignore_index=True)
    if df_all.empty:
        raise SystemExit("All inputs are empty after dropping unparseable pick_time rows.")

    ev_all = compute_event_table(df_all)

    # Summaries
    daily_picks = compute_daily_pick_counts(df_all)
    daily_events = compute_daily_event_counts(ev_all)
    event_stats = compute_event_size_stats(ev_all)
    station_rates = compute_station_pick_rates(df_all)
    phase_counts = compute_phase_counts(df_all)
    ps = compute_ps_delays(df_all, ps_delay_max=float(args.ps_delay_max))

    # Write CSVs
    if args.emit_csv:
        daily_picks.to_csv(outdir / "23_daily_pick_counts.csv", index=False)
        daily_events.to_csv(outdir / "23_daily_event_counts.csv", index=False)
        event_stats.to_csv(outdir / "23_event_size_stats.csv", index=False)
        station_rates.to_csv(outdir / "23_station_pick_rates.csv", index=False)
        phase_counts.to_csv(outdir / "23_phase_counts.csv", index=False)
        # P-S table (full, includes ok flag)
        if not ps.empty:
            ps.to_csv(outdir / "23_ps_delay_distribution.csv", index=False)
        # Event table useful for deeper debugging
        ev_all.to_csv(outdir / "23_event_table.csv", index=False)

    # Plots: time series
    plot_timeseries_multi(daily_picks, "n_picks", "Picks per day (UTC)", outdir / "23_picks_per_day.png")
    plot_timeseries_multi(daily_events, "n_events", "Events per day (UTC)", outdir / "23_events_per_day.png")
    plot_timeseries_stat(ev_all, "n_picks", "Median picks per event per day (UTC)", outdir / "23_median_picks_per_event_per_day.png")
    plot_timeseries_stat(ev_all, "duration_s", "Median event duration (s) per day (UTC)", outdir / "23_median_event_duration_per_day.png")

    # Plots: distributions
    plot_hist_by_source(ev_all, "n_picks", "Distribution: picks per event", outdir / "23_hist_picks_per_event.png", bins=60)
    plot_hist_by_source(ev_all, "duration_s", "Distribution: event duration (s)", outdir / "23_hist_event_duration_s.png", bins=80)

    if not ps.empty:
        # Use only ok delays for histogram (less dominated by parser bugs)
        ps_ok = ps[ps["ps_delay_ok"]].copy()
        if not ps_ok.empty:
            plot_hist_by_source(ps_ok, "ps_delay_s", f"Distribution: P–S delay (<= {args.ps_delay_max}s)", outdir / "23_hist_ps_delay_s.png", bins=80)
        else:
            # still write a plot showing "nothing ok"
            plt.figure(figsize=(8, 3))
            plt.title("P–S delay: no values within configured bounds")
            plt.text(0.01, 0.5, "All P–S delays fell outside [0, ps-delay-max].", transform=plt.gca().transAxes)
            plt.axis("off")
            savefig(outdir / "23_hist_ps_delay_s.png")

        plot_ps_delay_box_by_station(ps, top_stations=int(args.top_stations), outpath=outdir / "23_box_ps_delay_by_station.png", title="P–S delay by station")

    # Plots: station & phase health
    plot_station_stacked_area(station_rates, top_stations=int(args.top_stations), outpath=outdir / "23_station_pick_counts_stacked.png", title="Top-station pick counts (stacked area)")
    plot_phase_fraction(phase_counts, outpath=outdir / "23_phase_fraction.png", title="Phase fraction over time (stacked)")

    # QC flags JSON
    if args.emit_qc:
        qc = build_qc_flags(df_all, ev_all, ps, ps_delay_max=float(args.ps_delay_max))
        with open(outdir / "23_qc_flags.json", "w") as f:
            json.dump(qc, f, indent=2, default=str)
        print(f"QC flags: {outdir / '23_qc_flags.json'}")

    # Console summary
    print("\n=== STEP 23 COMPLETE ===")
    print(f"Total picks (all sources): {len(df_all)}")
    print(f"Total events (all sources): {len(ev_all)}")
    print(f"Plots dir: {outdir}")
    if args.emit_csv:
        print(f"CSV dir:   {outdir}")


if __name__ == "__main__":
    main()