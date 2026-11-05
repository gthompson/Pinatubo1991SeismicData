#!/usr/bin/env python3
"""
33_plot_event_association_diagnostics.py

STEP 33 of the Pinatubo FAIR pipeline.

Diagnostics for waveform ↔ pick association quality using:
  - STEP 30 individual pick ↔ waveform associations
  - STEP 32 authoritative event catalog + pick map

Focus:
  • Coverage
  • Gaps
  • Daily association statistics
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from .shared import (to_dt, savefig)


# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------

def run(
    *,
    waveform_index,
    step30_event_csv,
    event_catalog,
    pick_map,
    outdir
):
    """Run this Pinatubo step with explicit notebook arguments. Return tables and results for inspection."""
    from types import SimpleNamespace
    args = SimpleNamespace(
        waveform_index=waveform_index,
        step30_event_csv=step30_event_csv,
        event_catalog=event_catalog,
        pick_map=pick_map,
        outdir=outdir,
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------------
    # Load data
    # -------------------------------------------------------------------------

    wav = pd.read_csv(args.waveform_index, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})
    wav["date"] = to_dt(wav["starttime"]).dt.floor("D")

    ev = pd.read_csv(args.event_catalog, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})
    ev["date"] = to_dt(ev["starttime"]).dt.floor("D")

    picks = pd.read_csv(args.pick_map, dtype={"event_id": str, "waveform_id": str, "waveform_event_id": str, "source_event_id": str, "pick_id": str, "origin_id": str})
    picks["pick_time"] = to_dt(picks["pick_time"])
    picks["date"] = picks["pick_time"].dt.floor("D")

    # -------------------------------------------------------------------------
    # Daily waveform coverage
    # -------------------------------------------------------------------------

    daily_wav = wav.groupby("date").size().rename("n_waveforms")

    daily_ev = (
        ev.groupby(["date", "event_class"])
        .size()
        .unstack(fill_value=0)
    )

    df = pd.concat([daily_wav, daily_ev], axis=1).fillna(0)

    plt.figure(figsize=(11, 4))
    plt.plot(df.index, df["n_waveforms"], label="All waveforms", lw=2)
    plt.plot(df.index, df.get("WAV+PICKS", 0), label="Waveforms with picks")
    plt.plot(df.index, df.get("WAV_ONLY", 0), label="Waveforms without picks")
    plt.legend()
    plt.title("Waveform coverage per day")
    plt.xlabel("Date (UTC)")
    plt.ylabel("Count")
    plt.grid(True, alpha=0.3)
    savefig(outdir / "33_waveform_coverage.png")

    # -------------------------------------------------------------------------
    # Pick-only vs waveform events
    # -------------------------------------------------------------------------

    plt.figure(figsize=(11, 4))
    plt.plot(df.index, df.get("PICKS_ONLY", 0), label="Pick-only events")
    plt.plot(df.index, df.get("WAV+PICKS", 0), label="Waveform+Pick events")
    plt.legend()
    plt.title("Event association types per day")
    plt.xlabel("Date (UTC)")
    plt.ylabel("Events")
    plt.grid(True, alpha=0.3)
    savefig(outdir / "33_event_types.png")

    # -------------------------------------------------------------------------
    # Picks per event
    # -------------------------------------------------------------------------

    picks_per_event = (
        picks.groupby(["event_id", "event_class"])
        .size()
        .reset_index(name="n_picks")
    )

    evp = picks_per_event[picks_per_event["event_class"] == "WAV+PICKS"]

    if not evp.empty:
        evp["date"] = (
            ev.merge(evp[["event_id"]], on="event_id")["date"]
        )

        daily_mean = evp.groupby("date")["n_picks"].mean()

        plt.figure(figsize=(11, 4))
        plt.plot(daily_mean.index, daily_mean.values)
        plt.title("Mean picks per WAV+PICKS event per day")
        plt.xlabel("Date (UTC)")
        plt.ylabel("Mean picks")
        plt.grid(True, alpha=0.3)
        savefig(outdir / "33_mean_picks_per_event.png")

    # -------------------------------------------------------------------------
    # Primary vs secondary picks
    # -------------------------------------------------------------------------

    pri_frac = (
        picks.groupby(["date", "pick_priority"])
        .size()
        .unstack(fill_value=0)
    )

    if "primary" in pri_frac.columns:
        frac = pri_frac["primary"] / pri_frac.sum(axis=1)

        plt.figure(figsize=(11, 4))
        plt.plot(frac.index, frac.values)
        plt.title("Fraction of primary (individual) picks per day")
        plt.xlabel("Date (UTC)")
        plt.ylabel("Fraction")
        plt.ylim(0, 1)
        plt.grid(True, alpha=0.3)
        savefig(outdir / "33_primary_pick_fraction.png")

    print("\n=== STEP 33 COMPLETE ===")
    print(f"plots written to: {outdir}")
    return {k: v for k, v in locals().items() if (k.startswith("df") or k.startswith("catalog") or k.endswith("_rows") or k in ("events", "annotations", "errors", "stats", "diag", "merged", "suppressed", "matches", "rows", "unparsed"))}
