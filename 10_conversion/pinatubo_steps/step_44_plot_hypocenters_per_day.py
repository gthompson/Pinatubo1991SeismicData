#!/usr/bin/env python3
"""
42a_plot_hypocenters_per_day.py

Diagnostic plot: hypocenters per day from
- STEP 40: HYPO71 (Pinatubo_all.sum)
- STEP 41: PINAALL.DAT

Purpose:
- Visualize temporal overlap
- Explain low exact-match rate in STEP 42
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def load_daily_counts(csv: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(csv)

    if "origin_time" not in df.columns:
        raise ValueError(f"{csv} missing origin_time column")

    df["origin_time"] = pd.to_datetime(
        df["origin_time"], format="mixed", utc=True, errors="coerce"
    )
    df = df.dropna(subset=["origin_time"])

    df["date"] = df["origin_time"].dt.floor("D")

    daily = (
        df.groupby("date")
        .size()
        .rename("n_hypocenters")
        .reset_index()
    )

    daily["source"] = label
    return daily


def run(
    *,
    hypo40,
    hypo41,
    out
):
    """Run this Pinatubo step with explicit notebook arguments. Return tables and results for inspection."""
    from types import SimpleNamespace
    args = SimpleNamespace(
        hypo40=hypo40,
        hypo41=hypo41,
        out=out,
    )

    df40 = load_daily_counts(Path(args.hypo40), "HYPO71 (Pinatubo_all.sum)")
    df41 = load_daily_counts(Path(args.hypo41), "PINAALL.DAT")

    daily = pd.concat([df40, df41], ignore_index=True)

    plt.figure(figsize=(12, 4))
    for src, sub in daily.groupby("source"):
        plt.plot(
            sub["date"],
            sub["n_hypocenters"],
            marker="o",
            markersize=2,
            linewidth=1,
            label=src,
        )

    plt.title("Hypocenters per day (UTC)")
    plt.xlabel("Date")
    plt.ylabel("Number of hypocenters")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=180)
    plt.close()

    print("\nHypocenter daily plot written to:")
    print(out)
    return {k: v for k, v in locals().items() if (k.startswith("df") or k.startswith("catalog") or k.endswith("_rows") or k in ("events", "annotations", "errors", "stats", "diag", "merged", "suppressed", "matches", "rows", "unparsed"))}
