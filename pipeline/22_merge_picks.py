#!/usr/bin/env python3
"""
22_merge_picks.py

STEP 22 of the Pinatubo FAIR pipeline.

Merge pick indices from:
  - STEP 20: individual-event PHA picks (authoritative / primary)
  - STEP 21: monthly PHA block picks (secondary / provisional)

This step:
• preserves full raw provenance (raw_line, raw_lineno, pha_file)
• deduplicates picks within each event_id
• suppresses monthly picks that duplicate authoritative picks
• emits a unified, FAIR pick table

IMPORTANT SEMANTICS
-------------------
• event_id is the ONLY grouping key
• event_id_source distinguishes authoritative vs provisional groupings
• NO physical events are created here
"""

from pathlib import Path
import argparse
import pandas as pd


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def normalize_seed_id(row: pd.Series) -> str:
    """Return seed_id if present, otherwise construct XB.<sta>..<chan>"""
    sid = row.get("seed_id")
    if pd.notna(sid) and str(sid).strip():
        return str(sid).strip()
    sta = str(row.get("station", "")).strip()
    cha = str(row.get("channel", "")).strip()
    return f"XB.{sta}..{cha}"


def parse_time_col(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(df[col], format="mixed", errors="coerce", utc=True)


def dedupe_within_event(
    df: pd.DataFrame,
    time_col: str,
    tol_s: float,
):
    """
    Deduplicate picks within each event_id.

    For each (event_id, seed_norm, phase),
    picks within tol_s are duplicates.
    Keeps first occurrence.
    """
    if df.empty:
        return df.copy(), df.iloc[0:0].copy()

    df = df.sort_values(
        ["event_id", "seed_norm", "phase", time_col]
    ).reset_index(drop=True)

    key_cols = ["event_id", "seed_norm", "phase"]
    df["_dt"] = df.groupby(key_cols)[time_col].diff().dt.total_seconds()

    dup_mask = df["_dt"].notna() & (df["_dt"].abs() <= tol_s)

    suppressed = df.loc[dup_mask].drop(columns="_dt")
    kept = df.loc[~dup_mask].drop(columns="_dt")

    return kept.reset_index(drop=True), suppressed.reset_index(drop=True)


# -----------------------------------------------------------------------------
# Merge logic
# -----------------------------------------------------------------------------

def merge_pick_tables(
    df_primary: pd.DataFrame,
    df_secondary: pd.DataFrame,
    tolerance: float,
):

    # ------------------------------------------------------------------
    # Normalize & parse times
    # ------------------------------------------------------------------

    df_primary = df_primary.copy()
    df_secondary = df_secondary.copy()

    df_primary["seed_norm"] = df_primary.apply(normalize_seed_id, axis=1)
    df_secondary["seed_norm"] = df_secondary.apply(normalize_seed_id, axis=1)

    df_primary["pick_time_dt"] = parse_time_col(df_primary, "pick_time")
    df_secondary["pick_time_dt"] = parse_time_col(df_secondary, "pick_time")

    # Drop unparseable times
    prim_bad = df_primary["pick_time_dt"].isna()
    sec_bad = df_secondary["pick_time_dt"].isna()

    df_primary = df_primary.loc[~prim_bad].reset_index(drop=True)
    df_secondary = df_secondary.loc[~sec_bad].reset_index(drop=True)

    # ------------------------------------------------------------------
    # 1) Deduplicate within events
    # ------------------------------------------------------------------

    df_primary_kept, df_primary_dups = dedupe_within_event(
        df_primary,
        time_col="pick_time_dt",
        tol_s=tolerance,
    )

    df_secondary_kept, df_secondary_dups = dedupe_within_event(
        df_secondary,
        time_col="pick_time_dt",
        tol_s=tolerance,
    )

    # ------------------------------------------------------------------
    # 2) Suppress monthly picks duplicating authoritative picks
    # ------------------------------------------------------------------

    prim = df_primary_kept[
        ["seed_norm", "phase", "pick_time_dt"]
    ].sort_values(["seed_norm", "phase", "pick_time_dt"])

    sec = df_secondary_kept.sort_values(
        ["seed_norm", "phase", "pick_time_dt"]
    )

    keep_sec = []
    suppress_sec = []

    for (seed_norm, phase), sec_block in sec.groupby(
        ["seed_norm", "phase"], sort=False
    ):
        prim_block = prim[
            (prim["seed_norm"] == seed_norm) &
            (prim["phase"] == phase)
        ]

        if prim_block.empty:
            keep_sec.append(sec_block)
            continue

        m = pd.merge_asof(
            sec_block,
            prim_block,
            on="pick_time_dt",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=tolerance),
            suffixes=("", "_prim"),
        )

        matched = m["pick_time_dt_prim"].notna()
        suppress_sec.append(sec_block.loc[matched.values])
        keep_sec.append(sec_block.loc[~matched.values])

    df_secondary_final = (
        pd.concat(keep_sec, ignore_index=True)
        if keep_sec else sec.iloc[0:0].copy()
    )

    df_secondary_suppressed = (
        pd.concat(suppress_sec, ignore_index=True)
        if suppress_sec else sec.iloc[0:0].copy()
    )

    # ------------------------------------------------------------------
    # 3) Build merged output
    # ------------------------------------------------------------------

    df_primary_kept["pick_priority"] = "primary"
    df_secondary_final["pick_priority"] = "secondary"

    merged = pd.concat(
        [df_primary_kept, df_secondary_final],
        ignore_index=True,
    )

    merged = merged.drop(
        columns=["seed_norm", "pick_time_dt"],
        errors="ignore",
    )

    suppressed = pd.concat(
        [
            df_primary_dups.assign(
                suppressed_reason="primary_duplicate_within_event"
            ),
            df_secondary_dups.assign(
                suppressed_reason="secondary_duplicate_within_event"
            ),
            df_secondary_suppressed.assign(
                suppressed_reason="secondary_duplicate_of_primary"
            ),
        ],
        ignore_index=True,
    ).drop(
        columns=["seed_norm", "pick_time_dt"],
        errors="ignore",
    )

    diagnostics = {
        "primary_in": int(len(df_primary)),
        "primary_bad_time": int(prim_bad.sum()),
        "primary_dups_removed": int(len(df_primary_dups)),
        "primary_out": int(len(df_primary_kept)),
        "secondary_in": int(len(df_secondary)),
        "secondary_bad_time": int(sec_bad.sum()),
        "secondary_dups_removed": int(len(df_secondary_dups)),
        "secondary_suppressed_by_primary": int(len(df_secondary_suppressed)),
        "secondary_out": int(len(df_secondary_final)),
        "merged_out": int(len(merged)),
    }

    return merged, suppressed, diagnostics


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="STEP 22: Merge PHA pick tables")
    ap.add_argument("--primary", required=True)
    ap.add_argument("--secondary", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--time-tolerance", type=float, default=0.5)
    ap.add_argument("--report", default=None)
    args = ap.parse_args()

    df_primary = pd.read_csv(args.primary)
    df_secondary = pd.read_csv(args.secondary)

    merged, suppressed, diag = merge_pick_tables(
        df_primary, df_secondary, args.time_tolerance
    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out, index=False)

    print("\n=== STEP 22 SUMMARY ===")
    for k, v in diag.items():
        print(f"{k:35s}: {v}")
    print(f"Output CSV: {out}")

    if args.report:
        rpt = Path(args.report)
        rpt.parent.mkdir(parents=True, exist_ok=True)
        suppressed.to_csv(rpt, index=False)
        print(f"Suppressed CSV: {rpt}")


if __name__ == "__main__":
    main()
