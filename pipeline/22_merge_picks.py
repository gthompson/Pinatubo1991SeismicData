#!/usr/bin/env python3
"""
04_merge_picks.py

STEP 04 of the Pinatubo FAIR pipeline.

Merge pick indices from:
  - STEP 02: individual-event PHA picks (authoritative / primary)
  - STEP 03: monthly PHA picks (secondary)

Primary picks:
• deduplicated within each event (seed+phase+time within tolerance)
• always retained after dedup
• define authoritative pick groups

Secondary picks:
• deduplicated within each monthly block
• suppressed if they duplicate ANY primary pick (same seed_norm, phase, |dt|<=tol)
• otherwise retained as monthly-only groups

Duplicate definition:
• same normalized SEED id
• same phase
• |time difference| <= tolerance seconds

Outputs:
--------
• merged pick CSV with preserved grouping metadata
• optional CSV of suppressed picks
"""

import argparse
from pathlib import Path
import pandas as pd


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def normalize_seed_id(row: pd.Series) -> str:
    sid = row.get("seed_id")
    if pd.notna(sid) and str(sid).strip():
        return str(sid).strip()
    sta = str(row.get("station", "")).strip()
    cha = str(row.get("channel", "")).strip()
    return f"XB.{sta}..{cha}"


def parse_time_col(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(df[col], format="mixed", errors="coerce", utc=True)


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def dedupe_within_group(
    df: pd.DataFrame,
    group_cols: list[str],
    time_col: str,
    tol_s: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Deduplicate picks within each group (event_id for primary, monthly_block_id for secondary).

    Within each group and for each (seed_norm, phase), treat picks as duplicates if
    successive times differ by <= tol_s. Keep the FIRST occurrence, drop the rest.
    """
    if df.empty:
        return df.copy(), df.iloc[0:0].copy()

    df = df.copy()
    df = df.sort_values(group_cols + ["seed_norm", "phase", time_col]).reset_index(drop=True)

    # Compute time diffs within (group, seed, phase)
    key_cols = group_cols + ["seed_norm", "phase"]
    df["_dt"] = df.groupby(key_cols)[time_col].diff().dt.total_seconds()

    # Mark duplicates: within tolerance of previous pick in same (group, seed, phase)
    dup_mask = df["_dt"].notna() & (df["_dt"].abs() <= tol_s)

    suppressed = df.loc[dup_mask].drop(columns=["_dt"])
    kept = df.loc[~dup_mask].drop(columns=["_dt"])

    return kept.reset_index(drop=True), suppressed.reset_index(drop=True)


# -----------------------------------------------------------------------------
# Merge logic
# -----------------------------------------------------------------------------

def merge_pick_tables(df_primary: pd.DataFrame, df_secondary: pd.DataFrame, tolerance: float):
    # Normalise / parse times
    df_primary = df_primary.copy()
    df_secondary = df_secondary.copy()

    # Add required provenance cols so output schema is stable
    # (primary rows won't have monthly cols, secondary rows won't have event_id)
    full_cols = [
        "event_id", "event_id_source", "pick_group_id", "pick_group_type",
        "pick_id", "pha_file", "station", "channel", "seed_id", "phase", "pick_time",
        "onset", "first_motion", "weight",
        "pick_priority", "merged_pick_group_id", "merged_pick_group_type", "merged_event_hint",
        "event_source", "monthly_file", "monthly_block_id",
    ]
    df_primary = ensure_columns(df_primary, full_cols)
    df_secondary = ensure_columns(df_secondary, full_cols)

    df_primary["seed_norm"] = df_primary.apply(normalize_seed_id, axis=1)
    df_secondary["seed_norm"] = df_secondary.apply(normalize_seed_id, axis=1)

    df_primary["pick_time_dt"] = parse_time_col(df_primary, "pick_time")
    df_secondary["pick_time_dt"] = parse_time_col(df_secondary, "pick_time")

    # Drop rows with unparseable times (logically unusable)
    prim_bad = df_primary["pick_time_dt"].isna()
    sec_bad = df_secondary["pick_time_dt"].isna()
    df_primary = df_primary.loc[~prim_bad].reset_index(drop=True)
    df_secondary = df_secondary.loc[~sec_bad].reset_index(drop=True)

    # -------------------------------------------------------------------------
    # 1) Deduplicate within primary events (THIS FIXES YOUR SHOWN DUPLICATES)
    # -------------------------------------------------------------------------
    # Group key for primary is event_id (authoritative pick_group_id == event_id)
    df_primary_kept, df_primary_dups = dedupe_within_group(
        df_primary,
        group_cols=["event_id"],
        time_col="pick_time_dt",
        tol_s=tolerance,
    )

    # -------------------------------------------------------------------------
    # 2) Deduplicate within secondary monthly blocks
    # -------------------------------------------------------------------------
    df_secondary_kept, df_secondary_dups = dedupe_within_group(
        df_secondary,
        group_cols=["monthly_block_id"],
        time_col="pick_time_dt",
        tol_s=tolerance,
    )

    # -------------------------------------------------------------------------
    # 3) Suppress secondary picks that match primary picks (seed+phase within tol)
    #    Use merge_asof per (seed_norm, phase)
    # -------------------------------------------------------------------------
    prim = df_primary_kept[["seed_norm", "phase", "pick_time_dt"]].copy()
    prim = prim.sort_values(["seed_norm", "phase", "pick_time_dt"]).reset_index(drop=True)

    sec = df_secondary_kept.copy()
    sec = sec.sort_values(["seed_norm", "phase", "pick_time_dt"]).reset_index(drop=True)

    keep_sec_parts = []
    suppress_sec_parts = []

    # Work per (seed_norm, phase) to keep merge_asof valid
    for (seed_norm, phase), sec_block in sec.groupby(["seed_norm", "phase"], sort=False):
        prim_block = prim[(prim["seed_norm"] == seed_norm) & (prim["phase"] == phase)]
        if prim_block.empty:
            keep_sec_parts.append(sec_block)
            continue

        m = pd.merge_asof(
            sec_block.sort_values("pick_time_dt"),
            prim_block.sort_values("pick_time_dt"),
            on="pick_time_dt",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=tolerance),
            suffixes=("", "_prim"),
        )

        # If a match occurred, merge_asof will have non-null seed_norm_prim/phase_prim
        matched = m["seed_norm_prim"].notna()
        suppress_sec_parts.append(sec_block.loc[matched.values])
        keep_sec_parts.append(sec_block.loc[~matched.values])

    df_secondary_suppressed = (
        pd.concat(suppress_sec_parts, ignore_index=True)
        if suppress_sec_parts else sec.iloc[0:0].copy()
    )
    df_secondary_final = (
        pd.concat(keep_sec_parts, ignore_index=True)
        if keep_sec_parts else sec.iloc[0:0].copy()
    )

    # -------------------------------------------------------------------------
    # 4) Build merged table with required STEP 04 fields
    # -------------------------------------------------------------------------
    prim_out = df_primary_kept.copy()
    prim_out["pick_priority"] = "primary"
    prim_out["merged_pick_group_id"] = prim_out["pick_group_id"]
    prim_out["merged_pick_group_type"] = "authoritative"
    prim_out["merged_event_hint"] = prim_out["event_id"]

    sec_out = df_secondary_final.copy()
    sec_out["pick_priority"] = "secondary"
    sec_out["merged_pick_group_id"] = sec_out["monthly_block_id"]
    sec_out["merged_pick_group_type"] = "monthly_only"
    sec_out["merged_event_hint"] = pd.NA

    merged = pd.concat([prim_out, sec_out], ignore_index=True)

    # Put columns back in stable order (plus keep anything extra)
    base = [c for c in full_cols if c in merged.columns]
    extras = [c for c in merged.columns if c not in base and c not in ("seed_norm", "pick_time_dt")]
    merged = merged[base + extras].copy()

    # Build suppressed report: primary dups + secondary dups + secondary suppressed-by-primary
    suppressed = pd.concat(
        [
            df_primary_dups.assign(suppressed_reason="primary_duplicate_within_event"),
            df_secondary_dups.assign(suppressed_reason="secondary_duplicate_within_monthly_block"),
            df_secondary_suppressed.assign(suppressed_reason="secondary_duplicate_of_primary"),
        ],
        ignore_index=True,
    )

    # Drop helper cols
    merged = merged.drop(columns=["seed_norm", "pick_time_dt"], errors="ignore")
    suppressed = suppressed.drop(columns=["seed_norm", "pick_time_dt"], errors="ignore")

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
    ap = argparse.ArgumentParser(description="STEP 04: Merge PHA pick tables")
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

    print("\n=== STEP 04 SUMMARY ===")
    print(f"Primary picks (input):                 {diag['primary_in']}")
    print(f"Primary picks (bad time dropped):      {diag['primary_bad_time']}")
    print(f"Primary duplicates removed:            {diag['primary_dups_removed']}")
    print(f"Primary picks (kept):                  {diag['primary_out']}")
    print(f"Secondary picks (input):               {diag['secondary_in']}")
    print(f"Secondary picks (bad time dropped):    {diag['secondary_bad_time']}")
    print(f"Secondary duplicates removed:          {diag['secondary_dups_removed']}")
    print(f"Secondary suppressed vs primary:       {diag['secondary_suppressed_by_primary']}")
    print(f"Secondary picks (kept):                {diag['secondary_out']}")
    print(f"Merged picks (output):                 {diag['merged_out']}")
    print(f"Output CSV:                            {out}")

    if args.report:
        rpt = Path(args.report)
        rpt.parent.mkdir(parents=True, exist_ok=True)
        suppressed.to_csv(rpt, index=False)
        print(f"Suppressed CSV:                        {rpt}")


if __name__ == "__main__":
    main()