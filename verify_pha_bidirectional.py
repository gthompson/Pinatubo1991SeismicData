#!/usr/bin/env python3
"""
verify_pha_bidirectional.py

Bidirectional fuzzy matching between individual and monthly PHA CSVs.

For each date, report:
1. Individual → Monthly: How many individual picks match monthly? How many don't?
2. Monthly → Individual: How many monthly picks match individual? How many don't?

Match criteria (fuzzy):
- Same station, channel, phase (exact)
- pick_time within ±1.0 second
"""

import argparse
from pathlib import Path
import pandas as pd
from obspy import UTCDateTime
import sys

# import shared parser
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent / 'pipeline'))
from pha_parser import parse_individual_pha_file


def fuzzy_match_row(row_a, df_b, time_tol_sec=1.0):
    """
    Find if row_a matches any row in df_b (fuzzy on time).
    Returns the matching row(s) or empty DataFrame if no match.
    """
    try:
        time_a = pd.to_datetime(row_a['pick_time'])
        station_a = str(row_a['station']).upper()
        channel_a = str(row_a['channel']).upper()
        phase_a = str(row_a['phase']).upper()
    except:
        return pd.DataFrame()
    
    # Filter by exact match on station/channel/phase
    mask = (
        (df_b['station'].str.upper() == station_a) &
        (df_b['channel'].str.upper() == channel_a) &
        (df_b['phase'].str.upper() == phase_a)
    )
    candidates = df_b[mask]
    
    if candidates.empty:
        return pd.DataFrame()
    
    # Check time tolerance
    time_diffs = (pd.to_datetime(candidates['pick_time']) - time_a).dt.total_seconds().abs()
    matches = candidates[time_diffs <= time_tol_sec]
    
    return matches


def verify_date_bidirectional(date_str, monthly_csv_path, individual_csv_path, time_tol=1.0):
    """
    Perform bidirectional matching for a single date.
    
    Returns dict with:
    - individual_to_monthly: matched, unmatched counts
    - monthly_to_individual: matched, unmatched counts
    """
    # Load both CSVs
    try:
        df_monthly = pd.read_csv(monthly_csv_path)
        df_individual = pd.read_csv(individual_csv_path)
    except Exception as e:
        print(f"Error loading CSVs: {e}")
        return None
    
    # Filter to date of interest by pick_time
    df_monthly['date'] = pd.to_datetime(df_monthly['pick_time']).dt.strftime('%y%m%d')
    df_monthly_date = df_monthly[df_monthly['date'] == date_str].copy()
    
    df_individual['date'] = pd.to_datetime(df_individual['pick_time']).dt.strftime('%y%m%d')
    df_individual_date = df_individual[df_individual['date'] == date_str].copy()
    
    print(f"\n{'='*70}")
    print(f"DATE: {date_str}")
    print(f"{'='*70}")
    print(f"Individual CSV picks: {len(df_individual_date)}")
    print(f"Monthly CSV picks:    {len(df_monthly_date)}")
    
    # 1. Individual → Monthly: How many individual picks match monthly?
    print(f"\n>>> DIRECTION 1: Individual → Monthly")
    ind_matched = 0
    ind_unmatched = []
    for idx, row in df_individual_date.iterrows():
        matches = fuzzy_match_row(row, df_monthly_date, time_tol_sec=time_tol)
        if not matches.empty:
            ind_matched += 1
        else:
            ind_unmatched.append(row)
    
    print(f"    Matched:   {ind_matched} / {len(df_individual_date)} ({100*ind_matched/len(df_individual_date):.1f}%)")
    print(f"    Unmatched: {len(ind_unmatched)} / {len(df_individual_date)} ({100*len(ind_unmatched)/len(df_individual_date):.1f}%)")
    
    # 2. Monthly → Individual: How many monthly picks match individual?
    print(f"\n>>> DIRECTION 2: Monthly → Individual")
    mon_matched = 0
    mon_unmatched = []
    for idx, row in df_monthly_date.iterrows():
        matches = fuzzy_match_row(row, df_individual_date, time_tol_sec=time_tol)
        if not matches.empty:
            mon_matched += 1
        else:
            mon_unmatched.append(row)
    
    print(f"    Matched:   {mon_matched} / {len(df_monthly_date)} ({100*mon_matched/len(df_monthly_date):.1f}%)")
    print(f"    Unmatched: {len(mon_unmatched)} / {len(df_monthly_date)} ({100*len(mon_unmatched)/len(df_monthly_date):.1f}%)")
    
    # Sample some unmatched
    if ind_unmatched:
        print(f"\n    Sample unmatched individual picks (first 3):")
        for i, row in enumerate(ind_unmatched[:3]):
            print(f"      {row['station']} {row['phase']} @ {row['pick_time']} (weight={row['weight']})")
    
    if mon_unmatched:
        print(f"\n    Sample unmatched monthly picks (first 3):")
        for i, row in enumerate(mon_unmatched[:3]):
            print(f"      {row['station']} {row['phase']} @ {row['pick_time']} (weight={row['weight']})")
    
    return {
        'date': date_str,
        'individual_count': len(df_individual_date),
        'monthly_count': len(df_monthly_date),
        'individual_to_monthly_matched': ind_matched,
        'individual_to_monthly_unmatched': len(ind_unmatched),
        'monthly_to_individual_matched': mon_matched,
        'monthly_to_individual_unmatched': len(mon_unmatched),
    }


def main():
    ap = argparse.ArgumentParser(
        description="Bidirectional PHA CSV matching (±1.0 sec time tolerance)"
    )
    ap.add_argument("--data-top", required=True, help="DATA_TOP path")
    ap.add_argument("--monthly-csv", required=True, help="Path to monthly pha_events.csv")
    ap.add_argument("--individual-csv", required=True, help="Path to individual_pha_events.csv")
    ap.add_argument("--dates", nargs='+', default=['910603', '910610'], help="Dates to verify")
    args = ap.parse_args()
    
    print("=" * 70)
    print("BIDIRECTIONAL PHA CSV MATCHING (±1.0 sec tolerance)")
    print("=" * 70)
    
    results = []
    for date_str in args.dates:
        result = verify_date_bidirectional(
            date_str,
            args.monthly_csv,
            args.individual_csv,
            time_tol=1.0
        )
        if result:
            results.append(result)
    
    # Summary table
    print(f"\n{'='*70}")
    print("SUMMARY TABLE")
    print(f"{'='*70}")
    print(f"{'Date':<10} {'Ind→Mon':<20} {'Mon→Ind':<20}")
    print(f"{'-'*70}")
    for r in results:
        ind_pct = 100 * r['individual_to_monthly_matched'] / r['individual_count'] if r['individual_count'] > 0 else 0
        mon_pct = 100 * r['monthly_to_individual_matched'] / r['monthly_count'] if r['monthly_count'] > 0 else 0
        print(f"{r['date']:<10} {r['individual_to_monthly_matched']:>3}/{r['individual_count']:<3} ({ind_pct:>5.1f}%)  {r['monthly_to_individual_matched']:>3}/{r['monthly_count']:<3} ({mon_pct:>5.1f}%)")


if __name__ == "__main__":
    main()
