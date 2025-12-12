#!/usr/bin/env python3
"""
verify_pha_parsing.py

Verification script that:
1. Parses individual PHA files from two specific dates (1991/06/03 and 1991/06/10)
2. Compares them against the monthly PHA parsing results (pha_events.csv)
3. Reports match statistics and discrepancies

Individual PHA files use a slightly different format than monthly files:
  STN P W YYMMDDHHMMSS.FF     [S-delay S-weight] ...
  
Where:
  - STN: 4-char station code
  - P: phase letter (P or S)
  - W: weight/quality (0-5)
  - YYMMDDHHMMSS.FF: absolute timestamp
  - S-delay: time delay for S-wave (optional)
  - S-weight: S-wave quality (optional)
"""

import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
from obspy import UTCDateTime
import sys


# Use the shared parser from pipeline/pha_parser.py
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent / 'pipeline'))
from pha_parser import parse_individual_pha_file


def fuzzy_match_pick(individual_pick, csv_row, time_tol_sec=0.05):
    """
    Fuzzy match on station, channel, phase, and pick_time within tolerance.
    
    Match criteria:
    - Same station code
    - Same channel
    - Same phase
    - pick_time within ±time_tol_sec seconds
    """
    try:
        csv_time = UTCDateTime(csv_row['pick_time'])
        ind_time = individual_pick.get('time')
        if not ind_time:
            return False
    except:
        return False
    
    station_match = (str(individual_pick.get('station', '')).upper() == 
                    str(csv_row['station']).upper())
    channel_match = (str(individual_pick.get('channel', 'EHZ')).upper() == 
                    str(csv_row.get('channel', 'EHZ')).upper())
    phase_match = (str(individual_pick.get('phase', '')).upper() == 
                  str(csv_row['phase']).upper())
    
    # Check time difference
    time_diff = abs(float(ind_time - csv_time))
    time_match = time_diff <= time_tol_sec
    
    return station_match and channel_match and phase_match and time_match


def verify_date(date_str, data_top, csv_path):
    """
    Verify parsing for a specific date.
    
    Args:
        date_str: Date string like "910603" or "910610"
        data_top: Path to DATA_TOP directory
        csv_path: Path to pha_events.csv
    
    Returns:
        dict with verification results
    """
    legacy_pha_dir = Path(data_top) / "LEGACY" / "EVENT_METADATA" / "PHA" / date_str
    
    if not legacy_pha_dir.exists():
        print(f"❌ Directory not found: {legacy_pha_dir}")
        return None
    
    # Parse all individual PHA files for this date
    individual_picks = []
    pha_files = sorted(legacy_pha_dir.glob("*.PHA"))
    
    print(f"\n📋 Verifying date {date_str}")
    print(f"   Found {len(pha_files)} individual PHA files")
    
    for pha_file in pha_files:
        picks = parse_individual_pha_file(pha_file)
        individual_picks.extend(picks)
    
    print(f"   Total picks from individual files: {len(individual_picks)}")
    
    # Load CSV and filter to this date
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return None
    
    # Extract date from pick_time column
    df['date'] = pd.to_datetime(df['pick_time']).dt.strftime('%y%m%d')
    df_date = df[df['date'] == date_str].copy()

    # Precompute pandas timestamp columns for robust time comparisons
    df_date['event_origin_ts'] = pd.to_datetime(df_date['event_origin'])
    df_date['pick_time_ts'] = pd.to_datetime(df_date['pick_time'])
    
    print(f"   Picks in CSV for {date_str}: {len(df_date)}")
    
    # Try to match picks grouped by individual file (each file is one event)
    matches = []
    unmatched = []

    for pha_file in pha_files:
        picks = parse_individual_pha_file(pha_file)
        if not picks:
            continue

        # derive origin for this individual file: earliest P pick, else earliest pick
        origin_time = min((p['time'] for p in picks if p['phase'] == 'P'), default=None)
        if not origin_time:
            origin_time = min((p['time'] for p in picks), default=None)
        if not origin_time:
            continue

        event_origin = str(origin_time)

        for idx, ind_pick in enumerate(picks):
            # compute pick_time and offset
            pick_time = ind_pick['time']
            pick_time_str = str(pick_time)
            offset = float(pick_time - origin_time)

            # Build boolean mask to find fuzzy-matching rows in df_date
            # Only match on: station, channel, phase (exact)
            # and pick_time (fuzzy, within ±0.05 sec)
            time_tol = 0.05  # seconds
            
            mask = (
                (df_date['station'] == ind_pick['station']) &
                (df_date['channel'] == ind_pick.get('channel', 'EHZ')) &
                (df_date['phase'] == ind_pick['phase'])
            )
            
            # Find rows within time tolerance
            pick_ts = pd.to_datetime(str(pick_time))
            time_diffs = (df_date['pick_time_ts'] - pick_ts).dt.total_seconds().abs()
            mask = mask & (time_diffs <= time_tol)

            found_rows = df_date[mask]
            if not found_rows.empty:
                # Found fuzzy match
                matches.append({'file': pha_file.name, 'individual': ind_pick, 'csv_rows': found_rows})
            else:
                # No fuzzy match; collect candidates by station/phase for reporting
                csv_candidates = df_date[(df_date['station'] == ind_pick['station']) & (df_date['phase'] == ind_pick['phase'])]
                unmatched.append({'file': pha_file.name, 'individual': ind_pick, 'csv_candidates': csv_candidates})    # Calculate statistics
    total_individual = len(individual_picks)
    matched_count = len(matches)
    unmatched_count = len(unmatched)
    
    result = {
        'date': date_str,
        'total_individual_picks': total_individual,
        'total_csv_picks': len(df_date),
        'matched_picks': matched_count,
        'unmatched_picks': unmatched_count,
        'unmatched_details': unmatched,
        'matches': matches,
    }
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Verify PHA parsing by comparing individual and monthly files"
    )
    parser.add_argument("--data-top", required=True,
                       help="Path to DATA_TOP directory")
    parser.add_argument("--csv-path", required=True,
                       help="Path to pha_events.csv")
    parser.add_argument("--dates", nargs='+', default=['910603', '910610'],
                       help="Dates to verify (e.g., 910603 910610)")
    args = parser.parse_args()
    
    print("=" * 70)
    print("PHA PARSING VERIFICATION")
    print("=" * 70)
    
    all_results = []
    
    for date_str in args.dates:
        result = verify_date(date_str, args.data_top, args.csv_path)
        if result:
            all_results.append(result)
    
    # Summary report
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    total_individual = 0
    total_matched = 0
    total_unmatched = 0
    
    for result in all_results:
        print(f"\n📅 Date: {result['date']}")
        print(f"   Individual file picks:  {result['total_individual_picks']}")
        print(f"   CSV picks for date:     {result['total_csv_picks']}")
        print(f"   ✅ Matched:             {result['matched_picks']}")
        print(f"   ❌ Unmatched:           {result['unmatched_picks']}")
        
        if result['unmatched_picks'] > 0:
            print(f"\n   Unmatched picks:")
            for um in result['unmatched_details'][:5]:  # Show first 5
                ip = um['individual']
                raw = ip.get('raw_station', ip.get('station'))
                print(f"      {raw} {ip['phase']} @ {ip.get('time')} (weight={ip.get('weight')})")
                if len(um['csv_candidates']) > 0:
                    print(f"         → CSV has {len(um['csv_candidates'])} similar picks")
            if len(result['unmatched_details']) > 5:
                print(f"      ... and {len(result['unmatched_details']) - 5} more")
        
        total_individual += result['total_individual_picks']
        total_matched += result['matched_picks']
        total_unmatched += result['unmatched_picks']
    
    print("\n" + "=" * 70)
    print("OVERALL STATISTICS")
    print("=" * 70)
    print(f"Total picks from individual files:  {total_individual}")
    print(f"Total matched to CSV:               {total_matched}")
    print(f"Total unmatched:                    {total_unmatched}")
    if total_individual > 0:
        match_pct = (total_matched / total_individual) * 100
        print(f"Match rate:                         {match_pct:.1f}%")


if __name__ == "__main__":
    main()
