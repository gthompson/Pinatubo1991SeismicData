#!/usr/bin/env python3
"""
individual_pha_to_csv.py

Create a CSV of picks from individual-event PHA files.

Outputs columns matching the monthly parser CSV:
  event_origin, station, channel, phase, pick_time,
  pick_offset_from_origin, onset, first_motion, weight

Usage:
  python individual_pha_to_csv.py --data-top <DATA_TOP> [--dates 910603 910610] [--out <path>]
"""
import argparse
from pathlib import Path
import pandas as pd
import sys

# import shared parser
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent / 'pipeline'))
from pha_parser import parse_individual_pha_file


def process_dates(data_top, dates):
    data_top = Path(data_top).expanduser()
    results = []
    for date_str in dates:
        pha_dir = data_top / 'LEGACY' / 'EVENT_METADATA' / 'PHA' / date_str
        if not pha_dir.exists():
            print(f"Warning: {pha_dir} does not exist, skipping")
            continue
        pha_files = sorted(pha_dir.glob('*.PHA'))
        print(f"Processing {len(pha_files)} files for date {date_str}")
        for pha_file in pha_files:
            picks = parse_individual_pha_file(pha_file)
            if not picks:
                continue
            # compute origin_time: earliest pick.time
            origin_time = min(p['time'] for p in picks)
            for p in picks:
                pick_time = p['time']
                offset = float(pick_time - origin_time)
                results.append({
                    'event_origin': str(origin_time),
                    'station': p.get('station'),
                    'channel': p.get('channel', 'EHZ'),
                    'phase': p.get('phase'),
                    'pick_time': str(pick_time),
                    'pick_offset_from_origin': offset,
                    'onset': p.get('onset'),
                    'first_motion': p.get('first_motion'),
                    'weight': p.get('weight')
                })
    return results


def main():
    ap = argparse.ArgumentParser(description='Build CSV from individual PHA files')
    ap.add_argument('--data-top', required=True, help='DATA_TOP path')
    ap.add_argument('--dates', nargs='+', default=['910603','910610'], help='Dates to include (e.g., 910603)')
    ap.add_argument('--out', default=None, help='Output CSV path (optional)')
    args = ap.parse_args()

    out_path = Path(args.out) if args.out else Path(args.data_top) / 'LEGACY' / 'EVENT_METADATA' / 'PHA' / 'individual_pha_events.csv'
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = process_dates(args.data_top, args.dates)
    if not rows:
        print('No picks found; exiting')
        return
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    print(f'Wrote {len(df)} picks to {out_path}')

if __name__ == '__main__':
    main()
