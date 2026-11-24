#!/usr/bin/env python
"""Rewrite FLOAT32 MiniSEED waveform files as int32 STEIM2, in place, without changing any sample.

Step 10 wrote the 1991 archive as FLOAT32 although every sample is an integer count
(12-bit DMX samples less the 2048 offset). Integer-only consumers such as a Winston wave
server need integer data. FloVoPy now writes integers directly; this converts files
written before that fix.

For each file under WAV_DIR/YYYY/MM/:
  * files whose traces are all integer already are skipped;
  * every sample must be a whole number within int32, else the file is left alone and
    reported;
  * the new file is written beside the original with the same record length and byte
    order, read back, and compared trace by trace (ID, start time, sampling rate, sample
    count, every sample exactly); only then does it replace the original.
A CSV log of every file is written (--log). Run with --dry-run first to see what would change.

    python convert_wav_float_to_int.py WAV_DIR --log convert_log.csv [--workers 8] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np

INT32 = np.iinfo(np.int32)


def convert(path: str, dry_run: bool = False) -> dict:
    from obspy import read
    row = dict(file=path, status='', traces=0, encoding_before='', bytes_before=os.path.getsize(path),
               bytes_after='', error='')
    try:
        st = read(path, format='MSEED', details=True)
        row['traces'] = len(st)
        encodings = sorted({tr.stats.mseed.encoding for tr in st})
        row['encoding_before'] = ';'.join(encodings)
        if all(tr.data.dtype.kind in 'iu' for tr in st):
            row['status'] = 'already_integer'
            return row
        for tr in st:
            d = tr.data
            if not np.all(np.isfinite(d)) or np.any(d != np.round(d)) or d.min() < INT32.min or d.max() > INT32.max:
                row['status'] = 'not_integer_valued'
                row['error'] = f'{tr.id}: non-integer or out-of-range samples'
                return row
        if dry_run:
            row['status'] = 'would_convert'
            return row
        reclen = st[0].stats.mseed.record_length
        byteorder = st[0].stats.mseed.byteorder
        original = [(tr.id, tr.stats.starttime, tr.stats.sampling_rate, tr.stats.npts, tr.data.copy()) for tr in st]
        for tr in st:
            tr.data = tr.data.astype(np.int32)
        tmp = path + '.int32tmp'
        st.write(tmp, format='MSEED', encoding='STEIM2', reclen=reclen, byteorder=byteorder)
        back = read(tmp, format='MSEED', details=True)
        if len(back) != len(original):
            raise ValueError(f'trace count {len(back)} != {len(original)}')
        for (tid, t0, sr, n, data), tr in zip(original, back):
            if (tr.id, tr.stats.starttime, tr.stats.sampling_rate, tr.stats.npts) != (tid, t0, sr, n):
                raise ValueError(f'{tid}: header changed on rewrite')
            if tr.stats.mseed.encoding != 'STEIM2' or tr.data.dtype != np.int32 or not np.array_equal(tr.data, data):
                raise ValueError(f'{tid}: samples changed on rewrite')
        os.replace(tmp, path)
        row['status'] = 'converted'
        row['bytes_after'] = os.path.getsize(path)
    except Exception as exc:  # noqa: BLE001 - report and leave the original in place
        row['status'] = 'failed'
        row['error'] = f'{type(exc).__name__}: {exc}'
        tmp = path + '.int32tmp'
        if os.path.exists(tmp):
            os.remove(tmp)
    return row


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('wav_dir', type=Path)
    ap.add_argument('--log', type=Path, required=True)
    ap.add_argument('--workers', type=int, default=max(1, (os.cpu_count() or 2) - 2))
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    files = sorted(str(p) for p in args.wav_dir.glob('[0-9][0-9][0-9][0-9]/[0-9][0-9]/*')
                   if p.is_file() and not p.name.startswith('.') and not p.name.endswith('.int32tmp'))
    print(f'{len(files)} files under {args.wav_dir}', flush=True)
    rows = []
    with ProcessPoolExecutor(args.workers) as pool:
        for i, row in enumerate(pool.map(convert, files, [args.dry_run] * len(files), chunksize=20), 1):
            rows.append(row)
            if i % 2000 == 0:
                print(f'  {i}/{len(files)}', flush=True)
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with args.log.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    counts: dict[str, int] = {}
    for r in rows:
        counts[r['status']] = counts.get(r['status'], 0) + 1
    before = sum(r['bytes_before'] for r in rows)
    after = sum(r['bytes_after'] or r['bytes_before'] for r in rows)
    print(counts, f'bytes {before / 1e9:.2f} GB -> {after / 1e9:.2f} GB', f'log: {args.log}', sep='\n')
    return 1 if counts.get('failed') or counts.get('not_integer_valued') else 0


if __name__ == '__main__':
    sys.exit(main())
