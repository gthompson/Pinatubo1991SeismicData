#!/usr/bin/env python3
"""Generic event-waveform metrics pipeline using ObsPy and FloVoPy.

Reads SEISAN WAV archives or arbitrary waveform files, audits acquisition
artifacts on ORIGINAL data, filters a COPY using ObsPy Stream.filter(), and
computes noise/event-window EnhancedStream metrics in uncalibrated counts.

See README.md and config.example.json. Requires obspy, numpy, scipy, pandas,
flovopy (with EnhancedStream.ampengfft(allow_uncalibrated=True)).
"""
from __future__ import annotations
import argparse
import csv
import glob
import json
import logging
import math
import re
import sys
from pathlib import Path
from collections.abc import Mapping

import numpy as np
from scipy.signal import detrend as scipy_detrend, welch
from obspy import UTCDateTime, read, Stream
from flovopy.enhanced.stream import EnhancedStream
from temporal_features import extract_temporal_features

LOG = logging.getLogger('event_metrics')


def audit_trace(tr, cfg):
    """PSD-based screening of a narrowband artifact on the unfiltered trace.

    PSD ratio is a screening statistic, not a physical calibration or proof of
    instrumental origin. A result can be 'not_testable' when Nyquist/resolution
    is inadequate. Do not conflate not_testable with artifact absent.
    """
    fs = float(tr.stats.sampling_rate)
    x = np.asarray(tr.data, dtype=float)
    target = float(cfg['target_hz'])
    hw = float(cfg['search_halfwidth_hz'])
    row = dict(status='not_testable', detected='', peak_hz='',
               peak_psd='', background_psd='', psd_ratio='',
               power_fraction_search='', nperseg='')
    if len(x) < 16 or fs <= 2 * (target + hw) or not np.all(np.isfinite(x)):
        row['status'] = 'too_short_nyquist_or_nonfinite'
        return row
    nperseg = min(int(cfg.get('welch_nperseg', 2048)), len(x))
    x = scipy_detrend(x, type='linear')
    f, p = welch(x, fs=fs, nperseg=nperseg, detrend='constant', scaling='density')
    search = (f >= target - hw) & (f <= target + hw)
    bg_hw = float(cfg.get('background_halfwidth_hz', 2.5))
    exclusion = float(cfg.get('background_exclusion_hz', 0.8))
    background = (f >= target-bg_hw) & (f <= target+bg_hw) & (np.abs(f-target) >= exclusion)
    if not np.any(search) or np.count_nonzero(background) < 3:
        row['status'] = 'insufficient_frequency_bins'
        return row
    k = np.flatnonzero(search)[np.argmax(p[search])]
    peak, bg = float(p[k]), float(np.median(p[background]))
    total = float(np.sum(p))
    row.update(status='ok', detected=bool(peak / max(bg, 1e-30) >= float(cfg['psd_ratio_threshold'])),
               peak_hz=float(f[k]), peak_psd=peak, background_psd=bg,
               psd_ratio=peak/max(bg, 1e-30),
               power_fraction_search=float(np.sum(p[search])/total) if total > 0 else '',
               nperseg=nperseg)
    return row


def preprocess(stream, cfg):
    """Return a processed COPY; use ObsPy Stream.filter for low-pass."""
    out = stream.copy()
    if cfg.get('detrend'):
        out.detrend(cfg['detrend'])
    fraction = float(cfg.get('taper_fraction', 0))
    if fraction:
        out.taper(max_percentage=fraction, type='cosine')
    cutoff = cfg.get('lowpass_hz')
    if cutoff is not None:
        cutoff = float(cutoff)
        for tr in out:
            fs = float(tr.stats.sampling_rate)
            if cutoff <= 0 or cutoff >= fs/2:
                raise ValueError(f'{tr.id}: lowpass {cutoff} Hz incompatible with Nyquist {fs/2} Hz')
        out.filter('lowpass', freq=cutoff, corners=int(cfg.get('lowpass_corners', 6)),
                   zerophase=bool(cfg.get('zero_phase', True)))
    return out


def _plain(value):
    """Make ObsPy/NumPy nested metrics JSON serializable."""
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k,v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, np.ndarray):
        return _plain(value.tolist())
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, UTCDateTime):
        return str(value)
    if isinstance(value, (bool, str, int)) or value is None:
        return value
    return str(value)


def flatten(obj, prefix=''):
    """Flatten nested metric dicts into stable dotted column names."""
    result = {}
    for key, val in obj.items():
        name = f'{prefix}.{key}' if prefix else str(key)
        if isinstance(val, Mapping):
            result.update(flatten(val, name))
        elif isinstance(val, (list, tuple)):
            result[name] = json.dumps(_plain(val))
        else:
            result[name] = _plain(val)
    return result


def measure_window(trace, begin, end, label, cfg):
    """Extract a window from the filtered full trace; compute EnhancedStream metrics."""
    if end <= begin:
        return None, 'empty_window'
    cut = trace.copy().trim(begin, end, nearest_sample=True)
    if cut.stats.npts < 2 or (cut.stats.npts-1)*cut.stats.delta < float(cfg['minimum_window_seconds']):
        return None, 'window_too_short'
    cut.stats.units = cfg.get('units', 'counts')
    try:
        est = EnhancedStream(Stream(traces=[cut]))
        est.ampengfft(allow_uncalibrated=True,
                      compute_spectral=bool(cfg.get('compute_spectral', True)),
                      compute_sam=bool(cfg.get('compute_sam', True)),
                      compute_sem=bool(cfg.get('compute_sem', True)),
                      compute_bandratios=bool(cfg.get('compute_bandratios', True)),
                      bands=tuple(cfg.get('bands_hz', [0.8, 4., 4., 20.])))
        metrics = flatten(_plain(est[0].stats.get('metrics', {})))
        metrics.update(window=label, window_start=str(cut.stats.starttime),
                       window_end=str(cut.stats.endtime), window_npts=int(cut.stats.npts),
                       window_duration_s=float((cut.stats.npts-1)*cut.stats.delta))
        return metrics, 'ok'
    except Exception as exc:
        return None, f'{type(exc).__name__}: {exc}'


def iter_files(config):
    inp = config['input']
    mode = inp.get('mode', 'files')
    if mode == 'seisan':
        # Use the user's actual SEISAN archive interface when installed.
        # Fallback to direct YYYY/MM WAV directory traversal, avoiding any
        # assumption that SEISAN files carry a .mseed suffix.
        root = Path(inp['seisan_root'])
        db = inp['database']
        start = UTCDateTime(inp['start']); end = UTCDateTime(inp['end'])
        if end <= start:
            raise ValueError('input.end must be later than input.start')
        y, m = start.year, start.month
        seen = set()
        while (y, m) <= (end.year, end.month):
            folder = root / 'WAV' / db / str(y) / f'{m:02d}'
            if folder.is_dir():
                for path in sorted(folder.iterdir()):
                    if not path.is_file():
                        continue
                    # Filename time used for screening; actual trace times
                    # are checked in process_file().
                    match = re.match(r'(\d{4})-(\d{2})-(\d{2})-(\d{4})-(\d{2})', path.name)
                    if match:
                        yy,mm,dd,hhmm,ss = match.groups()
                        try:
                            t = UTCDateTime(int(yy),int(mm),int(dd),int(hhmm[:2]),int(hhmm[2:]),int(ss))
                            if not (start <= t < end):
                                continue
                        except Exception:
                            pass
                    if path not in seen:
                        seen.add(path)
                        yield path
            m += 1
            if m == 13: y,m = y+1,1
    elif mode == 'files':
        patterns = inp.get('paths', [])
        seen = set()
        for pattern in patterns:
            for s in sorted(glob.glob(str(pattern), recursive=True)):
                path = Path(s)
                if path.is_file() and path not in seen:
                    seen.add(path)
                    yield path
    else:
        raise ValueError(f'Unknown input.mode {mode!r}')


def process_file(path, config):
    p = config['processing']; a = config['artifact_audit']
    original = read(str(path))
    processed = Stream()
    audit_rows, metric_rows, qc_rows = [], [], []
    # Process traces independently: a problematic channel must not abort others.
    for index, tr in enumerate(original):
        base = dict(file=str(path), trace_index=index, trace_id=tr.id,
                    starttime=str(tr.stats.starttime), endtime=str(tr.stats.endtime),
                    sampling_rate=float(tr.stats.sampling_rate), npts=int(tr.stats.npts))
        try:
            if a.get('enabled', True):
                audit_rows.append({**base, **audit_trace(tr, a)})
            if tr.stats.npts < 16 or not np.all(np.isfinite(tr.data)):
                raise ValueError('trace too short or nonfinite')
            filtered = preprocess(Stream(traces=[tr]), p)[0]
            processed.append(filtered)
            start = filtered.stats.starttime
            noise_end = start + float(p['noise_seconds']) - filtered.stats.delta
            event_start = start + float(p.get('event_start_seconds', p['noise_seconds']))
            guard = float(p.get('noise_guard_seconds', 0))
            windows = [('noise', start, noise_end - guard),
                       ('event', event_start, filtered.stats.endtime)]
            results = {}
            for label, t0, t1 in windows:
                metrics, status = measure_window(filtered, t0, t1, label, p)
                qc_rows.append({**base, 'window':label, 'status':status})
                if metrics is not None:
                    metric_rows.append({**base, **metrics})
                    results[label] = metrics
            if p.get('compute_temporal_features', True) and 'event' in results:
                try:
                    event_trace=filtered.copy().trim(event_start,filtered.stats.endtime,nearest_sample=True)
                    noise_trace=filtered.copy().trim(start,noise_end-guard,nearest_sample=True) if 'noise' in results else None
                    l1,l2,h1,h2=map(float,p.get('bands_hz',[.8,4.,4.,20.]))
                    tc=p.get('temporal_features',{})
                    tm=extract_temporal_features(event_trace,noise_trace,
                        bands={'lf':(l1,l2),'hf':(h1,h2)},
                        smoothing_seconds=float(tc.get('smoothing_seconds',.25)),
                        snr_threshold=float(tc.get('snr_threshold',3.)),
                        filter_corners=int(tc.get('filter_corners',4)),
                        early_seconds=float(tc.get('early_seconds',2.)),
                        transition_min_seconds=float(tc.get('transition_min_seconds',.5)))
                    results['event'].update(tm)
                    # Update the correct event row, even when other traces/windows are present.
                    for row in reversed(metric_rows):
                        if row.get('trace_index')==index and row.get('window')=='event':
                            row.update(tm)
                            break
                except Exception as exc:
                    qc_rows.append({**base,'window':'temporal_features','status':f'ERROR: {type(exc).__name__}: {exc}'})
            if 'noise' in results and 'event' in results:
                n = results['noise'].get('sample_rms')
                e = results['event'].get('sample_rms')
                if n is not None and e is not None and n > 0:
                    ratio = float(e/n)
                    metric_rows[-1]['event_to_noise_rms_ratio'] = ratio
                    metric_rows[-1]['event_to_noise_rms_db'] = 20*np.log10(ratio) if ratio>0 else None
        except Exception as exc:
            LOG.exception('Trace failed: %s %s', path, tr.id)
            qc_rows.append({**base, 'window':'trace', 'status':f'ERROR: {exc}'})
    return audit_rows, metric_rows, qc_rows, processed


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = sorted({key for row in rows for key in row})
    with path.open('w', newline='') as fh:
        if columns:
            writer = csv.DictWriter(fh, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)


def run(config, max_files=None):
    out = Path(config['output']['directory']).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    (out/'config_used.json').write_text(json.dumps(config, indent=2)+'\n')
    audit_rows, metrics_rows, qc_rows, file_rows = [], [], [], []
    files = iter_files(config)
    for i, path in enumerate(files):
        if max_files is not None and i >= max_files:
            break
        LOG.info('Processing %s', path)
        try:
            a, m, q, processed = process_file(path, config)
            audit_rows.extend(a); metrics_rows.extend(m); qc_rows.extend(q)
            file_rows.append(dict(file=str(path), status='ok', traces=len(processed), metrics_rows=len(m)))
            if config['output'].get('save_filtered_mseed') and len(processed):
                dest = out/'filtered_mseed'/f'{i:06d}_{path.name}.mseed'
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.resolve() == path.resolve():
                    raise RuntimeError('Refusing to overwrite source')
                processed.write(str(dest), format='MSEED', encoding='FLOAT64')
        except Exception as exc:
            LOG.exception('File failed: %s', path)
            file_rows.append(dict(file=str(path), status=f'ERROR: {exc}'))
    write_csv(out/'artifact_audit.csv', audit_rows)
    write_csv(out/'trace_window_metrics.csv', metrics_rows)
    write_csv(out/'qc_log.csv', qc_rows)
    write_csv(out/'files_processed.csv', file_rows)
    LOG.info('Finished: %s files; %s metric rows; %s artifact rows',len(file_rows),len(metrics_rows),len(audit_rows))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--config', required=True, help='JSON configuration')
    ap.add_argument('--max-files', type=int, default=None, help='Pilot run limit')
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    config = json.loads(Path(args.config).read_text())
    output = run(config, max_files=args.max_files)
    print('Results:', output)


if __name__ == '__main__':
    main()
