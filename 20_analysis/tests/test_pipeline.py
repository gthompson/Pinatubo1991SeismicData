"""Run with: python -m pytest tests/test_pipeline.py (requires ObsPy/FloVoPy)."""
import json
from pathlib import Path

import numpy as np
from obspy import Trace, Stream, UTCDateTime
from pipeline import audit_trace, preprocess, process_file, _plain, write_csv

CONFIG = Path(__file__).resolve().parents[1] / 'config.example.json'

AUDIT = dict(target_hz=40.45, search_halfwidth_hz=0.6,
             background_halfwidth_hz=2.5, background_exclusion_hz=0.8,
             psd_ratio_threshold=10, welch_nperseg=2048)
FILTER = dict(detrend='linear', taper_fraction=0, lowpass_hz=25,
              lowpass_corners=6, zero_phase=True)


def test_artifact_detect_and_filter_preserve_original():
    fs = 100.
    t = np.arange(10000)/fs
    x = 20*np.sin(2*np.pi*40.45*t) + np.sin(2*np.pi*5*t)
    tr = Trace(data=x.copy(), header=dict(sampling_rate=fs, starttime=UTCDateTime(1991,6,1)))
    row = audit_trace(tr, AUDIT)
    assert row['status'] == 'ok' and row['detected']
    assert abs(row['peak_hz'] - 40.45) < 0.1
    out = preprocess(Stream([tr]), FILTER)
    assert np.array_equal(tr.data, x)
    assert np.std(out[0].data) < 0.2*np.std(x)


def test_nyquist_not_testable():
    tr = Trace(data=np.ones(1000), header={'sampling_rate': 50.})
    assert audit_trace(tr, AUDIT)['status'] == 'too_short_nyquist_or_nonfinite'


def pulses(fs=100, seconds=30):
    """12 Hz pulse at 8 s, then a 2 Hz pulse at 14 s."""
    t = np.arange(int(fs*seconds))/fs
    return np.exp(-.5*((t-8)/.7)**2)*np.sin(2*np.pi*12*t) + np.exp(-.5*((t-14)/.7)**2)*np.sin(2*np.pi*2*t)


def test_reusable_features_are_opt_in_and_leave_legacy_metrics_unchanged(tmp_path):
    cfg = json.loads(CONFIG.read_text())
    x = pulses() + np.random.default_rng(2).normal(0, .001, 3000)
    path = tmp_path/'test.mseed'
    Trace(x, header={'sampling_rate': 100, 'station': 'TEST', 'channel': 'EHZ'}).write(str(path), format='MSEED')
    _, old, q, _ = process_file(path, cfg)
    assert len(old) == 2, q
    cfg['processing']['compute_reusable_features'] = True
    _, new, q, _ = process_file(path, cfg)
    assert len(new) == 2, q
    assert 'features.values.hf_lf.peak_delay_s' in new[1]
    for a, b in zip(old, new):
        for k, v in a.items():
            if isinstance(v, float) and np.isnan(v):
                assert np.isnan(b[k])
            else:
                assert b[k] == v, k
    json.dumps(_plain(new), allow_nan=False)
    write_csv(tmp_path/'metrics.csv', new)
    assert 'features.version' in (tmp_path/'metrics.csv').read_text()
