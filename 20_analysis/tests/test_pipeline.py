"""Run with: python -m pytest tests/test_pipeline.py (requires ObsPy/FloVoPy)."""
import numpy as np
from obspy import Trace, Stream, UTCDateTime
from pipeline import audit_trace, preprocess

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
