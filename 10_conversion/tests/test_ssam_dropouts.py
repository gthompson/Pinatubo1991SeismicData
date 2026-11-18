"""Step 62: SSAM rows with 0 in every band are dropouts and do not count as coverage."""
import json, re, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load_step62():
    ns = dict(Path=Path, pd=pd, np=np, re=re, BOB_CONFIG={}, CONFIG={})
    for c in json.loads((ROOT / '62_ssam.ipynb').read_text())['cells']:
        source = ''.join(c['source'])
        if c['cell_type'] == 'code' and 'def quality_frame(' in source:
            exec(compile(source, 'step62', 'exec'), ns)
    return ns


class FakeDataset:
    def __init__(self, data):
        self.data = data


def test_all_zero_rows_are_flagged_and_excluded():
    ns = load_step62()
    bands = [f'value_{i:02d}' for i in range(1, 17)]
    rows = [[0] * 16,            # dropout
            [1] + [0] * 15,      # dead channel: flagged near-zero, kept
            [5] * 16,            # real values
            [0] * 15 + [40]]     # one live broad band: real
    frame = pd.DataFrame(rows, columns=bands, dtype=float)
    frame['series'] = 'DON1'
    frame['recorded_time'] = pd.date_range('1991-07-01', periods=len(rows), freq='min')
    # 'other' has no clock rule, so the times are left as recorded.
    out, _ = ns['quality_frame'](FakeDataset(frame), 'other')
    assert out.qc_all_zero.tolist() == [True, False, False, False]
    assert out.qc_near_zero.tolist() == [False, True, False, False]
    assert out.qc_use_for_coverage.tolist() == [False, True, True, True]
    assert out[bands].equals(frame[bands])  # values are not altered
