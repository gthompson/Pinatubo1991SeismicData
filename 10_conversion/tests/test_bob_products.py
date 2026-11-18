"""BOB product fixes: window labels, auxiliary sources, rain carried forward, mudflow skipped files."""
import json, re, tempfile
from datetime import datetime
from pathlib import Path
import sys
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load(notebook, tag):
    ns = dict(Path=Path, pd=pd, np=np, re=re, datetime=datetime, BOB_CONFIG={}, CONFIG={},
              display=lambda *a: None)
    for c in json.loads((ROOT / notebook).read_text())['cells']:
        if tag in c.get('metadata', {}).get('tags', []):
            exec(compile(''.join(c['source']), notebook, 'exec'), ns)
    return ns


class FakeDataset:
    def __init__(self, data):
        self.data = data


def test_window_labels_and_auxiliary_sources():
    ns = load('61_rsam_and_events.ipynb', 'pipeline_definitions')
    assert ns['WINDOW_LABELS']['rsam']['label'] == 'end'
    ns['WINDOW_LABELS']['other'] = dict(seconds=600, label='end', evidence='test')
    frame = pd.DataFrame(dict(series=['CAB', 'TEST1'], value_01=[5.0, 7.0],
                              recorded_time=pd.to_datetime(['1991-07-01 00:10', '1991-07-01 00:10'])))
    out, _ = ns['quality_frame'](FakeDataset(frame), 'other')   # 'other' has no clock rule
    assert out.window_start.iloc[0] == pd.Timestamp('1991-07-01 00:00')
    assert out.window_end.iloc[0] == pd.Timestamp('1991-07-01 00:10')
    assert out.qc_auxiliary_source.tolist() == [False, True]
    assert out.qc_use_for_coverage.tolist() == [True, False]


def test_hourly_counts_are_centred():
    ns = load('67_counts.ipynb', 'pipeline_definitions')
    frame = pd.DataFrame(dict(series=['CAB_CNT.DAT', 'PIE_CNT.DAT'], value_01=[5.0, 7.0],
                              recorded_time=pd.to_datetime(['1991-07-01 10:30', '1991-07-01 00:00'])))
    ns['WINDOW_LABELS']['other'] = ns['WINDOW_LABELS']['counts']
    out, _ = ns['quality_frame'](FakeDataset(frame), 'other')   # 'other' has no clock rule
    assert out.window_start.iloc[0] == pd.Timestamp('1991-07-01 10:00')
    assert out.window_end.iloc[0] == pd.Timestamp('1991-07-01 11:00')
    assert pd.isna(out.window_start.iloc[1])   # daily PIE counts have no hourly window


def test_rain_carried_forward():
    ns = load('64_rain.ipynb', 'product_definitions')
    raw = [[-998, 951], [63, 951], [63, 951], [53, 951]]
    data = pd.DataFrame(dict(source_file='01AUG91.DAT', source_line=[1, 2, 3, 4]))
    for i, name in enumerate(['value_01', 'value_02']):
        data[name + '_raw'] = [r[i] for r in raw]
        data[name] = data[name + '_raw'].where(data[name + '_raw'] != -998)
    out = ns['mark_carried_forward'](data)
    assert out.value_01_carried.tolist() == [False, False, True, False]
    assert out.value_02_carried.tolist() == [False, True, True, True]


def test_mudflow_da_files_listed_as_gmt_copies():
    ns = load('65_mudflow.ipynb', 'product_definitions')
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp)
        (src / 'MARL').mkdir(); (src / 'PROGS').mkdir()
        (src / 'MARL' / '11AUG91.DAT').write_text('DATA CONVERTED FROM GMT TO LOCAL TIME\n11 AUG 91 00 51   12.    8.   19.\n')
        (src / 'PROGS' / '11AUG91.DA2').write_text('11 AUG 91 16 51   12.    8.   19.\n')
        (src / 'PROGS' / 'READ.ME').write_text('notes')
        skipped = ns['skipped_inputs'](src).set_index('source_file')
        assert skipped.loc['PROGS/11AUG91.DA2', 'duplicate_of'] == 'MARL'
        assert skipped.loc['PROGS/11AUG91.DA2', 'records_matched'] == 1
        assert skipped.loc['PROGS/READ.ME', 'reason'] == 'not a .DAT data file'


def test_ssam_rsam_lag_finds_clock_offsets():
    ns = load('69_bob_coverage_and_review.ipynb', 'clock_check_definitions')
    rng = np.random.default_rng(1)
    t = pd.date_range('1991-10-01', '1991-10-03', freq='1min', inclusive='left')
    signal = pd.Series(np.exp(np.cumsum(rng.normal(0, .08, len(t)))) * 200, index=t)
    rsam = signal.rolling(10).mean().resample('10min', label='right', closed='right').last().dropna()
    # Day 2: RSAM labels 8 h late (RSAM PC on local time). SSAM labels everywhere 3 min late.
    late = rsam.index >= pd.Timestamp('1991-10-02')
    rsam.index = rsam.index + pd.to_timedelta(np.where(late, 8, 0), unit='h')
    ssam_time = t + pd.Timedelta('3min')
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for product in ('rsam', 'ssam'):
            (root / product).mkdir()
        bands = {f'value_{i:02d}': signal.values / 10 for i in range(1, 11)}
        pd.DataFrame(dict(series='CAB1', recorded_time=ssam_time, qc_use_for_coverage=True, **bands)).to_csv(root / 'ssam' / 'ssam_records.csv', index=False)
        pd.DataFrame(dict(series='CABZ', recorded_time=rsam.index, value_01=rsam.values, qc_use_for_coverage=True)).to_csv(root / 'rsam' / 'rsam_records.csv', index=False)
        out = ns['ssam_rsam_lags'](root, pairs={('ssam', 'CAB1'): 'CABZ'}).set_index('date')
    # lag_min > 0: RSAM matches SSAM labelled later.
    assert out.loc[datetime(1991, 10, 1).date(), 'lag_min'] == 3
    assert out.loc[datetime(1991, 10, 2).date(), 'lag_min'] == 3 - 480
