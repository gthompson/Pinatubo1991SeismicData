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
