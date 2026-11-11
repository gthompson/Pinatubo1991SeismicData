import unittest
import tempfile
from pathlib import Path
def _load_pipeline_notebook(filename):
    import json, types
    path = Path(__file__).resolve().parents[2] / filename
    namespace = {}
    for cell in json.loads(path.read_text())['cells']:
        if cell['cell_type'] == 'code' and 'pipeline_definitions' in cell.get('metadata', {}).get('tags', []):
            exec(compile(''.join(cell['source']), str(path), 'exec'), namespace)
    return types.SimpleNamespace(**namespace)

import pandas as pd
from pinatubo_steps import shared

class SharedTests(unittest.TestCase):
    def test_normalization_parity_and_missing(self):
        frame=pd.DataFrame([dict(seed_id=' XB.PI2..ELZ ',station='bad',channel='bad'),dict(station='CAB',channel='EHN'),dict(station=None,channel=pd.NA)])
        normalized=shared.normalize_seed_id_df(frame)
        self.assertEqual(list(normalized),['XB.PI2..ELZ','XB.CAB..EHN','XB...'])
        self.assertEqual(list(frame.apply(shared.normalize_seed_id,axis=1)),list(normalized))
        self.assertTrue(pd.isna(frame.iloc[1].seed_id))
    def test_columns_and_time(self):
        frame=pd.DataFrame({'a':[1]});result=shared.ensure_cols(frame,['a','b'])
        self.assertNotIn('b',frame);self.assertTrue(pd.isna(result.iloc[0].b))
        times=shared.to_dt(pd.Series(['1991-01-01','bad']))
        self.assertEqual(str(times.dt.tz),'UTC');self.assertTrue(pd.isna(times.iloc[1]))
    def test_savefig_creates_output_and_closes(self):
        import matplotlib.pyplot as plt
        with tempfile.TemporaryDirectory() as tmp:
            fig = plt.figure()
            plt.plot([0, 1], [0, 1])
            path = Path(tmp) / 'nested' / 'plot.png'
            shared.savefig(path)
            self.assertTrue(path.is_file())
            self.assertGreater(path.stat().st_size, 0)
            self.assertFalse(plt.fignum_exists(fig.number))
    def test_shared_import_identity(self):
        import json, types
        path32 = Path(__file__).resolve().parents[2] / '32_build_waveform_pick_events.ipynb'
        ns32 = {}
        for c in json.loads(path32.read_text())['cells']:
            source32 = ''.join(c.get('source', []))
            if c['cell_type'] == 'code' and 'def build_event_catalog(' in source32:
                exec(compile(source32, str(path32), 'exec'), ns32)
        s32 = types.SimpleNamespace(**ns32)
        import json, types
        notebook = Path(__file__).resolve().parents[2] / '23_plot_pick_event_diagnostics.ipynb'
        namespace = {}
        for cell in json.loads(notebook.read_text())['cells']:
            source = ''.join(cell.get('source', []))
            if cell['cell_type'] == 'code' and 'def load_pick_table(' in source:
                exec(compile(source, str(notebook), 'exec'), namespace)
        s23 = types.SimpleNamespace(**namespace)
        s51 = _load_pipeline_notebook('51_event_numbers_in_complete_windows.ipynb')
        s52 = _load_pipeline_notebook('52_build_seisan_rea_catalog.ipynb')
        s50 = _load_pipeline_notebook('50_build_obspy_catalog.ipynb')
        self.assertIs(s23.to_dt,s32.to_dt);self.assertIs(s23.ensure_cols,s32.ensure_cols)
        self.assertIs(s51.get_comment_value,s52.get_comment_value)
        self.assertIs(s50.associate_one_to_one_by_time,shared.associate_one_to_one_by_time)

if __name__=='__main__':unittest.main()
