import unittest
import tempfile
from pathlib import Path
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
        from pinatubo_steps import step_23_plot_pick_event_diagnostics as s23,step_32_build_waveform_pick_events as s32
        from pinatubo_steps import step_51_event_numbers_in_complete_windows as s51,step_52_build_seisan_rea_catalog as s52
        from pinatubo_steps import step_50_build_obspy_catalog as s50
        self.assertIs(s23.to_dt,s32.to_dt);self.assertIs(s23.ensure_cols,s32.ensure_cols)
        self.assertIs(s51.get_comment_value,s52.get_comment_value)
        self.assertIs(s50.associate_one_to_one_by_time,shared.associate_one_to_one_by_time)

if __name__=='__main__':unittest.main()
