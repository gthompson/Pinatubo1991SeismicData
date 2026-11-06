import tempfile
from pathlib import Path
import unittest
import numpy as np
from obspy import Trace,Stream,UTCDateTime
from pretrigger_qc import Settings,analyse_trace,analyse_files,select_files

class PilotTests(unittest.TestCase):
    def trace(self,x):return Trace(np.array(x,dtype=float),header=dict(sampling_rate=100.,network='XB',station='BUG',channel='EHZ',starttime=UTCDateTime(1991,6,3)))
    def test_step_at_ten_seconds(self):
        x=np.random.default_rng(5).normal(size=3000);x[1000:]*=15
        r=analyse_trace(self.trace(x));q=r['qc']
        self.assertTrue(9.75<=q['amplitude_onset_trace_s']<=10.5)
        self.assertTrue(10<=q['sta_lta_onset_trace_s']<=11)
        self.assertEqual(q['trigger_status'],'NOT_MEASURED')
        self.assertEqual(q['reference_status'],'USER_ASSUMPTION_FIRST_5_SECONDS_NOISE')
    def test_metric_definitions_and_partial_tail(self):
        x=np.tile([-1,1],125);r=analyse_trace(self.trace(x));m=r['one_second']
        np.testing.assert_allclose(m.rsam[:2],1);np.testing.assert_allclose(m.rms[:2],1)
        self.assertFalse(m.iloc[-1].complete);self.assertTrue(np.isnan(m.iloc[-1].rms))
    def test_masks_not_silently_filled(self):
        tr=self.trace(np.tile([-1,1],1000));tr.data=np.ma.array(tr.data,mask=False);tr.data.mask[500:600]=True
        r=analyse_trace(tr);self.assertTrue(np.isnan(r['one_second'].iloc[5].rsam))
        self.assertIn('masked_or_nonfinite',r['qc']['qc_flags'])
    def test_flat_prefix_and_constant_unresolved(self):
        tr=self.trace(np.zeros(2000));r=analyse_trace(tr)
        self.assertEqual(r['qc']['flat_prefix_s'],20);self.assertTrue(np.isnan(r['qc']['amplitude_onset_trace_s']))
        self.assertIn('constant_trace',r['qc']['qc_flags'])
    def test_padding_flag(self):
        x=np.random.default_rng(9).normal(size=2000);x[:100]=0
        self.assertIn('possible_padding',analyse_trace(self.trace(x))['qc']['qc_flags'])
    def test_no_time_or_data_changes(self):
        tr=self.trace(np.random.default_rng(1).normal(size=3000));tr.data[100]=np.inf;before=tr.copy();analyse_trace(tr)
        np.testing.assert_equal(tr.data,before.data);self.assertEqual(tr.stats.starttime,before.stats.starttime)
    def test_review_preserved_and_irig_excluded(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);tr=self.trace(np.random.default_rng(3).normal(size=2000));irig=tr.copy();irig.stats.station='IRIG'
            path=root/'record';Stream([tr,irig]).write(str(path),format='MSEED',encoding='FLOAT64')
            r=analyse_files([path],root/'out');review=r['output']/'manual_window_review.csv'
            self.assertEqual(len(r['qc']),1);review.write_text('reviewed by human\n')
            rr=analyse_files([path],root/'out');self.assertEqual(review.read_text(),'reviewed by human\n')
            self.assertEqual(r['output'],rr['output'])
    def test_selection_spreads_recording_days(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'1991/06';p.mkdir(parents=True)
            for day in [1,2,3,4,5]:
                for f in range(3):(p/f'1991-06-{day:02d}-000{f}-00M.TEST_001').touch()
            selected,report=select_files(folder,['1991/06','1991/07'],2)
            self.assertEqual(len(selected),2);self.assertIn('-01-',selected[0].name);self.assertIn('-05-',selected[1].name)
            self.assertEqual(report.iloc[1].files_selected,0)
    def test_invalid_settings_and_empty_selection(self):
        with self.assertRaises(ValueError):Settings(reference_end_s=0)
        with self.assertRaises(ValueError):analyse_files([],'.')

if __name__=='__main__':unittest.main()
