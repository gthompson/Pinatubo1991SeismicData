"""Tests of the local batch adapter (analyse_files). The detection engine is tested in FloVoPy."""
import tempfile
from pathlib import Path
import unittest
import numpy as np
from obspy import Trace,Stream,UTCDateTime
from pretrigger_qc import analyse_files

class AdapterTests(unittest.TestCase):
    def trace(self,x,station='BUG'):return Trace(np.array(x,dtype=float),header=dict(sampling_rate=100.,network='XB',station=station,channel='EHZ',starttime=UTCDateTime(1991,6,3)))
    def test_review_preserved_and_irig_excluded(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);tr=self.trace(np.random.default_rng(3).normal(size=2000));irig=tr.copy();irig.stats.station='IRIG'
            path=root/'record';Stream([tr,irig]).write(str(path),format='MSEED',encoding='FLOAT64')
            r=analyse_files([path],root/'out');review=r['output']/'manual_window_review.csv'
            self.assertEqual(len(r['qc']),1);review.write_text('reviewed by human\n')
            rr=analyse_files([path],root/'out');self.assertEqual(review.read_text(),'reviewed by human\n')
            self.assertEqual(r['output'],rr['output'])
    def test_empty_selection(self):
        with self.assertRaises(ValueError):analyse_files([],'.')
    def test_file_relative_columns(self):
        rng=[np.random.default_rng(s).normal(size=8000) for s in (3,4)]
        for x,onset in zip(rng,(10,11)):x[onset*100:]*=10
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);path=root/'event'
            Stream([self.trace(rng[0],'AAA'),self.trace(rng[1],'BBB')]).write(str(path),format='MSEED',encoding='FLOAT64')
            r=analyse_files([path],root/'out')
        self.assertEqual(r['network'].iloc[0].network_status,'candidate_cluster')
        for q in r['qc'].itertuples():
            self.assertTrue(9<=q.selected_onset_file_s<=12)
            self.assertLessEqual(q.proposed_noise_end_file_s,q.selected_onset_file_s-1+1e-8)
        self.assertFalse(any(c.endswith('_record_s') for c in r['qc'].columns))

if __name__=='__main__':unittest.main()
