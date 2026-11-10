from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[2] / "10_conversion")]
import json,unittest
import numpy as np
import pandas as pd
from obspy import Trace
import hybrid_diagnostics as h
from pinatubo_project import test_directory

class HybridDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.cfg=json.loads((Path(h.__file__).parent/'hybrid_test_config.json').read_text())
    def signal(self,hf=4,lf=8):
        fs=100.;t=np.arange(4000)/fs;x=np.random.default_rng(15).normal(0,.1,len(t))
        if hf is not None:x+=3*np.exp(-.5*((t-hf)/.65)**2)*np.sin(2*np.pi*10*t)
        if lf is not None:x+=3*np.exp(-.5*((t-lf)/.8)**2)*np.sin(2*np.pi*2*t)
        return Trace(x,header={'sampling_rate':fs}),Trace(np.random.default_rng(55).normal(0,.1,1000),header={'sampling_rate':fs})
    def test_hf_then_lf_positive_lag_and_independent_if_transition(self):
        tr,n=self.signal();row,detail=h.analyse_hybrid_trace(tr,n,self.cfg)
        self.assertAlmostEqual(row['lf_minus_hf_peak_delay_s'],4,delta=.15)
        self.assertTrue(row['peak_delay_candidate']);self.assertTrue(row['if_candidate'])
        self.assertGreater(row['if_drop_hz'],2);self.assertIn('instantaneous_hz_raw',detail['hilbert'])
    def test_reverse_synchronous_and_single_band_not_peak_candidates(self):
        for hf,lf in [(8,4),(5,5),(5,None),(None,5),(None,None)]:
            with self.subTest(hf=hf,lf=lf):
                row,_=h.analyse_hybrid_trace(*self.signal(hf,lf),self.cfg)
                self.assertIsNot(row['peak_delay_candidate'],True)
    def test_gap_is_not_replaced_or_time_compressed(self):
        tr,n=self.signal();tr.data=np.ma.array(tr.data,mask=False);tr.data.mask[100:200]=True
        before=tr.copy();row,detail=h.analyse_hybrid_trace(tr,n,self.cfg)
        self.assertIsNone(row['peak_delay_candidate']);self.assertEqual(row['screen_class'],'not_testable')
        np.testing.assert_equal(tr.data.mask,before.data.mask)
    def test_threshold_is_explicit_and_input_not_mutated(self):
        tr,n=self.signal();before=tr.data.copy();cfg={**self.cfg,'peak_delay_range_s':[5,10]}
        row,_=h.analyse_hybrid_trace(tr,n,cfg);self.assertFalse(row['peak_delay_candidate'])
        np.testing.assert_equal(tr.data,before)
    def test_label_evaluation_does_not_count_unresolved_as_negative(self):
        scores=pd.DataFrame([dict(trace_key='a',peak_delay_candidate=True,if_candidate=True),
            dict(trace_key='b',peak_delay_candidate=None,if_candidate=False)])
        dest=test_directory('hybrid_unit')/'labels.csv'
        pd.DataFrame([dict(trace_key='a',is_hybrid=True),dict(trace_key='b',is_hybrid=False)]).to_csv(dest,index=False)
        report=h.evaluate_labels(scores,dest);peak=report[report.method.eq('peak_delay')].iloc[0]
        self.assertEqual(peak.resolved,1);self.assertEqual(peak.unresolved,1)
    def test_invalid_configuration_is_rejected(self):
        cfg={**self.cfg,'if_step_s':0}
        with self.assertRaises(ValueError):h.analyse_hybrid_trace(*self.signal(),cfg)
