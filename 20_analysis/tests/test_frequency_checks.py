import unittest
import numpy as np
from obspy import Trace
from pretrigger_qc import Settings, analyse_trace, frequency_checks

class FrequencyTests(unittest.TestCase):
    def test_tone_frequency_and_crossings(self):
        fs=100;t=np.arange(2000)/fs;x=np.sin(2*np.pi*6*t)
        f,q=frequency_checks(x,fs,Settings())
        self.assertAlmostEqual(f.centroid_hz.median(),6,places=1)
        self.assertLess(abs(f.zero_crossing_hz.median()-6),.6)
        self.assertTrue(np.isnan(q['frequency_change_trace_s']))
    def test_frequency_only_change_without_amplitude_increase(self):
        fs=100;t=np.arange(3000)/fs;phase=2*np.pi*np.cumsum(np.where(t<10,3,12))/fs
        tr=Trace(np.sin(phase),header={'sampling_rate':fs})
        r=analyse_trace(tr)
        self.assertTrue(np.isnan(r['qc']['amplitude_onset_trace_s']))
        self.assertTrue(10<=r['qc']['frequency_change_trace_s']<=12)
        self.assertEqual(r['qc']['proposed_noise_end_trace_s'],5)
    def test_amplitude_increase_noise_guard(self):
        fs=100;t=np.arange(3000)/fs;x=np.sin(2*np.pi*6*t)*np.where(t<10,1,10)
        r=analyse_trace(Trace(x,header={'sampling_rate':fs}));q=r['qc']
        self.assertEqual(q['proposed_noise_end_trace_s'],q['amplitude_onset_trace_s']-1)
        self.assertGreaterEqual(q['proposed_noise_end_trace_s'],5)
        self.assertTrue(np.isnan(q['frequency_change_trace_s']))
    def test_guard_conflict_not_silently_forced(self):
        fs=100;t=np.arange(2000)/fs;x=np.sin(2*np.pi*6*t)*np.where(t<5.25,1,10)
        q=analyse_trace(Trace(x,header={'sampling_rate':fs}))['qc']
        self.assertTrue(np.isnan(q['proposed_noise_end_trace_s']))
        self.assertIn('CONFLICT',q['noise_proposal_status'])
    def test_frequency_gaps_remain_missing(self):
        fs=100;t=np.arange(2000)/fs;x=np.sin(2*np.pi*6*t);x[900:1100]=np.nan
        f,_=frequency_checks(x,fs,Settings())
        self.assertTrue(f.loc[(f.window_start_s<11)&(f.window_end_s>9),'centroid_hz'].isna().all())
if __name__=='__main__':unittest.main()
