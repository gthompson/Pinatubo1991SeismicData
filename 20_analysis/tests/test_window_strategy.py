from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import numpy as np
from obspy import Trace,Stream,UTCDateTime
from pretrigger_qc import Settings,analyse_files,analyse_trace

class WindowStrategyTests(unittest.TestCase):
    def tr(self,station,onset=None,late=None,channel='EHZ',seed=1):
        x=np.random.default_rng(seed).normal(size=8000)
        if onset is not None:x[int(onset*100):]*=10
        if late is not None:x[int(late*100):]*=10
        return Trace(x,header=dict(sampling_rate=100.,network='XB',station=station,channel=channel,starttime=UTCDateTime(1991,6,3)))
    def run_stream(self,traces):
        tmp=TemporaryDirectory();self.addCleanup(tmp.cleanup);root=Path(tmp.name)
        path=root/'event';Stream(traces).write(str(path),format='MSEED',encoding='FLOAT64')
        return analyse_files([path],root/'out')
    def test_clear_multi_station_event(self):
        r=self.run_stream([self.tr('AAA',10,seed=3),self.tr('BBB',11,seed=4)])
        self.assertEqual(r['network'].iloc[0].network_status,'candidate_cluster')
        self.assertEqual(r['network'].iloc[0].network_station_support,2)
        for q in r['qc'].itertuples():
            self.assertTrue(9<=q.selected_onset_file_s<=12)
            self.assertLessEqual(q.proposed_noise_end_file_s,q.selected_onset_file_s-1+1e-8)
            self.assertGreaterEqual(q.proposed_noise_end_file_s,5)
            self.assertEqual(q.noise_acceptance,'UNREVIEWED')
    def test_weak_station_late_excursion_falls_back_to_five(self):
        r=self.run_stream([self.tr('AAA',10,seed=3),self.tr('BBB',11,seed=4),self.tr('CAB',late=70,seed=5)])
        q=r['qc'].set_index('trace_id').loc['XB.CAB..EHZ']
        self.assertTrue(q.amplitude_onset_file_s>60)
        self.assertTrue(np.isnan(q.selected_onset_file_s));self.assertEqual(q.proposed_noise_end_file_s,5)
    def test_same_station_components_cannot_create_network(self):
        r=self.run_stream([self.tr('CAB',10,seed=3),self.tr('CAB',10,channel='EHN',seed=4)])
        self.assertEqual(r['network'].iloc[0].network_status,'unresolved')
        self.assertTrue((r['qc'].proposed_noise_end_file_s==5).all())
    def test_earlier_weak_rise_beats_later_strong_phase(self):
        early=self.tr('CCC',seed=6);early.data[850:1400]*=2.2;early.data[1400:]*=12
        r=self.run_stream([self.tr('AAA',9,seed=3),self.tr('BBB',9.5,seed=4),early])
        q=r['qc'].set_index('trace_id').loc['XB.CCC..EHZ']
        self.assertTrue(q.amplitude_onset_file_s>12)
        self.assertLess(q.selected_onset_file_s,10);self.assertLess(q.proposed_noise_end_file_s,9)
    def test_short_pulse_limits_noise_prefix(self):
        pulse=self.tr('CCC',10,seed=6);pulse.data[750]=50
        r=self.run_stream([self.tr('AAA',10,seed=3),self.tr('BBB',11,seed=4),pulse])
        q=r['qc'].set_index('trace_id').loc['XB.CCC..EHZ']
        self.assertLessEqual(q.proposed_noise_end_file_s,6.5)
        self.assertIn('earlier_',q.qc_flags)
    def test_initial_padding_is_not_valid_noise(self):
        bad=self.tr('CCC',10,seed=6);bad.data[:100]=0
        r=self.run_stream([self.tr('AAA',10,seed=3),self.tr('BBB',11,seed=4),bad])
        q=r['qc'].set_index('trace_id').loc['XB.CCC..EHZ']
        self.assertTrue(np.isnan(q.proposed_noise_end_file_s));self.assertEqual(q.noise_proposal_status,'INVALID_INITIAL_NOISE')
    def test_initial_pulse_is_flagged_despite_five_second_assumption(self):
        bad=self.tr('CCC',10,seed=6);bad.data[200]=50
        r=self.run_stream([self.tr('AAA',10,seed=3),self.tr('BBB',11,seed=4),bad])
        q=r['qc'].set_index('trace_id').loc['XB.CCC..EHZ']
        self.assertTrue(np.isnan(q.proposed_noise_end_file_s));self.assertIn('initial_reference_contains_large_transient',q.qc_flags)
    def test_minimum_and_guard_conflict_stays_invalid(self):
        r=self.run_stream([self.tr('AAA',5.25,seed=3),self.tr('BBB',5.25,seed=4)])
        self.assertTrue(r['qc'].proposed_noise_end_file_s.isna().all())
        self.assertTrue(r['qc'].noise_proposal_status.str.startswith('CONFLICT').all())
    def test_noise_only_record_is_unresolved(self):
        r=self.run_stream([self.tr('AAA',seed=3),self.tr('BBB',seed=4)])
        self.assertEqual(r['network'].iloc[0].network_status,'unresolved')
        self.assertTrue((r['qc'].proposed_noise_end_file_s==5).all())
    def test_hilbert_signed_noise_is_retained_and_smoothed(self):
        fs=100;t=np.arange(3000)/fs
        tr=Trace(np.cos(2*np.pi*5*t)+.8*np.cos(2*np.pi*12*t),header={'sampling_rate':fs})
        r=analyse_trace(tr);h=r['hilbert'];negative=(h.instantaneous_hz_raw<0)&~h.edge_excluded&~h.low_envelope
        self.assertTrue(negative.any());self.assertTrue(h.loc[negative,'instantaneous_hz_valid'].notna().all())
        noise=h[(h.time_trace_s>=1)&(h.time_trace_s<5)]
        self.assertGreater(noise.instantaneous_hz_smoothed.notna().mean(),.8)

if __name__=='__main__':unittest.main()
