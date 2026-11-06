from pathlib import Path
from tempfile import TemporaryDirectory
import json,sys,unittest
import numpy as np
import pandas as pd
from obspy import Trace,Stream,UTCDateTime
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import analysis_workflow as w

class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name);self.base=root/'20_analysis';self.base.mkdir();self.wav=root/'WAV';self.wav.mkdir()
        self.cfg=json.loads((Path(w.__file__).parent/'analysis_workflow.json').read_text())
        self.cfg['wav_root']=str(self.wav);self.cfg['selection']['explicit_files']=[]
        for i in range(2):
            rng=np.random.default_rng(7+i);x=rng.normal(size=4000);x[1000:2500]*=10
            tr=Trace(x,header=dict(network='XB',station='BUG' if i==0 else 'GRN',channel='EHZ',sampling_rate=100.,starttime=UTCDateTime(1991,6,3)+i*.2))
            path=self.wav/f'1991-06-03-000{i}-00M.TEST_001';Stream([tr]).write(str(path),format='MSEED',encoding='FLOAT64')
            self.cfg['selection']['explicit_files'].append(str(path))
        (self.base/'analysis_workflow.json').write_text(json.dumps(self.cfg))
        self.ctx=w.load_context(self.base)
    def prepare_review(self):
        inventory,_=w.build_inventory(self.ctx)
        rows=[]
        for row in inventory[inventory.is_waveform].to_dict('records'):
            rows.append(dict(trace_key=row['trace_key'],source_fingerprint=row['source_fingerprint'],review_status='UNREVIEWED',
                reviewer_notes='',accepted_noise_start_file_s=np.nan,accepted_noise_end_file_s=np.nan,
                accepted_signal_start_file_s=np.nan,accepted_signal_end_file_s=np.nan))
        review=pd.DataFrame(rows);review.to_csv(self.ctx['out']/'window_review.csv',index=False)
        return review
    def accept(self):
        review=self.prepare_review();key=review.trace_key.iloc[0]
        w.save_review_decisions(self.ctx,{key:dict(review_status='ACCEPTED',accepted_noise_start_file_s=0.,
            accepted_noise_end_file_s=5.,accepted_signal_start_file_s=10.,accepted_signal_end_file_s=25.,reviewer_notes='synthetic fixture')})
        return key
    def test_empty_acceptance_never_uses_proposals(self):
        self.prepare_review();metrics,errors=w.compute_reviewed_metrics(self.ctx)
        self.assertTrue(metrics.empty);self.assertEqual(list(metrics),w.METRIC_FIELDS)
    def test_half_open_cut_and_no_mutation(self):
        trace=Trace(np.arange(2000,dtype=float),header={'sampling_rate':100.});before=trace.copy()
        cut=w.cut_half_open(trace,.15,1.05,trace.stats.starttime)
        self.assertEqual(len(cut),90);self.assertEqual(cut.data[-1],104)
        np.testing.assert_array_equal(trace.data,before.data)
    def test_guard_overlap_rejected_and_review_preserved(self):
        self.accept();path=self.ctx['out']/'window_review.csv';before=path.read_bytes()
        key=pd.read_csv(path).trace_key.iloc[0]
        with self.assertRaises(ValueError):w.save_review_decisions(self.ctx,{key:dict(accepted_signal_start_file_s=5.5)})
        self.assertEqual(before,path.read_bytes())
    def test_source_fingerprint_mismatch_rejected(self):
        self.accept();frame=pd.read_csv(self.ctx['out']/'window_review.csv');frame.loc[0,'source_fingerprint']='changed'
        approved,issues=w.validate_review(self.ctx,frame,False)
        self.assertTrue(approved.empty);self.assertIn('source_fingerprint_mismatch',issues.reason.to_list())
    def test_incomplete_event_does_not_get_record_end(self):
        self.accept();frame=pd.read_csv(self.ctx['out']/'window_review.csv');frame.loc[0,'accepted_signal_end_file_s']=np.nan
        approved,issues=w.validate_review(self.ctx,frame,False)
        self.assertTrue(approved.empty);self.assertIn('event_incomplete_bounds',issues.reason.to_list())
    def test_both_windows_get_features_and_amplitudes(self):
        self.accept();metrics,errors=w.compute_reviewed_metrics(self.ctx)
        self.assertTrue(errors.empty);self.assertEqual(set(metrics.window),{'noise','event'})
        self.assertTrue(metrics['feature.spectral.power_peak_hz'].notna().all())
        self.assertTrue(metrics['amplitude.rms_counts'].gt(0).all())
        self.assertGreater(metrics.loc[metrics.window=='event','event_to_noise_rms_ratio'].iloc[0],5)
        self.assertTrue(metrics.loc[metrics.window=='noise','feature.full.threshold_onset_s'].isna().all())
        self.assertTrue(metrics['artifact_pre_status'].notna().all());self.assertTrue(metrics['artifact_post_status'].notna().all())
        self.assertEqual(metrics.psd_segment_s.nunique(),1)
    def test_changed_review_makes_metrics_stale(self):
        self.accept();w.compute_reviewed_metrics(self.ctx)
        path=self.ctx['out']/'window_review.csv';path.write_text(path.read_text()+'\n')
        with self.assertRaises(ValueError):w.load_metrics(self.ctx)
    def test_matched_density_psds_and_multitaper_normalization(self):
        fs=100;t=np.arange(10000)/fs;tr=Trace(3*np.sin(2*np.pi*6*t),header={'sampling_rate':fs})
        specs=w.matched_psds([tr,tr.copy()],self.cfg)
        for key in ['welch_psd','multitaper_psd']:
            spec=specs[0];power=np.trapezoid(spec[key],spec['frequency_hz'])
            self.assertAlmostEqual(power,4.5,delta=.15)
            self.assertLess(abs(spec['frequency_hz'][np.argmax(spec[key])]-6),1)
            np.testing.assert_equal(spec[key],specs[1][key])
        mt=w.multitaper_spectrogram(tr,2.56,.16,2.5)
        self.assertAlmostEqual(np.trapezoid(mt.values[:,0],mt.frequencies),4.5,delta=.15)
    def test_gaps_rejected_not_compressed(self):
        tr=Trace(np.ones(2000),header={'sampling_rate':100.});tr.data=np.ma.array(tr.data,mask=False);tr.data.mask[300:500]=True
        with self.assertRaises(ValueError):w.processed_copy(tr,self.cfg)
        mt=w.multitaper_spectrogram(tr,2.56,.16,2.5)
        self.assertTrue(np.isnan(mt.values[:,mt.coverage<1]).all())
    def test_not_testable_artifact_is_missing_not_false(self):
        result=w.narrowband_audit(np.ones(500),50.)
        self.assertTrue(np.isnan(result['detected']));self.assertNotEqual(result['status'],'ok')
    def test_noise_only_acceptance_is_valid(self):
        review=self.prepare_review();key=review.trace_key.iloc[0]
        w.save_review_decisions(self.ctx,{key:dict(review_status='ACCEPTED',accepted_noise_start_file_s=0,accepted_noise_end_file_s=5)})
        approved,issues=w.validate_review(self.ctx)
        self.assertEqual(approved.window.tolist(),['noise']);self.assertFalse(issues.severity.eq('error').any())
    def test_schema_complete_and_unique(self):
        schema=w.schema_table();self.assertEqual(len(schema),len(w.METRIC_FIELDS));self.assertFalse(schema.column.duplicated().any())

    def test_short_filtered_lf_summaries_are_missing_not_zero(self):
        self.accept();metrics,errors=w.compute_reviewed_metrics(self.ctx)
        noise=metrics[metrics.window.eq('noise')].iloc[0]
        self.assertTrue(np.isnan(noise['sam.lf_counts']))
        self.assertTrue(np.isnan(noise['sem.lf_counts2_s']))
        self.assertTrue(np.isfinite(noise['sam.hf_counts']))
        self.assertTrue(np.isfinite(noise['psd.lf_power_counts2']))

    def test_source_change_during_session_requires_new_context(self):
        w.build_inventory(self.ctx)
        path=Path(self.ctx['files'][0]);path.write_bytes(path.read_bytes()+b'changed')
        with self.assertRaises(ValueError):w.build_inventory(self.ctx)

    def test_frozen_publication_selection_is_not_silently_replaced(self):
        path=self.base/'publication_selection.json';path.write_text('{"run_id":"another_run"}')
        with self.assertRaises(ValueError):w.publication_figures(self.ctx)
        self.assertEqual(path.read_text(),'{"run_id":"another_run"}')

if __name__=='__main__':unittest.main()
