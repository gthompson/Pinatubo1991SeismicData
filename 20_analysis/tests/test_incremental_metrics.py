from pathlib import Path
import json,sys,unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
from obspy import Stream
import test_analysis_workflow as fixtures
import analysis_workflow as w
from flovopy.enhanced.stream import EnhancedStream

class IncrementalMetricTests(unittest.TestCase):
    def setUp(self):
        fixture=fixtures.WorkflowTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        self.fixture=fixture;self.ctx=fixture.ctx;self.key=fixture.accept()
    def compute(self,ctx=None):
        ctx=self.ctx if ctx is None else ctx
        result,errors=w.compute_reviewed_metrics(ctx)
        self.assertTrue(errors.empty,errors.to_string());return result,pd.read_csv(ctx['out']/'event_metrics_index.csv')
    def test_unchanged_events_require_no_waveform_reads_or_metric_calls(self):
        first,index=self.compute();paths=index.sidecar_path.to_list();mtimes={p:Path(p).stat().st_mtime_ns for p in paths}
        with patch.object(w,'read',side_effect=AssertionError('Unchanged waveform must not be read')),patch.object(w,'EnhancedStream',side_effect=AssertionError('Unchanged metrics must not run')):
            second,index=self.compute()
        self.assertTrue(index.status.eq('reused').all());pd.testing.assert_frame_equal(first,second)
        self.assertEqual(mtimes,{p:Path(p).stat().st_mtime_ns for p in paths})
    def test_adding_file_reuses_existing_measurements_and_review(self):
        self.compute()
        from flovopy.processing.detection import PretriggerSettings
        detection=PretriggerSettings(compute_frequency=False,compute_hilbert=False)
        w.run_window_proposals(self.ctx,detection,max_detail_files=0)
        base=self.fixture.base;cfg=json.loads((base/'analysis_workflow.json').read_text())
        new=self.fixture.wav/'new.mseed';original=self.ctx['files'][1]
        Stream(w.waveform_traces(w.read(str(original)))).write(str(new),format='MSEED',encoding='FLOAT64')
        cfg['selection']['explicit_files'].append(str(new));(base/'analysis_workflow.json').write_text(json.dumps(cfg));ctx=w.load_context(base)
        real_read=w.read;calls=[]
        def tracked(path,*args,**kwargs):calls.append(str(path));return real_read(path,*args,**kwargs)
        with patch.object(w,'read',side_effect=tracked),patch.object(w,'EnhancedStream',side_effect=AssertionError('Old features must be reused')):
            metrics,index=self.compute(ctx)
            proposals,_=w.run_window_proposals(ctx,detection,max_detail_files=0)
        self.assertEqual(json.loads((proposals['output']/'progress.json').read_text())['reused'],2)
        self.assertEqual(index.status.eq('reused').sum(),2);self.assertEqual(index.status.eq('metadata_only').sum(),1)
        self.assertEqual([str(Path(path).resolve()) for path in calls],[str(new.resolve())]);self.assertEqual(set(metrics.trace_key),{self.key})
    def test_only_changed_accepted_windows_are_recomputed(self):
        _,old=self.compute();w.save_review_decisions(self.ctx,{self.key:dict(accepted_signal_end_file_s=24.)})
        _,index=self.compute();self.assertEqual(index.status.eq('computed').sum(),1);self.assertEqual(index.status.eq('reused').sum(),1)
        changed=index[index.status.eq('computed')].record_id.iloc[0]
        self.assertNotEqual(index.set_index('record_id').loc[changed,'sidecar_path'],old.set_index('record_id').loc[changed,'sidecar_path'])
        self.assertTrue(all(Path(p).exists() for p in old.sidecar_path))
    def test_notes_refresh_without_recomputing_features(self):
        self.compute();w.save_review_decisions(self.ctx,{self.key:dict(reviewer_notes='Updated interpretation only')})
        with patch.object(w,'EnhancedStream',side_effect=AssertionError('Notes must not trigger numerical work')):
            _,index=self.compute()
        self.assertTrue(index.status.eq('reused').all())
    def test_every_new_metric_and_numeric_spectrum_is_retained(self):
        original=EnhancedStream.ampengfft
        def extra(stream,*args,**kwargs):
            original(stream,*args,**kwargs)
            for tr in stream:tr.stats.metrics['future_metric']={'values':np.array([0.,2.,3.])}
        with patch.object(EnhancedStream,'ampengfft',extra):_,index=self.compute()
        record=index[index.status.eq('computed')].iloc[0];payload=json.loads(Path(record.sidecar_path).read_text())
        trace=payload['traces'][0]
        self.assertEqual(trace['enhancedstream']['metrics']['future_metric']['values'],[0.,2.,3.])
        self.assertIsInstance(trace['enhancedstream']['spectral']['freqs'],list)
        self.assertIsInstance(trace['features']['values'],dict);self.assertIn('provenance',trace['features'])
        self.assertIn('station_metrics',trace['enhancedstream']);self.assertIn('welch_psd',trace['psd'])
        full=pd.read_csv(self.ctx['out']/'all_window_metrics.csv')
        self.assertEqual(json.loads(full['trace.enhancedstream.metrics.future_metric.values'].iloc[0]),[0.,2.,3.])
    def test_ensemble_exports_rebuild_from_json_without_measurements(self):
        metrics,_=self.compute()
        with patch.object(w,'read',side_effect=AssertionError('Export must not read waveforms')):
            again=w.export_event_metrics(self.ctx)
        pd.testing.assert_frame_equal(metrics,again)
        self.assertTrue((self.ctx['out']/'events.qml').exists())
    def test_corrupted_cache_is_rebuilt_for_that_event(self):
        _,index=self.compute();path=Path(index[index.status.eq('computed')].sidecar_path.iloc[0]);path.write_text('{corrupt')
        _,index=self.compute();self.assertEqual(index.status.eq('computed').sum(),1)
        self.assertIn('traces',json.loads(path.read_text()))

if __name__=='__main__':unittest.main()
