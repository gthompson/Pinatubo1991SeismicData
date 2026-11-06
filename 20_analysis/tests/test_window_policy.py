from pathlib import Path
import unittest,json
from unittest.mock import patch
import numpy as np
import pandas as pd
import test_analysis_workflow as fixtures
import analysis_workflow as w

class WindowPolicyTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.WorkflowTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.ctx=self.fixture.ctx
    def policy(self,mode='fixed',fallback='fixed'):
        self.ctx['config']['windows']=dict(policy=mode,noise_seconds=5.,detected_guard_s=1.,detected_fallback=fallback,event_end='record_end')
    def test_fixed_exact_five_then_remaining_record_no_review_required(self):
        self.policy();windows,issues=w.select_analysis_windows(self.ctx)
        self.assertEqual(len(windows),4);self.assertTrue(issues.empty)
        for key,group in windows.groupby('trace_key'):
            noise=group[group.window.eq('noise')].iloc[0];event=group[group.window.eq('event')].iloc[0]
            self.assertEqual(noise.window_end_file_s-noise.window_start_file_s,5.)
            self.assertEqual(event.window_start_file_s,noise.window_end_file_s)
            self.assertEqual(event.window_end_file_s,40.)
            self.assertTrue(event.event_end_record_limited)
        self.assertFalse((self.ctx['out']/'window_review.csv').exists())
    def test_detected_applies_guard_and_fixed_fallback(self):
        self.policy('detected');inventory,_=w.build_inventory(self.ctx);keys=inventory[inventory.is_waveform].trace_key.to_list()
        proposals=pd.DataFrame([dict(trace_key=keys[0],noise_proposal_status='EXTENDED_NETWORK_SUPPORTED_REVIEW',
            proposed_noise_start_file_s=0.,proposed_noise_end_file_s=9.,selected_onset_file_s=10.,qc_flags=''),
            dict(trace_key=keys[1],noise_proposal_status='MINIMUM_ONLY_WEAK_OR_UNRESOLVED',
            proposed_noise_start_file_s=0.,proposed_noise_end_file_s=5.,selected_onset_file_s=np.nan,qc_flags='')])
        selected,issues=w.select_analysis_windows(self.ctx,proposals=proposals)
        first=selected[selected.trace_key.eq(keys[0])].set_index('window')
        self.assertEqual(first.loc['noise','window_end_file_s'],9.)
        self.assertEqual(first.loc['event','window_start_file_s'],10.)
        second=selected[selected.trace_key.eq(keys[1])]
        self.assertTrue(second.window_selection_method.eq('fixed_fallback').all())
        self.assertTrue(second.window_flags.str.contains('unresolved_detection').all())
        self.ctx['config']['windows']['detected_fallback']='skip'
        skipped,issues=w.select_analysis_windows(self.ctx,proposals=proposals)
        self.assertEqual(set(skipped.trace_key),{keys[0]});self.assertEqual(len(issues),1)
    def test_policy_runs_metrics_and_reuses_cache(self):
        self.policy();metrics,errors=w.compute_metrics(self.ctx)
        self.assertTrue(errors.empty,errors.to_string());self.assertEqual(len(metrics),4)
        self.assertTrue(metrics.window_policy.eq('fixed').all())
        self.assertTrue(metrics.loc[metrics.window.eq('event'),'event_end_record_limited'].all())
        with patch.object(w,'read',side_effect=AssertionError('Do not reread unchanged files')):
            reused,errors=w.compute_metrics(self.ctx)
        self.assertTrue(errors.empty);pd.testing.assert_frame_equal(metrics,reused)
    def test_policy_change_preserves_previous_versions(self):
        self.policy();w.compute_metrics(self.ctx);old=pd.read_csv(self.ctx['out']/'event_metrics_index.csv')
        self.ctx['config']['windows']['noise_seconds']=6.
        metrics,errors=w.compute_metrics(self.ctx);new=pd.read_csv(self.ctx['out']/'event_metrics_index.csv')
        self.assertTrue(errors.empty);self.assertTrue(new.status.eq('computed').all())
        self.assertTrue(all(Path(p).exists() for p in old.sidecar_path))
        self.assertTrue(metrics.loc[metrics.window.eq('noise'),'window_duration_s'].eq(6).all())
    def test_auto_metrics_not_invalidated_by_unrelated_manual_review_notes(self):
        self.policy();w.compute_metrics(self.ctx)
        (self.ctx['out']/'window_review.csv').write_text('review_status\nUNREVIEWED\n')
        metrics=w.load_metrics(self.ctx);self.assertEqual(len(metrics),4)
    def test_configuration_switch_preserves_archive_selection(self):
        path=self.fixture.base/'analysis_workflow.json';before=json.loads(path.read_text())['selection']
        w.configure_window_policy(self.fixture.base,policy='fixed')
        cfg=json.loads(path.read_text());self.assertEqual(cfg['selection'],before)
        self.assertEqual(cfg['windows']['policy'],'fixed')
    def test_short_records_do_not_get_padded_or_forced_event_windows(self):
        self.policy();inventory,_=w.build_inventory(self.ctx)
        inventory.loc[inventory.is_waveform,'duration_s']=6.
        inventory.to_csv(self.ctx['out']/'trace_inventory.csv',index=False)
        windows,issues=w.select_analysis_windows(self.ctx)
        self.assertEqual(set(windows.window),{'noise'});self.assertEqual(len(issues),2)

if __name__=='__main__':unittest.main()
