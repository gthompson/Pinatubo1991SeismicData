from pathlib import Path
from tempfile import TemporaryDirectory
import unittest,json,sys
import numpy as np
import pandas as pd
from obspy import Trace,Stream,UTCDateTime
from flovopy.processing.detection import PretriggerSettings
import analysis_workflow as w

class TimeplotTests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name)/'20_analysis';self.base.mkdir();self.wav=self.base/'WAV';self.wav.mkdir()
        cfg=json.loads((Path(w.__file__).parent/'analysis_workflow.json').read_text());cfg['products_directory']=str(self.base/'products');cfg['wav_root']=str(self.wav)
        cfg['selection']=dict(mode='sample',months=['1991/05','1991/06'],files_per_month=3,explicit_files=[],max_files=None)
        for month in ['1991/05','1991/06']:
            folder=self.wav/month;folder.mkdir(parents=True)
            for i in range(6):
                traces=[]
                for j,station in enumerate(['BUG','GRN']):
                    x=np.random.default_rng(i*3+j).normal(size=2500);x[1000:]*=10
                    traces.append(Trace(x,header=dict(network='XB',station=station,channel='EHZ',sampling_rate=100.,starttime=UTCDateTime(1991,int(month[-2:]),1)+i*3600)))
                Stream(traces).write(str(folder/f'1991-{month[-2:]}-01-{i:02d}00-00M.TEST_002'),format='MSEED',encoding='FLOAT64')
        (self.base/'analysis_workflow.json').write_text(json.dumps(cfg));self.ctx=w.load_context(self.base)
    def test_sample_takes_several_files_per_day_and_spreads_each_month(self):
        self.assertEqual(len(self.ctx['files']),6)
        names=[p.name for p in self.ctx['files']]
        self.assertTrue(any('-0000-' in name for name in names));self.assertTrue(any('-0500-' in name for name in names))
    def test_cap_preserves_both_months(self):
        w.configure_selection(self.base,mode='sample',files_per_month=6,months=['1991/05','1991/06'],max_files=4)
        ctx=w.load_context(self.base);self.assertEqual(len(ctx['files']),4)
        self.assertEqual({p.parent.name for p in ctx['files']},{'05','06'})
    def test_cache_resume_retains_only_requested_examples_and_preserves_review(self):
        cfg=PretriggerSettings(compute_frequency=False,compute_hilbert=False)
        result,review=w.run_window_proposals(self.ctx,cfg,max_detail_files=1,progress_every=10)
        self.assertEqual(len(result['details']),1);self.assertEqual(len(result['qc']),12)
        w.save_review_decisions(self.ctx,{review.trace_key.iloc[0]:dict(reviewer_notes='keep this note')})
        # No retained examples: every file comes from its completed cache.
        again,saved=w.run_window_proposals(self.ctx,cfg,max_detail_files=0,progress_every=10)
        self.assertEqual(saved.loc[0,'reviewer_notes'],'keep this note');self.assertEqual(len(again['details']),0)
        self.assertEqual(json.loads((again['output']/'progress.json').read_text())['reused'],6)
        self.assertTrue((again['output']/'file_cache_index.csv').exists())
    def test_fallback_excluded_offsets_removed_components_get_one_vote(self):
        inventory,_=w.build_inventory(self.ctx);base=inventory[inventory.is_waveform].head(3).copy()
        # Modify inventory metadata for this numerical summary fixture only.
        base.loc[base.index[0],['station','network']]=['BUG','XB']
        base.loc[base.index[1],['station','network']]=['BUG','XB']
        base.loc[base.index[2],['station','network']]=['GRN','XB']
        base.to_csv(self.ctx['out']/'trace_inventory.csv',index=False)
        rows=[]
        for i,meta in enumerate(base.to_dict('records')):
            rows.append(dict(trace_key=meta['trace_key'],record_id='one_file',trace_index=i,
                file_reference_recorded_time='1991-05-01T00:00:00Z',proposed_noise_start_trace_s=0.,
                proposed_noise_end_trace_s=[8.,10.,5.][i],selected_onset_file_s=[10.,12.,np.nan][i],
                noise_proposal_status='MINIMUM_ONLY_WEAK_OR_UNRESOLVED' if i==2 else 'EXTENDED_NETWORK_SUPPORTED_REVIEW'))
        table,events=w.noise_window_time_tables(self.ctx,pd.DataFrame(rows))
        self.assertEqual(events.supported_stations.iloc[0],1)
        self.assertEqual(events.noise_station_median_s.iloc[0],9.)
        self.assertEqual(events.minimum_only_traces.iloc[0],1)
        self.assertFalse(table.supported_noise_proposal.iloc[2])
        fig,_,_=w.plot_noise_windows_through_time(self.ctx,pd.DataFrame(rows))
        __import__('matplotlib.pyplot',fromlist=['close']).close(fig)
        self.assertTrue((self.ctx['out']/'figures/noise_windows_through_time.pdf').exists())
    def test_config_change_backs_up_prior_selection(self):
        before=(self.base/'analysis_workflow.json').read_bytes()
        w.configure_selection(self.base,mode='all',files_per_month=50)
        backups=list(__import__('pinatubo_project').output_path('20_analysis','config_backups').glob('analysis_workflow.before_'+w.digest(before)[:12]+'.json'));self.assertEqual(len(backups),1)
        self.assertEqual(backups[0].read_bytes(),before)
        self.assertEqual(len(w.load_context(self.base)['files']),12)

if __name__=='__main__':unittest.main()
