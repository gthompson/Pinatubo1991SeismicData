import unittest,tempfile,contextlib,io,pickle,shutil
from pathlib import Path
def _load_pipeline_notebook(filename):
    import json, types
    path = Path(__file__).resolve().parents[2] / filename
    namespace = {}
    for cell in json.loads(path.read_text())['cells']:
        if cell['cell_type'] == 'code' and 'pipeline_definitions' in cell.get('metadata', {}).get('tags', []):
            exec(compile(''.join(cell['source']), str(path), 'exec'), namespace)
    return types.SimpleNamespace(**namespace)

import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import Catalog,Event,Origin,Pick,WaveformStreamID,Comment,ResourceIdentifier
from flovopy.core import vdap
# Exercise the implementation embedded in the canonical notebook.
import json, types
_notebook = Path(__file__).resolve().parents[2] / '20_parse_individual_phase_files.ipynb'
_namespace = {}
for _cell in json.loads(_notebook.read_text())['cells']:
    _source = ''.join(_cell.get('source', []))
    if _cell['cell_type'] == 'code' and 'def run(' in _source:
        exec(compile(_source, str(_notebook), 'exec'), _namespace)
s20 = types.SimpleNamespace(run=_namespace['run'])
_ns_s40 = {}
for _cell in json.loads((Path(__file__).resolve().parents[2] / '40_build_hypocenter_indexes.ipynb').read_text())['cells']:
    _source = ''.join(_cell.get('source', []))
    if _cell['cell_type'] == 'code' and 'def run_summary(' in _source:
        exec(compile(_source, '40_build_hypocenter_indexes.ipynb', 'exec'), _ns_s40)
s40 = types.SimpleNamespace(**_ns_s40)
s40.run = s40.run_summary
_ns_s43 = {}
for _cell in json.loads((Path(__file__).resolve().parents[2] / '43_associate_hypocenters.ipynb').read_text())['cells']:
    _source = ''.join(_cell.get('source', []))
    if _cell['cell_type'] == 'code' and 'def run(' in _source:
        exec(compile(_source, '43_associate_hypocenters.ipynb', 'exec'), _ns_s43)
s43 = types.SimpleNamespace(**_ns_s43)
s52 = _load_pipeline_notebook('52_build_seisan_rea_catalog.ipynb')

FIXTURES=Path(vdap.__file__).parents[1]/'tests/data/vdap'
class PipelineMigrationTests(unittest.TestCase):
    def test_individual_pha_adapter_real_input(self):
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            tmp=Path(tmp);raw=tmp/'PHA';raw.mkdir()
            shutil.copyfile(FIXTURES/'9106030M.PHA',raw/'9106030M.PHA')
            (raw/'9106030N.PHA').write_text('PI2L P PU0 910603125636.37\n')
            s20.run(pha_root=raw,out_csv=tmp/'picks.csv',error_log=tmp/'errors.log')
            table=pd.read_csv(tmp/'picks.csv')
            self.assertEqual(len(table),10)
            self.assertIn('XB.PI2..ELZ',set(table.seed_id))
            self.assertIn('XB.PPO..EHE',set(table.seed_id))
            self.assertTrue(table.raw_line.notna().all())

    def test_hypo_adapter_schema_and_empty(self):
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            tmp=Path(tmp)
            s40.run(summary_file=FIXTURES/'hypo71_summary_excerpt.sum',out_csv=tmp/'index.csv',error_log=tmp/'errors.log')
            table=pd.read_csv(tmp/'index.csv')
            self.assertEqual(len(table),5);self.assertIn('nass',table);self.assertIn('rms',table)
            self.assertEqual(table.iloc[0].source_line,1)
            empty=tmp/'empty.sum';empty.write_text('not a record\n')
            s40.run(summary_file=empty,out_csv=tmp/'empty.csv',error_log=tmp/'bad.log')
            self.assertTrue(pd.read_csv(tmp/'empty.csv').empty)

    def test_geodetics_association(self):
        self.assertFalse(hasattr(s43,'km_distance'))
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            tmp=Path(tmp)
            row=dict(origin_time='1991-06-03T12:00:00Z',latitude=15.,longitude=120.,depth_km=3.,magnitude=2.,source_file='sample',source_line=1)
            pd.DataFrame([row]).to_csv(tmp/'a.csv',index=False)
            pd.DataFrame([row]).to_csv(tmp/'b.csv',index=False)
            s43.run(hypo40=tmp/'a.csv',hypo41=tmp/'b.csv',time_tol=5.,dist_tol=15.,out_event_csv=tmp/'events.csv',out_origin_csv=tmp/'origins.csv')
            self.assertEqual(len(pd.read_csv(tmp/'events.csv')),1)
            self.assertEqual(len(pd.read_csv(tmp/'origins.csv')),2)

    def test_rea_adapter_metadata_reuse_and_source_immutability(self):
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            tmp=Path(tmp);time=UTCDateTime('1991-06-03T12:00:00')
            real=Event(resource_id=ResourceIdentifier('smi:local/test/real'),origins=[Origin(time=time,latitude=15,longitude=120,depth=3000)],comments=[Comment(text='event_class:H_ONLY')])
            pickonly=Event(resource_id=ResourceIdentifier('smi:local/test/pick'),picks=[Pick(time=time,waveform_id=WaveformStreamID(seed_string='XB.PI2..ELZ'))],comments=[Comment(text='event_class:P_ONLY')])
            catalog=Catalog(events=[real,pickonly]);source=tmp/'cat.pkl'
            with source.open('wb') as stream:pickle.dump(catalog,stream)
            before=source.read_bytes()
            result=s52.run(catalog_pkl=source,rea_dir=tmp/'REA',collision='shift',write_select=True)
            self.assertFalse(result['failed']);self.assertEqual(len(result['written']),2)
            texts=[path.read_text() for path in result['written']]
            self.assertTrue(any('ORIGIN_STATUS:REAL' in text for text in texts))
            self.assertTrue(any('ORIGIN_STATUS:DERIVED' in text for text in texts))
            self.assertTrue(any('HYPOCENTER_PRESENT:False' in text for text in texts))
            self.assertEqual(source.read_bytes(),before)
            again=s52.run(catalog_pkl=source,rea_dir=tmp/'REA',collision='shift',write_select=True)
            self.assertEqual(len(again['reused']),2,again)
            self.assertTrue(all(not path.name.endswith('_1') for path in again['reused']))

    def test_step43_equal_times_keep_input_order(self):
        # Two summary-file lines with the same time but different solutions: the first line is preferred,
        # on every run (an unstable sort could pick either).
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            tmp=Path(tmp)
            rows=[dict(origin_time='1991-07-03T08:45:35.87Z',latitude=15.15+0.001*i,longitude=120.3,depth_km=d,magnitude=1.,source_file='sum',source_line=i+1)
                  for i,d in enumerate([4.49,2.84])]
            rows += [dict(origin_time=f'1991-07-03T09:00:{i:02d}Z',latitude=15.,longitude=120.,depth_km=1.,magnitude=1.,source_file='sum',source_line=i+3) for i in range(40)]
            pd.DataFrame(rows).to_csv(tmp/'a.csv',index=False)
            pd.DataFrame(rows[:1]).to_csv(tmp/'b.csv',index=False)
            s43.run(hypo40=tmp/'a.csv',hypo41=tmp/'b.csv',time_tol=5.,dist_tol=15.,out_event_csv=tmp/'events.csv',out_origin_csv=tmp/'origins.csv')
            first=pd.read_csv(tmp/'events.csv').iloc[0]
            self.assertEqual(first.preferred_origin_id,'hypo40_0')
            self.assertEqual(first.preferred_depth_km,4.49)
            origins=pd.read_csv(tmp/'origins.csv')
            self.assertEqual(list(origins.origin_id[:3]),['hypo40_0','hypo40_1','pinaall_0'])

    def test_rea_presence_flags_follow_step50(self):
        # Step 50 counts a step-32 pick container as picks even when no pick object survives;
        # step 52 must then agree with EVENT_CLASS and report the picks written separately.
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            tmp=Path(tmp);time=UTCDateTime('1991-06-03T12:00:00')
            event=Event(resource_id=ResourceIdentifier('smi:local/test/w_p'),
                        comments=[Comment(text=f'waveform_starttime:{time}'),Comment(text='wavfile:1991-06-03-1200-00M.PNTBO_001'),
                                  Comment(text='event_class:W_P'),Comment(text='waveform_present:True'),
                                  Comment(text='picks_present:True'),Comment(text='hypocenter_present:False')])
            source=tmp/'cat.pkl'
            with source.open('wb') as stream:pickle.dump(Catalog(events=[event]),stream)
            result=s52.run(catalog_pkl=source,rea_dir=tmp/'REA',collision='shift')
            text=result['written'][0].read_text()
            self.assertIn('EVENT_CLASS:W_P ',text)
            self.assertIn('PICKS_PRESENT:True ',text)
            self.assertIn('N_PICKS:0 ',text)

    def test_rea_diagnostics_class_labels(self):
        s53 = _load_pipeline_notebook('53_seisan_rea_diagnostics.ipynb')
        line=lambda text:(' '+text).ljust(79)+'3'
        self.assertEqual(s53.get_event_class([line('event_class:W_P'),line('EVENT_CLASS:W_P_H')]),'W_P_H')
        self.assertEqual(s53.get_event_class([line('EVENT_CLASS:W+P')]),'W_P')
        self.assertEqual(s53.get_event_class([line('EVENT_CLASS:H')]),'H_ONLY')
        self.assertIsNone(s53.get_event_class([line('event_class:W_P')]))
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            tmp=Path(tmp);rea=tmp/'REA'/'1991'/'06';rea.mkdir(parents=True)
            for i,label in enumerate(['W_P_H','W_ONLY','H_ONLY']):
                (rea/f'03-1200-0{i}L.S199106').write_text('\n'.join([' 1991  6 3 1200 0'+str(i)+'.0 L'.ljust(79)+'1',line('EVENT_CLASS:'+label)])+'\n')
            import matplotlib; matplotlib.use('Agg')
            s53.run(rea_dir=tmp/'REA',out_dir=tmp/'out')
            table=pd.read_csv(tmp/'out'/'53_daily_counts.csv')
            self.assertTrue({'W_P_H','W_ONLY','H_ONLY'} <= set(table.columns))
            self.assertEqual(table.loc[0,['wave','picks','hypo']].tolist(),[2,1,2])

if __name__=='__main__':unittest.main()
