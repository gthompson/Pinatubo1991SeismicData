import unittest,tempfile,contextlib,io,pickle,shutil
from pathlib import Path
import pandas as pd
from obspy import UTCDateTime
from obspy.core.event import Catalog,Event,Origin,Pick,WaveformStreamID,Comment,ResourceIdentifier
from flovopy.core import vdap
from pinatubo_steps import step_20_parse_individual_phase_files as s20
from pinatubo_steps import step_40_build_hypocenter_index_pinatubo_all as s40
from pinatubo_steps import step_43_associate_hypocenters as s43
from pinatubo_steps import step_52_build_seisan_rea_catalog as s52

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

if __name__=='__main__':unittest.main()
