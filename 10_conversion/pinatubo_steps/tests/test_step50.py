import importlib.util,unittest,tempfile,contextlib,io,json,math
from pathlib import Path
import pandas as pd
import numpy as np
import nbformat
from obspy import read_events
root=Path(__file__).resolve().parents[2]
path=root/'pinatubo_steps/step_50_build_obspy_catalog.py'
import sys
sys.path.insert(0, str(root))
m = importlib.import_module('pinatubo_steps.step_50_build_obspy_catalog')
base=pd.Timestamp('1991-06-03T12:00:00Z')
def wave(times,ids=None):return pd.DataFrame({'event_id':ids or [f'w{i}' for i in range(len(times))],'event_time':[base+pd.Timedelta(seconds=t) for t in times]})
def hypo(times):return pd.DataFrame({'event_id':[f'h{i}' for i in range(len(times))],'preferred_origin_time':[base+pd.Timedelta(seconds=t) for t in times]})
class Tests(unittest.TestCase):
 def test_competing(self):
  w,h=wave([0,1]),hypo([0])
  a=m.associate_one_to_one_by_time(w,h,tolerance_seconds=2);b=m.associate_nearest_by_time(w,h,2)
  self.assertEqual(len(a),1);self.assertEqual(len(b),2)
  self.assertFalse(a.event_id_h.duplicated().any())
 def test_boundaries_and_no_match(self):
  for fn in (m.associate_one_to_one_by_time,m.associate_nearest_by_time):
   self.assertEqual(len(fn(wave([0]),hypo([2]),tolerance_seconds=2)),1)
   self.assertTrue(fn(wave([0]),hypo([2.01]),tolerance_seconds=2).empty)
   self.assertTrue(fn(wave([]),hypo([0])).empty)
   self.assertTrue(fn(wave([0]),hypo([])).empty)
 def test_ties(self):
  a=m.associate_one_to_one_by_time(wave([0,0],['b','a']),hypo([0]))
  self.assertEqual(a.iloc[0].event_id_w,'a')
  b=m.associate_one_to_one_by_time(wave([0,0],['a','b']),hypo([0]))
  pd.testing.assert_frame_equal(a,b)
 def test_bad_arguments(self):
  for fn in (m.associate_one_to_one_by_time,m.associate_nearest_by_time):
   for tol in (-1,float('nan'),float('inf')):
    with self.assertRaises(ValueError):fn(wave([0]),hypo([0]),tolerance_seconds=tol)
   with self.assertRaises(ValueError):fn(wave([0,1],['a','a']),hypo([0]))
 def fixture(self,p):
  tables={
   'waveform_event_index':pd.DataFrame([dict(event_id='001',starttime=base.isoformat(),event_class='WAV_ONLY',wav_file='a.M.PNTBO_001'),dict(event_id='002',starttime=(base+pd.Timedelta(seconds=1)).isoformat(),event_class='WAV_ONLY',wav_file='b.M.PNTBO_001'),dict(event_id='003',starttime=(base+pd.Timedelta(seconds=20)).isoformat(),event_class='PICKS_ONLY')]),
   'waveform_pick_map':pd.DataFrame([dict(event_id='003',pick_id='p1')]),
   'pick_index':pd.DataFrame([dict(pick_id='p1',seed_id='XB.PI2..ELZ',pick_time=(base+pd.Timedelta(seconds=20)).isoformat(),phase=np.nan)]),
   'hypo_event_index':pd.DataFrame([dict(event_id='01',preferred_origin_time=base.isoformat()),dict(event_id='02',preferred_origin_time=(base+pd.Timedelta(seconds=100)).isoformat())]),
   'hypo_origin_index':pd.DataFrame([dict(event_id=h,origin_id='o'+h,origin_time=(base+pd.Timedelta(seconds=t)).isoformat(),latitude=15.,longitude=120.,depth_km=3.) for h,t in [('01',0),('02',100)]])}
  args={}
  for name,df in tables.items():args[name]=p/(name+'.csv');df.to_csv(args[name],index=False)
  return args
 def test_catalog_policies_and_readback(self):
  with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
   p=Path(tmp);args=self.fixture(p)
   for policy,expected in [('one_to_one',1),('nearest',2)]:
    r=m.run(**args,out_prefix=p/policy,association_policy=policy,origin_time_tol=2)
    self.assertEqual(len(r['matches']),expected)
    self.assertEqual(r['class_counts']['W_H'],expected)
    self.assertEqual(r['class_counts']['H_ONLY'],1)
    self.assertEqual(r['class_counts']['P_ONLY'],1)
    self.assertEqual(len(r['catalog_all']),4)
    self.assertEqual(len(r['catalog_waveform']),2)
    self.assertEqual(str(r['catalog_all'][0].resource_id),'event/001')
    picks=[pick for event in r['catalog_all'] for pick in event.picks]
    self.assertEqual(picks[0].waveform_id.get_seed_string(),'XB.PI2..ELZ')
    self.assertIsNone(picks[0].phase_hint)
    reread=read_events(str(r['output_paths']['all_xml']))
    self.assertEqual(len(reread),4)
    self.assertTrue(all(path.is_file() for path in r['output_paths'].values()))
 def test_default_and_empty_hypocenters(self):
  with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
   p=Path(tmp);args=self.fixture(p)
   for key in ('hypo_event_index','hypo_origin_index'):
    df=pd.read_csv(args[key]);df.iloc[:0].to_csv(args[key],index=False)
   r=m.run(**args,out_prefix=p/'empty')
   self.assertEqual(r['association_policy'],'one_to_one')
   self.assertEqual(len(r['catalog_all']),3)
   self.assertEqual(r['class_counts']['W_ONLY'],2)
   self.assertTrue(r['matches'].empty)
 def test_notebook_and_cli(self):
  nb=nbformat.read(root/'50_build_obspy_catalog.ipynb',as_version=4);nbformat.validate(nb)
  for c in nb.cells:
   if c.cell_type=='code':compile(c.source,'notebook','exec')
  with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
   p=Path(tmp);args=self.fixture(p);cli=[]
   for key,value in args.items():cli.extend(['--'+key.replace('_','-'),str(value)])
   cli.extend(['--out-prefix',str(p/'cli'),'--association-policy','nearest','--origin-time-tol','2'])
   self.assertEqual(m.main(cli)['association_policy'],'nearest')
if __name__=='__main__':unittest.main()
