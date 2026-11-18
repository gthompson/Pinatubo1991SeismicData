import importlib.util,unittest,tempfile,contextlib,io,json,math
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
import numpy as np
import nbformat
from obspy import read_events
root=Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(root))
m = _load_pipeline_notebook('50_build_obspy_catalog.ipynb')
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
   self.assertEqual(r['association_policy'],'picks_then_window')
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


class PicksThenWindowTests(unittest.TestCase):
 def setUp(self):
  from pinatubo_steps.shared import associate_hypocenters
  self.f=associate_hypocenters
  T=lambda s:(base+pd.Timedelta(seconds=s)).isoformat()
  # w1: record 0-60 s with picks from 11 s; w2: record 100-160 s, no picks; p3: pick-only event at 300 s.
  self.evt=pd.DataFrame([dict(event_id='w1',starttime=T(0),endtime=T(60),event_class='WAV+PICKS'),
                         dict(event_id='w2',starttime=T(100),endtime=T(160),event_class='WAV_ONLY'),
                         dict(event_id='p3',starttime=T(300),endtime=T(301),event_class='PICKS_ONLY')])
  self.map=pd.DataFrame([dict(event_id='w1',pick_time=T(11),phase='P'),dict(event_id='w1',pick_time=T(12),phase='S'),
                         dict(event_id='p3',pick_time=T(301.5),phase='P')])
  self.T=T
 def hyp(self,times):return pd.DataFrame({'event_id':[f'h{i}' for i in range(len(times))],'preferred_origin_time':[self.T(t) for t in times]})
 def test_picks_then_window(self):
  # h0 9.5 s into w1 (picks), h1 15 s into w2 (window; outside ±10 s of start), h2 just before p3's pick,
  # h3 inside w1 but w1 already taken, h4 nowhere.
  m,inside=self.f(self.evt,self.map,self.hyp([9.5,115,300,40,500]))
  got={r.event_id_h:(r.event_id_w,r.method) for r in m.itertuples()}
  self.assertEqual(got,{'h0':('w1','picks'),'h1':('w2','window'),'h2':('p3','picks')})
  self.assertEqual(inside,{'h3':'w1'})
  self.assertAlmostEqual(m.set_index('event_id_h').loc['h1','origin_minus_start_s'],15.0)
 def test_window_lead_and_bounds(self):
  m,_=self.f(self.evt,self.map.iloc[:0],self.hyp([92]))   # 8 s before w2 starts: inside the 10-s lead
  self.assertEqual(m.event_id_w.tolist(),['w2'])
  m,_=self.f(self.evt,self.map.iloc[:0],self.hyp([85]))   # 15 s before: no link
  self.assertTrue(m.empty)
  with self.assertRaises(ValueError):self.f(self.evt,self.map,self.hyp([0]),origin_lead_s=-1)
  with self.assertRaises(ValueError):self.f(pd.concat([self.evt,self.evt]),self.map,self.hyp([0]))
 def test_catalog_default_policy(self):
  with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
   p=Path(tmp);args=Tests.fixture(self,p)
   r=m.run(**args,out_prefix=p/'ptw')
   self.assertEqual(r['association_policy'],'picks_then_window')
   self.assertEqual(r['matches'].event_id_h.tolist(),['01'])


class PreferredOriginTests(unittest.TestCase):
 def test_step43_choice_is_preferred(self):
  with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
   p=Path(tmp);args=Tests.fixture(self,p)
   ho=pd.read_csv(args['hypo_origin_index'],dtype=str)
   extra=ho.iloc[[0]].assign(origin_id='o01b',origin_time=(base+pd.Timedelta(seconds=0.5)).isoformat())
   pd.concat([ho,extra]).to_csv(args['hypo_origin_index'],index=False)
   he=pd.read_csv(args['hypo_event_index'],dtype=str)
   he['preferred_origin_id']=['o01b','o02'];he['preferred_origin_source']=['hypo40','hypo40']
   he.to_csv(args['hypo_event_index'],index=False)
   r=m.run(**args,out_prefix=p/'pref')
   linked=next(e for e in r['catalog_all'] if len(e.origins)==2)
   self.assertEqual(str(linked.preferred_origin_id),'origin/o01b')
   self.assertIn('preferred_origin:step43_origin_id; source:hypo40',[c.text for c in linked.comments])
   # Without an ID column the time decides; the H-only event keeps its only origin.
   he.drop(columns=['preferred_origin_id']).to_csv(args['hypo_event_index'],index=False)
   r=m.run(**args,out_prefix=p/'pref2')
   linked=next(e for e in r['catalog_all'] if len(e.origins)==2)
   self.assertEqual(str(linked.preferred_origin_id),'origin/o01')  # preferred_origin_time = base
