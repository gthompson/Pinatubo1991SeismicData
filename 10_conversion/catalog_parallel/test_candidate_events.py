import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
import numpy as np
import pandas as pd
from obspy import Trace, Stream, UTCDateTime, read_events

from candidate_events import Settings, group_window, measure_pick, propose_associations, run, integration_tables
from catalog_workflow import build_sources, merge_sources, get
import test_catalog_workflow

T=UTCDateTime('1991-06-01T00:00:00Z')

def pick(pid, sec, sta, phase='P', group='g'):
    return dict(pick_id=pid, pick_time=str(T+sec), seed_id=f'XB.{sta}..EHZ',
                phase=phase,pick_group_id=group,waveform_seed_id=f'XB.{sta}..EHZ')


def signal(sta, times, seed=1):
    rng=np.random.default_rng(seed); fs=100.;x=rng.normal(0,.01,6000)
    for t in times:
        start=int(t*fs); n=120
        x[start:start+n]+=np.sin(np.arange(n)*.6)*np.exp(-np.arange(n)/100)
    tr=Trace(x.astype('float32'));tr.stats.update(dict(network='XB',station=sta,location='',channel='EHZ',sampling_rate=fs,starttime=T))
    return tr


class CandidateTests(unittest.TestCase):
    def test_two_and_three_events_cluster_across_stations(self):
        rows=[pick(f'{i}{s}',t+lag,s,group=str(i)) for i,t in enumerate([10,25,40]) for s,lag in [('A',0),('B',.7)]]
        c,m,_=group_window('w',pd.DataFrame(rows),Settings())
        self.assertEqual(len(c),3)
        self.assertTrue(all(r['stable'] and r['n_stations']==2 for r in c))
        self.assertEqual(len({r['pick_id'] for r in m}),6)

    def test_chain_span_repeated_station_and_gap_sensitivity_flagged(self):
        p=pd.DataFrame([pick(str(i),10+i*1.8,'A',group=str(i)) for i in range(7)])
        c,_,_=group_window('w',p,Settings())
        self.assertGreater(len(c),1)
        self.assertTrue(all(r['end_epoch']-r['start_epoch']<=6 for r in c))
        self.assertTrue(any('repeated_station_P' in r['flags'] for r in c))
        self.assertTrue(any(not r['stable'] for r in c))

    def test_S_requires_unambiguous_group_and_preceding_station_P(self):
        p=pd.DataFrame([pick('p1',10,'A'),pick('p2',10.5,'B'),pick('s1',12,'A','S'),
                        pick('s2',13,'C','S'),pick('out',90,'A')])
        _,m,_=group_window('w',p,Settings(),start=T.timestamp,end=(T+60).timestamp)
        m={r['pick_id']:r for r in m}
        self.assertEqual(m['p1']['candidate_id'],m['s1']['candidate_id'])
        self.assertIsNone(m['s2']['candidate_id'])
        self.assertEqual(m['out']['membership_reason'],'outside_waveform_window')

    def test_noise_flat_and_clipped_waveforms_not_supported(self):
        st=Stream([signal('A',[10])]);cache={}
        self.assertTrue(measure_pick(st,pick('p',10,'A'),Settings(),cache)['support'])
        self.assertFalse(measure_pick(st,pick('p',40,'A'),Settings(),cache)['support'])
        flat=st.copy();flat[0].data[:]=0
        self.assertEqual(measure_pick(flat,pick('p',10,'A'),Settings(),{})['quality'],'flat_trace')
        clipped=st.copy();clipped[0].data[1000:1020]=2
        self.assertEqual(measure_pick(clipped,pick('p',10,'A'),Settings(),{})['quality'],'possible_clipping')
        self.assertFalse(measure_pick(st,pick('p',10,'B'),Settings(),{})['support'])

    def test_competing_origins_are_not_greedily_assigned(self):
        c=pd.DataFrame([dict(candidate_id='c',start_epoch=(T+10).timestamp,status='supported_candidate')])
        h=pd.DataFrame([dict(event_id='1',preferred_origin_time=str(T+8)),dict(event_id='2',preferred_origin_time=str(T+9))])
        a,e=propose_associations(c,h,Settings())
        self.assertTrue(a.candidate_id.isna().all())
        self.assertEqual(set(a.status),{'multiple_hypocenter_groups'})
        self.assertEqual(len(e),2)

    def make_fixture(self,root):
        paths=test_catalog_workflow.SourceCatalogTests().fixture(root)
        st=Stream([signal('A',[11,21],1),signal('B',[11.3,21.3],2)])
        st.write(str(root/'SEISAN/WAV/PNTBO/1991/06/trigger'),format='MSEED')
        records=[pick('p1',11,'A',group='first'),pick('p2',11.3,'B',group='first'),
                 pick('p3',21,'A',group='second'),pick('p4',21.3,'B',group='second'),
                 pick('s1',13,'A','S',group='first'),pick('noise',40,'A',group='third'),
                 pick('unmapped',50,'A',group='unknown')]
        p=pd.DataFrame(records);p['event_id']='original';p['pha_file']='test.pha'
        p.to_csv(paths['MERGED_PICK_INDEX'],index=False)
        m=p[p.pick_id.ne('unmapped')][['pick_id','pick_group_id','waveform_seed_id']].copy();m['event_id']='w1'
        m.to_csv(paths['PICK_MAP_CSV'],index=False)
        return paths

    def test_full_candidate_to_quakeml_roundtrip_keeps_every_pick(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=self.make_fixture(root)
            summary=run(paths,root/'candidates',replace(Settings(),max_waveforms=None,max_plots=0))
            self.assertEqual(summary['pick_count'],7)
            self.assertEqual(summary['candidate_count'],3)
            self.assertEqual(summary['association_status_counts']['supported_unique_proposal'],2)
            s,m,matches,_=integration_tables(paths,root/'candidates')
            self.assertEqual(len(m),7);self.assertEqual(len(matches),2)
            build_sources(paths,root/'sources',candidate_bundle_dir=root/'candidates')
            cat,report=merge_sources(root/'sources',root/'merged')
            self.assertEqual(report['origins'],6);self.assertEqual(report['picks'],7)
            located={get(e)['hypocenter_group_id']:e for e in cat if e.origins}
            self.assertEqual(len(located['1'].picks),3);self.assertEqual(len(located['2'].picks),2)
            self.assertEqual(get(located['1'])['waveform_file'],get(located['2'])['waveform_file'])
            allp=[p for e in cat for p in e.picks]
            self.assertEqual(len({str(p.resource_id) for p in allp}),7)
            self.assertIn('unmapped_upstream',[get(p)['candidate_membership_reason'] for p in allp])
            # A changed diagnostic must never be applied silently.
            f=root/'candidates/pick_membership.csv';f.write_text(f.read_text()+'\n')
            with self.assertRaisesRegex(ValueError,'diagnostic changed'):
                integration_tables(paths,root/'candidates')

    def test_unchecked_waveforms_do_not_enable_automatic_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=self.make_fixture(root)
            summary=run(paths,root/'candidates',replace(Settings(),max_waveforms=0,max_plots=0))
            self.assertNotIn('supported_unique_proposal',summary['association_status_counts'])
            self.assertEqual(summary['waveforms_read'],0)

    def test_same_station_channels_do_not_supply_two_stations(self):
        rows=[pick('p1',10,'A'),pick('p2',10.1,'A')];rows[1]['seed_id']='XB.A..EHN'
        c,_,_=group_window('w',pd.DataFrame(rows),Settings())
        self.assertEqual(c[0]['n_stations'],1)

if __name__=='__main__': unittest.main()
