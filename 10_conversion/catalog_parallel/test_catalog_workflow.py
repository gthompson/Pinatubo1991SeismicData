"""Small integration cases independent of the real archive."""
import json
import tempfile
import unittest
from pathlib import Path
import sys
import os
import pandas as pd
from obspy import read_events

sys.path.insert(0, str(Path(os.environ.get('PINATUBO_CODE_ROOT', str(Path(__file__).resolve().parents[2]))) / '10_conversion'))
from catalog_workflow import SOURCES, build_sources, merge_sources, get, check_catalog


class SourceCatalogTests(unittest.TestCase):
    def fixture(self, root):
        raw = root / 'raw'; raw.mkdir()
        for name in SOURCES.values():
            (raw / name).write_text('fixture\n')
        wav = root / 'SEISAN/WAV/PNTBO/1991/06/trigger'; wav.parent.mkdir(parents=True)
        wav.write_bytes(b'fixture')
        rows = []
        for source, h, line, sec, mag in [('hypo40','1',1,9,0.5), ('hypo40','1',2,9,0.6),
                                        ('brun','1',1,9,1.4), ('brun','2',2,20,0),
                                        ('pinaall','2',1,20,0.7), ('gold','3',1,120,1.2)]:
            t = f'1991-06-01T00:{sec//60:02}:{sec%60:02}Z'
            rows.append(dict(event_id=h, origin_id=f'{source}_{line}', origin_time=t,
                raw_origin_time=t, latitude=15., longitude=120., depth_km=2., magnitude=mag,
                reported_magnitude=mag, source=source, source_file=SOURCES[source], source_line=line,
                clock_regime='fixture', clock_uncertainty_s=25.))
        he=pd.DataFrame([dict(event_id=h, preferred_origin_time=t, preferred_origin_id=o)
            for h,t,o in [('1','1991-06-01T00:00:09Z','brun_1'),
                          ('2','1991-06-01T00:00:20Z','brun_2'),
                          ('3','1991-06-01T00:02:00Z','gold_1')]])
        spine=pd.DataFrame([dict(event_id='w1',event_class='WAV+PICKS', wav_file=str(wav),
            starttime='1991-06-01T00:00:00Z',endtime='1991-06-01T00:01:00Z')])
        mapping=pd.DataFrame([dict(event_id='w1',pick_id='p1',phase='P',pick_time='1991-06-01T00:00:11Z')])
        picks=pd.DataFrame([dict(pick_id='p1',phase='P',pick_time='1991-06-01T00:00:11Z',seed_id='XB.A..EHZ',
                                 onset='I',first_motion='U',weight=0)])
        paths={'LEGACY_HYPO_DIR':str(raw),'SEISAN_WAV_DB':str(root/'SEISAN/WAV/PNTBO')}
        for key,table in [('HYPO71_ORIGIN_INDEX',pd.DataFrame(rows)),('HYPO71_EVENT_INDEX',he),
                          ('WAVEFORM_PICK_EVENT_INDEX',spine),('PICK_MAP_CSV',mapping),('MERGED_PICK_INDEX',picks)]:
            path=root/(key+'.csv');table.to_csv(path,index=False);paths[key]=str(path)
        return paths

    def test_roundtrip_shared_waveform_missing_magnitude_and_duplicate_origins(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); paths=self.fixture(root)
            manifest=build_sources(paths,root/'sources')
            cat,report=merge_sources(root/'sources',root/'merged')
            self.assertEqual(report['origins'],6)
            self.assertEqual(report['magnitudes'],5)
            self.assertEqual(report['events'],3)
            self.assertEqual(report['picks'],1)
            events={get(e)['hypocenter_group_id']:e for e in cat}
            self.assertEqual(get(events['1'])['waveform_file'],get(events['2'])['waveform_file'])
            self.assertEqual(get(events['2'])['association'],'shared_window_candidate')
            self.assertEqual(len(events['2'].picks),0)
            self.assertIsNone(get(events['3'])['waveform_file'])
            self.assertEqual(get(events['1'].preferred_magnitude())['source'],'brun')
            self.assertEqual(get(events['2'].preferred_magnitude())['source'],'pinaall')
            subset=read_events(str(root/'sources/hypo40.xml'))
            self.assertEqual(len(subset),1)
            self.assertEqual(len(subset[0].origins),2)
            self.assertEqual({get(m)['source'] for m in subset[0].magnitudes},{'hypo40'})
            self.assertEqual(str(subset[0].resource_id),str(events['1'].resource_id))
            with self.assertRaises(FileExistsError):
                build_sources(paths,root/'sources')
            path=root/'sources/brun.xml';path.write_text(path.read_text()+'\n')
            with self.assertRaisesRegex(ValueError,'bundle changed'):
                merge_sources(root/'sources',root/'tampered')

    def test_missing_waveform_is_retained_as_missing_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); paths=self.fixture(root)
            (root/'SEISAN/WAV/PNTBO/1991/06/trigger').unlink()
            build_sources(paths,root/'sources')
            cat=read_events(str(root/'sources/brun.xml'))
            self.assertTrue(get(cat[0])['waveform_file'])
            self.assertFalse(get(cat[0])['waveform_exists'])

    def test_dangling_reference_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=self.fixture(root)
            build_sources(paths,root/'sources')
            cat=read_events(str(root/'sources/brun.xml'))
            cat[0].magnitudes[0].origin_id='smi:local/does/not/exist'
            with self.assertRaisesRegex(ValueError,'Dangling magnitude'):
                check_catalog(cat)

if __name__=='__main__':
    unittest.main()
