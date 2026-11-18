"""Step 32: monthly groups are attached whole, and dropped duplicates do not resurface."""
import json, sys, types, unittest
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def load_step32():
    ns = {}
    for c in json.loads((ROOT / '32_build_waveform_pick_events.ipynb').read_text())['cells']:
        source = ''.join(c.get('source', []))
        if c['cell_type'] == 'code' and 'def build_event_catalog(' in source:
            exec(compile(source, 'step32', 'exec'), ns)
    return types.SimpleNamespace(**ns)


s32 = load_step32()
BASE = pd.Timestamp('1991-07-01T00:00:00Z')
T = lambda s: (BASE + pd.Timedelta(seconds=s)).isoformat()


class Step32Tests(unittest.TestCase):
    def build(self):
        wav = pd.DataFrame([dict(event_id='w1', starttime=T(0), endtime=T(30), wav_file='w1', stations='CAB', channels='EHZ'),
                            dict(event_id='w2', starttime=T(100), endtime=T(130), wav_file='w2', stations='CAB', channels='EHZ')])
        rows = []
        def add(group, sta, phase, t):
            rows.append(dict(pick_id=f'{group}_{len(rows)}', event_id=group, event_id_source='monthly_phase_block',
                             seed_id=f'XB.{sta}..EHZ', station=sta, channel='EHZ', phase=phase, pick_time=T(t)))
        add('g1', 'CAB', 'P', 25); add('g1', 'BUG', 'P', 28); add('g1', 'FNG', 'P', 32)   # last pick beyond w1's end
        add('g2', 'CAB', 'P', 25.1); add('g2', 'BUG', 'P', 28.1)                         # duplicate copy of g1
        add('g4', 'CAB', 'P', 29); add('g4', 'BUG', 'P', 101); add('g4', 'FNG', 'P', 102) # mostly in w2
        add('g3', 'CAB', 'P', 500)                                                       # no record
        picks = pd.DataFrame(rows)
        empty_map = pd.DataFrame(columns=['pick_id', 'waveform_id', 'time_offset_from_start'])
        return s32.build_event_catalog(wav, picks, empty_map, tol_s=0.5), picks

    def test_whole_groups_and_duplicates(self):
        (events, pick_map, qc, suppressed), picks = self.build()
        where = dict(zip(pick_map.pick_id, pick_map.event_id))
        self.assertEqual({where[p] for p in picks.pick_id[picks.event_id.eq('g1')]}, {'wav_w1'})
        self.assertEqual({where[p] for p in picks.pick_id[picks.event_id.eq('g4')]}, {'wav_w2'})
        self.assertEqual(set(events.event_id), {'wav_w1', 'wav_w2', 'pick_g3'})
        self.assertEqual(sorted(suppressed.pick_group_id), ['g2', 'g2'])
        flagged = pick_map[pick_map.outside_record.fillna(False).astype(bool)]
        self.assertEqual(sorted(flagged.pick_time), sorted([T(32), T(29)]))
        # Every pick is accounted for exactly once.
        self.assertEqual(sorted(list(pick_map.pick_id) + list(suppressed.pick_id)), sorted(picks.pick_id))


if __name__ == '__main__':
    unittest.main()
