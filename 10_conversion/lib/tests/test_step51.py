"""Step 51: reporting windows are derived from days with waveforms, picks and hypocentres."""
import json, sys, types, unittest
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def load_step51():
    ns = {}
    for c in json.loads((ROOT / '51_event_numbers_in_complete_windows.ipynb').read_text())['cells']:
        source = ''.join(c.get('source', []))
        if c['cell_type'] == 'code' and 'def derive_coverage_windows(' in source:
            exec(compile(source, 'step51', 'exec'), ns)
    return types.SimpleNamespace(**ns)


s51 = load_step51()


def days(start, counts):
    """counts: list of (W, P, H) per consecutive day."""
    idx = pd.date_range(start, periods=len(counts), freq='D', name='date')
    return pd.DataFrame(counts, index=idx, columns=['W', 'P', 'H'])


class Step51Tests(unittest.TestCase):
    def test_class_sources(self):
        self.assertEqual(s51.class_sources('W_P_H'), {'W', 'P', 'H'})
        self.assertEqual(s51.class_sources('H_ONLY'), {'H'})
        self.assertEqual(s51.class_sources('P_H'), {'P', 'H'})

    def test_short_gaps_merge_long_gaps_split(self):
        full = (100, 100, 100)
        counts = [full] * 5 + [(100, 0, 0)] * 3 + [full] * 5 + [(100, 0, 0)] * 4 + [full] * 5
        w = s51.derive_coverage_windows(days('1991-07-01', counts), max_gap_days=3)
        self.assertEqual(w, [('1991-07-01', '1991-07-13'), ('1991-07-18', '1991-07-22')])

    def test_partial_edge_days_trimmed(self):
        full = (100, 100, 100)
        counts = [(100, 100, 2), (100, 100, 20)] + [full] * 6 + [(100, 1, 50)]
        w = s51.derive_coverage_windows(days('1991-05-06', counts), min_edge_fraction=0.25)
        # 6 May (H=2) and 14 May (P=1) are partial; 7 May (H=20 < 25) too.
        self.assertEqual(w, [('1991-05-08', '1991-05-13')])

    def test_interior_low_days_kept(self):
        full = (100, 100, 100)
        counts = [full] * 3 + [(100, 100, 1)] + [full] * 3
        w = s51.derive_coverage_windows(days('1991-08-01', counts))
        self.assertEqual(w, [('1991-08-01', '1991-08-07')])

    def test_daily_source_counts_fills_missing_days(self):
        df = pd.DataFrame({'date': pd.to_datetime(['1991-07-01', '1991-07-03']),
                           'event_time_utc': pd.to_datetime(['1991-07-01', '1991-07-03']),
                           'event_class': ['W_P_H', 'W_ONLY']})
        day = s51.daily_source_counts(df)
        self.assertEqual(day.loc['1991-07-02'].tolist(), [0, 0, 0])
        self.assertEqual(day.loc['1991-07-03'].tolist(), [1, 0, 0])


if __name__ == '__main__':
    unittest.main()
