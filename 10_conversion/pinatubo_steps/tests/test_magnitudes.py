"""A reported magnitude of 0.00 is a missing value (steps 40-43)."""
import sys, unittest
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pinatubo_steps.shared import mark_missing_magnitudes, MISSING_MAGNITUDE_REASON


class MissingMagnitudeTests(unittest.TestCase):
    def test_zero_becomes_missing_and_is_kept_as_reported(self):
        out = mark_missing_magnitudes(pd.DataFrame({'magnitude': [0.0, 1.2, -0.3, 0.01]}))
        self.assertTrue(pd.isna(out.magnitude[0]))
        self.assertEqual(out.magnitude[1:].tolist(), [1.2, -0.3, 0.01])
        self.assertEqual(out.reported_magnitude.tolist(), [0.0, 1.2, -0.3, 0.01])
        self.assertEqual(out.magnitude_missing_reason.tolist(), [MISSING_MAGNITUDE_REASON, '', '', ''])

    def test_idempotent(self):
        once = mark_missing_magnitudes(pd.DataFrame({'magnitude': [0.0, 2.0]}))
        twice = mark_missing_magnitudes(once)
        pd.testing.assert_frame_equal(once, twice)

    def test_reread_csv_keeps_reason(self):
        once = mark_missing_magnitudes(pd.DataFrame({'magnitude': [0.0, 2.0]}))
        import io
        buf = io.StringIO(); once.to_csv(buf, index=False); buf.seek(0)
        again = mark_missing_magnitudes(pd.read_csv(buf))
        self.assertEqual(again.magnitude_missing_reason.tolist(), [MISSING_MAGNITUDE_REASON, ''])
        self.assertTrue(pd.isna(again.magnitude[0]))


if __name__ == '__main__':
    unittest.main()
