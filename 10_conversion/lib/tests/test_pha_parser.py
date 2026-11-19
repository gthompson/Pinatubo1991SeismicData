"""Regression tests for fixed-column timestamps in lib.pha_parser.

Lines are copied from the legacy monthly PHA files (pinjul91.pha, pinaug91.pha).
"""
import unittest

from obspy import UTCDateTime

from lib.pha_parser import parse_phase_line


def picks(line):
    return [(p['phase'], p['time']) for p in parse_phase_line(line) or []]


class FixedColumnTimestampTests(unittest.TestCase):
    def test_normal_p_and_s_line(self):
        line = 'BUGZ PU0 910603073938.49       39.99 S 2'
        self.assertEqual(picks(line), [('P', UTCDateTime('1991-06-03T07:39:38.49')),
                                       ('S', UTCDateTime('1991-06-03T07:39:39.99'))])

    def test_s_only_line_keeps_minute(self):
        # Blank P seconds: '91 8 2 2 6' is 02:06, not 02:00:06.
        line = 'CABN     91 8 2 2 6            37.91 S 3'
        self.assertEqual(picks(line), [('S', UTCDateTime('1991-08-02T02:06:37.91'))])

    def test_s_only_line_two_digit_hour_and_minute(self):
        line = 'CABZ     91 7311942            16.45 S  '
        self.assertEqual(picks(line), [('S', UTCDateTime('1991-07-31T19:42:16.45'))])

    def test_s_seconds_above_sixty_add_to_minute(self):
        line = 'PI2L     91 8 4 2 3            63.19ES?1'
        self.assertEqual(picks(line), [('S', UTCDateTime('1991-08-04T02:04:03.19'))])

    def test_p_code_with_blank_p_seconds_gives_no_p_pick(self):
        line = 'CABN P 4 91 8 823 4            70.13 S 2'
        self.assertEqual(picks(line), [('S', UTCDateTime('1991-08-08T23:05:10.13'))])

    def test_timestamp_shifted_one_column(self):
        line = 'BUGZIPU0  91 8 6 65625.02'
        self.assertEqual(picks(line), [('P', UTCDateTime('1991-08-06T06:56:25.02'))])


class MonthlyBlockTests(unittest.TestCase):
    def test_block_index_counts_empty_blocks_and_errors_are_logged(self):
        import tempfile
        from pathlib import Path
        from lib.pha_parser import parse_pha_file
        text = '\n'.join(['garbage card', '10',
                          'CABZ PU0 91 7311700 1.00', 'PI2ZIPU0 91 7311700 2.00',
                          'QADZIPU0 91 7311700 3.00', 'CRWZIPU0 91 7311800 3.00', '10', ''])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'pinjul91.pha'; path.write_text(text)
            errors = []
            events = parse_pha_file(path, errors)
        self.assertEqual([e['block_index'] for e in events], [1])
        self.assertEqual(len(events[0]['picks']), 3)
        self.assertTrue(any(':1: error:' in e for e in errors))
        self.assertTrue(any('trimmed P XB.CRW..EHZ' in e for e in errors))


class SplitBlockTests(unittest.TestCase):
    def test_block_without_separator_is_split(self):
        import tempfile
        from pathlib import Path
        from lib.pha_parser import parse_pha_file
        text = '\n'.join(['CABZ PU0 91 7311700 1.00', 'PI2ZIPU0 91 7311700 2.00', 'QADZIPU0 91 7311700 3.00',
                          'CABZ PU0 91 7311701 1.00', 'PI2ZIPU0 91 7311701 2.00', 'QADZIPU0 91 7311701 3.00', '10', ''])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'pinjul91.pha'; path.write_text(text)
            errors = []
            events = parse_pha_file(path, errors)
        self.assertEqual([(e['block_index'], e['sub_index'], len(e['picks'])) for e in events], [(0, 0, 3), (0, 1, 3)])
        self.assertTrue(any('holds 2 events' in e for e in errors))


if __name__ == '__main__':
    unittest.main()
