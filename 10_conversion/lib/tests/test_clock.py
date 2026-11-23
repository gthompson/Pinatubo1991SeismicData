"""Checks on the Pinatubo clock table (clock_regimes.csv) against known records."""
import unittest

import pandas as pd
from obspy import UTCDateTime

from flovopy.core.clock import dos_mktime_wall_clock
from lib.clock import (apply_bob_clock, apply_hypocenter_clock, apply_pick_clock,
                                  regimes, waveform_time_correction)

UTC = dict(time_basis='utc')


def wav(t, seq, marker=None):
    offset, label = waveform_time_correction(UTC)(None, seq, UTCDateTime(t), {'localtime': marker})
    return None if offset is None else UTCDateTime(t) + offset, label


class PinatuboClockTests(unittest.TestCase):
    def test_recorded_basis_is_a_no_op_and_utc_is_default(self):
        recorded = dict(time_basis='recorded')
        self.assertIsNone(waveform_time_correction(recorded))
        df = pd.DataFrame(dict(pick_time=['1991-06-03T12:00:00']))
        self.assertIs(apply_pick_clock(df, recorded, 'pick_individual'), df)
        self.assertIsNotNone(waveform_time_correction({}))

    def test_header_marker_must_agree_with_sequence(self):
        self.assertEqual(wav('1991-05-26T15:41:33', '91052617', 0)[0], UTCDateTime('1991-05-26T00:41:33'))
        self.assertEqual(wav('1991-05-26T08:40:26', '9105261G', -25200)[0], UTCDateTime('1991-05-26T01:40:26'))
        # Old-version marker after the reset, or new-version marker before it: no rule, file fails visibly.
        self.assertIsNone(wav('1991-06-01T00:00', '91060100', 0)[0])
        self.assertIsNone(wav('1991-05-20T00:00', '91052000', -25200)[0])

    def test_waveform_reset_on_26_may(self):
        # Last file before and first file after the PC clock was set back.
        self.assertEqual(wav('1991-05-26T15:41:33', '91052617')[0], UTCDateTime('1991-05-26T00:41:33'))
        self.assertEqual(wav('1991-05-26T08:40:26', '9105261G')[0], UTCDateTime('1991-05-26T01:40:26'))
        # PINAALL line 218 (01:40:38 UTC) is the 9105261G event.
        self.assertEqual(wav('1991-05-10T12:00', '91051000')[0], UTCDateTime('1991-05-09T21:00'))

    def test_header_rows_match_mktime_after_reset(self):
        for t in ['1991-06-15T03:00', '1991-10-27T08:59', '1991-10-27T09:01', '1991-12-01T00:00']:
            self.assertEqual(wav(t, '91999999')[0], dos_mktime_wall_clock(t))

    def test_monthly_picks_switch_clock(self):
        df = pd.DataFrame(dict(pha_file=['pinmay91.pha', 'pinmay91.pha', 'pinjun91.pha', 'pinjul91.pha'],
                               raw_lineno=[5451, 5542, 100, 10],
                               pick_time=['1991-05-26T01:43:12.76', '1991-05-26T09:57:29.71',
                                          '1991-06-03T12:56:36.37', '1991-07-01T00:00:00']))
        out = apply_pick_clock(df, UTC, 'pick_monthly')
        self.assertEqual(out.pick_time.tolist(), ['1991-05-25T10:43:12.760000', '1991-05-26T02:57:29.710000',
                                                  '1991-06-03T05:56:36.370000', '1991-07-01T00:00:00'])

    def test_hypocenters_and_allsum_matching(self):
        pinaall = pd.DataFrame(dict(source_line=[217, 218], latitude=[15.157, 15.158], longitude=[120.31, 120.32],
                                    origin_time=['1991-05-26T08:31:25.61', '1991-05-26T01:40:38.67']))
        allsum = pd.DataFrame(dict(source_line=[1, 2, 3], latitude=[15.158, 15.157, 15.2], longitude=[120.32, 120.31, 120.3],
                                   origin_time=['1991-05-26T01:40:38.67', '1991-05-26T08:31:25.61', '1991-05-26T05:00:00']))
        p, a = apply_hypocenter_clock(pinaall, allsum, UTC)
        self.assertEqual(p.origin_time.tolist(), ['1991-05-26T00:31:25.610000', '1991-05-26T01:40:38.670000'])
        self.assertEqual(a.origin_time.tolist()[:2], ['1991-05-26T01:40:38.670000', '1991-05-26T00:31:25.610000'])
        self.assertTrue(pd.isna(a.origin_time[2]))
        self.assertEqual(a.clock_status[2], 'ambiguous')
        # A waveform file whose header window holds raw + 7 h decides (pre-reset file 91052617).
        wav = pd.DataFrame(dict(event_id=['91052617'], raw_starttime=['1991-05-26T11:59:55'],
                                starttime=['1991-05-25T20:59:55'], endtime=['1991-05-25T21:01:00']))
        _, a = apply_hypocenter_clock(pinaall, allsum, UTC, wav)
        self.assertEqual(a.origin_time[2], '1991-05-25T21:00:00')
        self.assertEqual(a.clock_status[2], 'resolved_by_waveform_91052617')

    def test_bob(self):
        frame = pd.DataFrame(dict(recorded_time=pd.to_datetime(['1991-05-05 00:00', '1991-06-15 08:00', '1991-10-01 00:00'])))
        out = apply_bob_clock(frame, UTC, 'rsam')
        self.assertEqual(out.recorded_time.tolist(), list(pd.to_datetime(['1991-05-04 09:00', '1991-06-15 00:00', '1991-10-01 00:00'])))
        self.assertEqual(out.clock_confidence.tolist(), ['low', 'high', 'medium'])
        self.assertEqual(out.clock_uncertainty_s.tolist(), [10800.0, 300.0, 300.0])
        # SSAM ran on WILLIE (MDETECT; GMT after 26 May), not on the RSAM PC clock.
        ssam = apply_bob_clock(frame, UTC, 'ssam')
        self.assertEqual(ssam.recorded_time.tolist()[1:], list(pd.to_datetime(['1991-06-15 08:00', '1991-10-01 00:00'])))
        # Tilt labels are PHT (battery charging follows the PHT solar day).
        tilt = apply_bob_clock(frame, UTC, 'tilt')
        self.assertEqual(tilt.recorded_time.iloc[1], pd.Timestamp('1991-06-15 00:00'))
        self.assertEqual(set(tilt.clock_regime), {'T_PHT'})

    def test_ricky_lahar_and_date_only_products(self):
        frame = pd.DataFrame(dict(series='USAC', recorded_time=pd.to_datetime(
            ['1991-08-01 12:00', '1991-10-01 12:00', '1991-11-16 12:00'])))
        for product in ('rain', 'mudflow'):
            out = apply_bob_clock(frame, UTC, product)
            # PHT until 13 Sep, GMT until 15 Nov, then untested: label kept and flagged.
            self.assertEqual(out.recorded_time.tolist(), list(pd.to_datetime(
                ['1991-08-01 04:00', '1991-10-01 12:00', '1991-11-16 12:00'])))
            self.assertEqual(out.clock_status.tolist(), ['resolved', 'resolved', 'unverified'])
            self.assertTrue(out.raw_recorded_time.equals(frame.recorded_time))
        counts = pd.DataFrame(dict(series=['CAB_CNT.DAT', 'CAB_HR.DAT', 'PIE_CNT.DAT'],
                                   recorded_time=pd.to_datetime(['1991-08-01 10:30', '1991-10-01 10:30', '1991-05-10 00:00'])))
        out = apply_bob_clock(counts, UTC, 'counts')
        self.assertEqual(out.recorded_time.tolist(), list(pd.to_datetime(
            ['1991-08-01 02:30', '1991-10-01 10:30', '1991-05-10 00:00'])))
        self.assertEqual(out.clock_status.tolist(), ['resolved', 'resolved', 'date_only'])
        for product in ('cospec', 'radio'):
            out = apply_bob_clock(counts.assign(series='X'), UTC, product)
            self.assertTrue(out.recorded_time.equals(counts.recorded_time))
            self.assertEqual(set(out.clock_status), {'date_only'})

    def test_every_stream_has_rules(self):
        self.assertEqual(regimes().streams, {'waveform', 'pick_individual', 'pick_monthly', 'hypo_pinaall',
                                             'hypo_allsum', 'tandy_rsam', 'ssam', 'sleepy_tilt', 'ricky_lahar',
                                             'cab_hourly_counts', 'hypo_gold', 'hypo_brun', 'hypo_pepi'})


if __name__ == '__main__':
    unittest.main()


class RelocationClockTests(unittest.TestCase):
    """GOLD and PEPI follow the hypocentre (W) rules, BRUN the monthly-pick (H) rules; records the rules
    cannot place are matched to a HYPO71 origin."""

    def test_streams_and_matching(self):
        import pandas as pd
        from lib.clock import apply_relocation_clock
        config = {"time_basis": "utc"}
        ref = pd.DataFrame(dict(origin_time=['1991-05-26T01:40:38', '1991-06-01T00:17:18'], latitude=[15.14, 15.15],
                                longitude=[120.35, 120.33], clock_status=['resolved_by_pinaall_line_218', 'resolved'],
                                clock_uncertainty_s=[25.0, 25.0], source_file=['PINAALL.DAT', 'PINAALL.DAT'],
                                source_line=[218, 300]))
        brun = pd.DataFrame(dict(origin_time=['1991-06-01T07:17:18.15', '1991-05-26T08:40:38.3'],
                                 latitude=[15.15, 15.141], longitude=[120.33, 120.351]))
        out = apply_relocation_clock(brun, 'brun', config, ref)
        self.assertEqual(list(out.origin_time), ['1991-06-01T00:17:18.150000', '1991-05-26T01:40:38.300000'])
        self.assertEqual(out.clock_regime.iloc[0], 'H_UTC_PDT_until_switch')
        self.assertEqual(out.clock_regime.iloc[1], 'H_UTC_seq_matched')  # repeated 26 May window: matched
        self.assertEqual(out.clock_status.iloc[1], 'resolved_by_PINAALL.DAT_line_218')
        gold = pd.DataFrame(dict(origin_time=['1991-05-20T12:00:00', '1991-05-26T05:00:00'],
                                 latitude=[15.1, 15.1], longitude=[120.3, 120.3]))
        out = apply_relocation_clock(gold, 'gold', config, ref)
        self.assertEqual(out.origin_time.iloc[0], '1991-05-20T04:00:00')  # W on PHT: -8 h
        self.assertTrue(pd.isna(out.origin_time.iloc[1]))               # no HYPO71 match: left unplaced
        self.assertEqual(out.clock_status.iloc[1], 'ambiguous_no_match')
