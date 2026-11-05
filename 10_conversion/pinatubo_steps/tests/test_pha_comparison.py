import unittest,tempfile
from pathlib import Path
import pandas as pd
from pinatubo_steps.pha_comparison import compare_pick_sources,compare_pick_csvs

T=pd.Timestamp('1991-06-03T12:00:00Z')
def table(times,channel='EHZ'):
    return pd.DataFrame([dict(pick_id=f'p{i}',event_id='91060301',station='PI2',channel=channel,
        phase='P',pick_time=(T+pd.Timedelta(seconds=t)).isoformat(),pha_file='91060301.PHA',raw_lineno=i+1) for i,t in enumerate(times)],
        columns=['pick_id','event_id','station','channel','phase','pick_time','pha_file','raw_lineno'])

class PHAComparisonTests(unittest.TestCase):
    def summary(self,result,source,tolerance,date='ALL'):
        frame=result['summary'];return frame[(frame.source==source)&(frame.tolerance_s==tolerance)&(frame.date==date)].iloc[0]

    def test_tolerances_signed_offsets_and_provenance(self):
        left=table([0]);right=table([.05]);before=left.copy(deep=True)
        result=compare_pick_sources(left,right,tolerances=(.049,.05,1))
        self.assertEqual(self.summary(result,'individual',.049).matched,0)
        self.assertEqual(self.summary(result,'individual',.05).matched,1)
        pair=result['pairs'][result['pairs'].tolerance_s==.05].iloc[0]
        self.assertAlmostEqual(pair.delta_s,.05)
        self.assertEqual(pair.individual_pick_id,'p0')
        self.assertEqual(pair.individual_raw_lineno,1)
        self.assertTrue(pair.mutual_unique)
        pd.testing.assert_frame_equal(left,before)

    def test_reused_counterparts_and_ambiguity(self):
        result=compare_pick_sources(table([0,.02]),table([.01]),tolerances=(.05,))
        left=self.summary(result,'individual',.05);right=self.summary(result,'monthly',.05)
        self.assertEqual(left.matched,2);self.assertEqual(right.matched,1)
        self.assertEqual(left.reused_counterpart,2)
        self.assertEqual(right.multiple_candidates,1)
        self.assertEqual(len(result['ambiguous_pairs']),2)
        self.assertEqual(left.mutual_unique,0)

    def test_gain_network_phase_identity(self):
        left=table([0],channel='ELZ');right=table([0],channel='EHZ')
        self.assertTrue(compare_pick_sources(left,right)['pairs'].empty)
        right=table([0],channel='ELZ');right['seed_id']='YY.PI2..ELZ'
        self.assertTrue(compare_pick_sources(left,right)['pairs'].empty)
        right=table([0],channel='ELZ');right['phase']='S'
        self.assertTrue(compare_pick_sources(left,right)['pairs'].empty)

    def test_empty_and_missing_dates(self):
        result=compare_pick_sources(table([]),table([]),dates=['910603'],tolerances=(.05,))
        self.assertTrue(result['pairs'].empty)
        self.assertTrue(result['summary'].match_rate.isna().all())
        self.assertTrue(result['summary'].coverage_status.eq('no_records').all())
        result=compare_pick_sources(table([0]),table([]),tolerances=(.05,))
        self.assertEqual(self.summary(result,'individual',.05).unmatched,1)
        self.assertTrue(pd.isna(self.summary(result,'monthly',.05).match_rate))

    def test_cross_midnight_context(self):
        left=table([0]);right=table([0])
        left['pick_time']='1991-06-03T23:59:59.99Z';right['pick_time']='1991-06-04T00:00:00.01Z'
        result=compare_pick_sources(left,right,dates=['910603'],tolerances=(.05,))
        self.assertEqual(self.summary(result,'individual',.05).matched,1)
        self.assertEqual(self.summary(result,'monthly',.05).valid_picks,0)
        self.assertFalse(result['pairs'].iloc[0].monthly_in_scope)

    def test_invalid_rows_and_date_discrepancies(self):
        left=table([0,1]);left.loc[0,'pick_time']='bad'
        left.loc[1,'pha_path']='/PHA/910603/910610A6.PHA'
        left.loc[1,'pick_time']='1991-06-10T08:10:31Z'
        result=compare_pick_sources(left,table([]),tolerances=(.05,))
        self.assertEqual(len(result['invalid']),1)
        audit=result['date_audit'].query('date_mismatch').iloc[0]
        self.assertIn('folder_vs_pick',audit.issues)
        self.assertIn('folder_vs_filename',audit.issues)
        self.assertNotIn('filename_vs_pick',audit.issues)
        self.assertEqual(self.summary(result,'individual',.05).invalid_picks,1)
        self.assertEqual(len(result['unmatched']),1)

    def test_csv_exports_and_input_preservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp=Path(tmp);left=tmp/'a.csv';right=tmp/'b.csv'
            table([0]).to_csv(left,index=False);table([.01]).to_csv(right,index=False)
            before=(left.read_bytes(),right.read_bytes())
            result=compare_pick_csvs(left,right,outdir=tmp/'reports')
            self.assertEqual(len(list((tmp/'reports').glob('*.csv'))),9)
            for path in (tmp/'reports').glob('*.csv'):pd.read_csv(path)
            self.assertEqual(before,(left.read_bytes(),right.read_bytes()))
            with self.assertRaises(FileNotFoundError):compare_pick_csvs(tmp/'missing.csv',right,outdir=tmp)
            # Refuse a report path that would replace an input.
            colliding=tmp/'23_pha_summary.csv';table([0]).to_csv(colliding,index=False)
            with self.assertRaises(ValueError):compare_pick_csvs(colliding,right,outdir=tmp)

    def test_nearest_outside_tolerance(self):
        result=compare_pick_sources(table([0]),table([2]),tolerances=(.05,1))
        self.assertTrue(result['pairs'].empty)
        candidates=result['nearest_candidates']
        self.assertEqual(len(candidates),2)
        self.assertTrue(candidates.reason.eq('outside_largest_tolerance').all())
        self.assertTrue(candidates.delta_s.eq(2).all())
    def test_validation(self):
        for tolerances in [[],[-1],[float('nan')],[float('inf')]]:
            with self.assertRaises(ValueError):compare_pick_sources(table([]),table([]),tolerances=tolerances)
        with self.assertRaises(ValueError):compare_pick_sources(pd.DataFrame(),table([]))

if __name__=='__main__':unittest.main()
