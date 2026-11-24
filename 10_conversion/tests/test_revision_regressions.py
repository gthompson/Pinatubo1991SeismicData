"""Regression checks for review fixes; no production cells or products are run."""
import ast
import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]


def definitions(name, **extra):
    ns = dict(pd=pd, np=np, json=json, **extra)
    for cell in json.loads((ROOT / name).read_text())['cells']:
        if cell['cell_type'] != 'code':
            continue
        tree = ast.parse(''.join(cell['source']))
        nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.Import, ast.ImportFrom))]
        # Imports within configuration cells would load paths; only definitions are needed here.
        nodes = [node for node in nodes if isinstance(node, ast.FunctionDef)]
        exec('from __future__ import annotations\n' + ast.unparse(ast.Module(body=nodes, type_ignores=[])), ns)
    return ns


def tagged_notebook(name):
    from types import SimpleNamespace
    namespace = {}
    for cell in json.loads((ROOT / name).read_text())['cells']:
        if cell['cell_type'] == 'code' and 'pipeline_definitions' in cell.get('metadata', {}).get('tags', []):
            exec(compile(''.join(cell['source']), str(ROOT / name), 'exec'), namespace)
    return SimpleNamespace(**namespace)


def raw_catalogue():
    return pd.DataFrame(dict(time=['1991-06-01T00:00:00', '1991-06-01T00:00:20', '1991-06-01T00:00:40'],
                             source=['USGS', 'ISC', 'USGS'], event_id=['A', 'B', 'C'],
                             lat=[15.] * 3, lon=[120.] * 3, mag=[4.] * 3))


def test_external_candidates_are_disjoint_and_provenance_is_complete():
    raw = raw_catalogue()
    result = definitions('72_external_clock_anchors.ipynb')['merge_catalogue'](raw)
    assert result.event_id.is_unique
    members = [x['event_id'] for group in result.catalogue_members for x in json.loads(group)]
    assert sorted(members) == ['A', 'B', 'C']
    assert result.event_id.tolist() == ['B', 'C']


def test_same_external_id_with_conflicting_location_is_kept_once():
    raw = raw_catalogue().iloc[:2].copy()
    raw['event_id'] = 'ISC/A'; raw['source'] = 'ISC'; raw['lat'] = [15., 18.]
    result = definitions('72_external_clock_anchors.ipynb')['merge_catalogue'](raw)
    assert len(result) == 1 and result.catalogue_member_count.iloc[0] == 2
    assert len(json.loads(result.catalogue_members.iloc[0])) == 2


def test_empty_catalogue_has_usable_schema():
    result = definitions('72_external_clock_anchors.ipynb')['merge_catalogue'](raw_catalogue().iloc[:0])
    assert result.empty and {'t', 'sources', 'external_event_ids'} <= set(result)


def test_hypo71_comparison_excludes_closer_brun_origin():
    t = pd.Timestamp('1991-06-01', tz='UTC')
    cat = pd.DataFrame([dict(event_id='external', t=t, dist_km=10, mag=4, sources='ISC')])
    origins = pd.DataFrame(dict(source=['brun', 'hypo40'], origin_time=[t, t + pd.Timedelta(seconds=3)],
                                clock_regime=['rule', 'rule']))
    result = definitions('72_external_clock_anchors.ipynb')['compare_local'](cat, origins, 80, 600)
    assert result.nearest_hypo71_source.iloc[0] == 'hypo40'
    assert result.hypo71_minus_catalogue_s.iloc[0] == 3
    assert result.hypo71_within_window.iloc[0] == 1


def test_review_ambiguity_requires_explicit_identity():
    join = definitions('72_external_clock_anchors.ipynb')['join_anchor_reviews']
    matches = pd.DataFrame(dict(record=['w', 'w'], external_event_id=['A', 'B'], origin=[1, 2]))
    with pytest.raises(ValueError, match='specify external_event_id'):
        join(pd.DataFrame([dict(record='w')]), matches)
    result = join(pd.DataFrame([dict(record='w', external_event_id='B', reference_phase='pP')]), matches)
    assert result.origin.iloc[0] == 2 and result.reference_phase.iloc[0] == 'pP'
    with pytest.raises(ValueError, match='absent'):
        join(pd.DataFrame([dict(record='w', external_event_id='C')]), matches)
    unique = join(pd.DataFrame([dict(record='w')]), matches.iloc[:1])
    assert unique.identity_resolution.iloc[0] == 'unique_candidate_legacy_review'


def test_no_phase_falls_back_to_earliest_pick():
    from lib.shared import associate_hypocenters
    t = pd.Timestamp('1991-06-01', tz='UTC')
    events = pd.DataFrame([dict(event_id='w', event_time=t, endtime=t + pd.Timedelta(seconds=30))])
    origins = pd.DataFrame([dict(event_id='h', preferred_origin_time=t)])
    picks = pd.DataFrame([dict(event_id='w', pick_time=t + pd.Timedelta(seconds=2))])
    matches, _ = associate_hypocenters(events, picks, origins)
    assert matches.method.tolist() == ['picks']
    assert matches.first_arrival_minus_origin_s.tolist() == [2.]


def test_waveform_uncertainty_does_not_change_times_or_guess_rules():
    from lib.clock import annotate_waveform_clock, regimes
    rule = next(r for r in regimes().rows if 'waveform' in r['streams'])
    frame = pd.DataFrame(dict(clock_regime=[rule['regime'], rule['regime'], 'unknown'],
                              clock_offset_s=[rule['offset_s'], 123, 0], starttime=['unchanged'] * 3))
    before = frame.copy(deep=True)
    result = annotate_waveform_clock(frame, {})
    assert result.clock_uncertainty_s.iloc[0] == rule['uncertainty_s']
    assert result.clock_uncertainty_s.iloc[1:].isna().all()
    assert result.starttime.equals(frame.starttime)
    pd.testing.assert_frame_equal(before, frame)
    assert annotate_waveform_clock(frame, {'time_basis': 'recorded'}).clock_uncertainty_s.isna().all()


def test_pick_metadata_round_trips_without_invented_pick_error(tmp_path):
    m = tagged_notebook('50_build_obspy_catalog.ipynb')
    from obspy import read_events
    from obspy.core.event import Catalog, Event
    row = pd.Series(dict(pick_id='p', seed_id='XB.CAB..EHZ', pick_time='1991-06-01', phase='P',
                         onset='I', first_motion='D', weight=0, clock_regime='rule', clock_uncertainty_s=25))
    pick = m.build_pick(row)
    assert pick.onset == 'impulsive' and pick.polarity == 'negative'
    assert pick.time_errors.uncertainty is None
    event = Event(picks=[pick]); m.stabilize_comment_ids(event)
    ids = [str(c.resource_id) for c in pick.comments]; m.stabilize_comment_ids(event)
    assert ids == [str(c.resource_id) for c in pick.comments]
    path = tmp_path / 'p.xml'; Catalog(events=[event]).write(str(path), format='QUAKEML')
    saved = read_events(str(path))[0].picks[0]
    assert saved.onset == 'impulsive' and 'historical_weight:0' in [c.text for c in saved.comments]
    assert 'historical_clock_uncertainty_s:25' in [c.text for c in saved.comments]


def test_pair_denominators_and_unclipped_histogram(tmp_path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    n = json.loads((ROOT / '47_hypocenter_source_comparison.ipynb').read_text())
    origin_path = tmp_path / 'origins.csv'
    pd.DataFrame([dict(event_id=1, source=s, origin_id=str(i), origin_time='1991-06-01T00:00:00Z',
                       latitude=15, longitude=120, depth_km=depth, magnitude=mag)
                  for i, (s, depth, mag) in enumerate([('hypo40', 2, 1), ('brun', 3, 2), ('brun', 40, 9)])]).to_csv(origin_path, index=False)
    ns = dict(pd=pd, np=np, plt=plt, ORIGIN_INDEX=origin_path, OUT_DIR=tmp_path,
              PAIRS=[('hypo40', 'brun')], display=lambda *x: None)
    exec(''.join(n['cells'][8]['source']), ns)
    stats = ns['pair_stats'].iloc[0]
    assert stats.events == 1 and stats.origin_pairs == 2 and stats.statistics_unit == 'origin_pair'
    assert stats.dmag_median == -4.5
    exec(''.join(n['cells'][9]['source']), ns)
    assert (tmp_path / '47_pair_differences.png').is_file()
    table = pd.read_csv(tmp_path / '47_pairs_hypo40_brun.csv')
    assert len(table) == 2 and table.event_id.nunique() == 1
    plt.close('all')


def test_unreadable_rea_files_are_reported(tmp_path):
    m = tagged_notebook('81_seisan_rea_diagnostics.ipynb')
    rea = tmp_path / 'rea'; rea.mkdir(); out = tmp_path / 'out'
    good = rea / '01-0000-00L.S199106'; bad = rea / '02-0000-00L.S199106'
    good.write_text(' EVENT_CLASS:W_ONLY\n'); bad.write_text('unreadable')
    original = Path.read_text
    def read(path, *args, **kwargs):
        if path == bad:
            raise PermissionError('synthetic unreadable file')
        return original(path, *args, **kwargs)
    with patch.object(Path, 'read_text', read), pytest.warns(UserWarning, match='Incomplete REA audit'):
        m.run(rea_dir=rea, out_dir=out)
    assert len(pd.read_csv(out / '81_unreadable_sfiles.csv')) == 1
    audit = pd.read_csv(out / '81_read_audit.csv').iloc[0]
    assert audit.discovered == 2 and audit.read == 1 and audit.classified == 1


def test_duplicate_review_includes_non_depth_changes():
    import importlib.util
    spec = importlib.util.spec_from_file_location('revision_review', ROOT.parent / 'validation/prepare_revision_review.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    frame = pd.DataFrame([dict(event_id='a', source='brun', origin_id=str(i), origin_time='1991-06-01',
                               latitude=15 + i * .01, longitude=120, depth_km=2, magnitude=np.nan)
                          for i in range(2)])
    result = module.duplicate_origin_review(frame).iloc[0]
    assert not result.identical_reported_parameters
    assert result.depth_km_span == 0 and result.max_epicentral_separation_km > 1
    assert result.magnitude_missing == 2
    frame['latitude'] = 15
    assert module.duplicate_origin_review(frame).identical_reported_parameters.iloc[0]


def test_review_bundle_counts_and_no_overwrite(tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location('revision_review', ROOT.parent / 'validation/prepare_revision_review.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    root = tmp_path / 'utc'; p = root / 'pipeline_tmp'; p.mkdir(parents=True)
    files = {
        '43_hypo71_origin_index.csv': [dict(event_id='001', origin_id='o', source='brun', origin_time='1991-06-01', latitude=15, longitude=120, depth_km=3, magnitude=1)],
        '43_hypo71_event_index.csv': [dict(event_id='001', preferred_origin_id='o', preferred_origin_source='brun')],
        '22_merged_pick_index.csv': [dict(pick_id='001'), dict(pick_id='002')],
        '22_suppressed_picks.csv': [dict(pick_id='old')],
        '32_waveform_pick_event_map_suppressed_duplicates.csv': [dict(pick_id='002')],
        '32_waveform_pick_event_map.csv': [dict(pick_id='001')],
        '10_waveform_index.csv': [dict(event_id='w', ntraces=3)],
    }
    for name, rows in files.items(): pd.DataFrame(rows).to_csv(p / name, index=False)
    metadata = root / 'FAIR/metadata/50_pin_catalog_exports'; metadata.mkdir(parents=True)
    pd.DataFrame([dict(event_id='c')]).to_csv(metadata / 'event_manifest.csv', index=False)
    dest = tmp_path / 'review'; result = module.prepare(root, dest)
    assert result['counts']['NumberMergedPhasePicks'] == 2
    assert result['counts']['NumberRetainedPhasePicks'] == 1
    assert result['differing_brun_groups'] == 0
    assert '\\newcommand{\\NumberPicksSuppressedMerge}{1}' in (dest / 'counts.tex').read_text()
    with pytest.raises(FileExistsError): module.prepare(root, dest)
    pd.DataFrame([dict(pick_id='003')]).to_csv(p / '32_waveform_pick_event_map.csv', index=False)
    with pytest.raises(ValueError, match='conservation'): module.prepare(root, tmp_path / 'bad')
