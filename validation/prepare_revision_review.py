"""Read existing outputs and prepare review tables/counts in a fresh directory.

No production writes, preferred-origin changes, release certification or manuscript edits.
Usage: python validation/prepare_revision_review.py --output-root /path/to/utc --out /new/review
"""
from __future__ import annotations
import argparse
import hashlib
import json
from itertools import combinations
from pathlib import Path
import math
import pandas as pd


def distance_km(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (a.latitude, a.longitude, b.latitude, b.longitude))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 12742.0 * math.asin(math.sqrt(min(1., max(0., h))))


def duplicate_origin_review(origins):
    """Include differences in any reported parameter, not only nonzero depth range."""
    fields = ['origin_time', 'latitude', 'longitude', 'depth_km', 'magnitude']
    rows = []
    for (event_id, source), group in origins.groupby(['event_id', 'source'], sort=True):
        if len(group) < 2:
            continue
        times = pd.to_datetime(group.origin_time, utc=True, format='mixed')
        pairs = list(combinations(group.itertuples(), 2))
        row = dict(event_id=event_id, source=source, origins=len(group),
                   identical_reported_parameters=len(group[fields].drop_duplicates()) == 1,
                   origin_ids=';'.join(group.origin_id.astype(str)),
                   time_span_s=(times.max() - times.min()).total_seconds(),
                   max_epicentral_separation_km=max(distance_km(a, b) for a, b in pairs))
        for field in ['depth_km', 'magnitude']:
            values = pd.to_numeric(group[field], errors='coerce')
            row[field + '_span'] = float(values.max() - values.min()) if values.notna().sum() >= 2 else None
            row[field + '_missing'] = int(values.isna().sum())
        rows.append(row)
    columns = ['event_id', 'source', 'origins', 'identical_reported_parameters', 'origin_ids',
               'time_span_s', 'max_epicentral_separation_km', 'depth_km_span', 'depth_km_missing',
               'magnitude_span', 'magnitude_missing']
    return pd.DataFrame(rows, columns=columns)


def prepare(output_root, out):
    root, dest = Path(output_root).expanduser().resolve(), Path(out).expanduser().resolve()
    if dest == root or root in dest.parents:
        raise ValueError('Review destination must be outside the production output root')
    paths = {
        'origins': root / 'pipeline_tmp/43_hypo71_origin_index.csv',
        'groups': root / 'pipeline_tmp/43_hypo71_event_index.csv',
        'merged_picks': root / 'pipeline_tmp/22_merged_pick_index.csv',
        'suppressed_merge': root / 'pipeline_tmp/22_suppressed_picks.csv',
        'suppressed_container': root / 'pipeline_tmp/32_waveform_pick_event_map_suppressed_duplicates.csv',
        'retained_picks': root / 'pipeline_tmp/32_waveform_pick_event_map.csv',
        'waveforms': root / 'pipeline_tmp/10_waveform_index.csv',
        'catalogue': root / 'FAIR/metadata/50_pin_catalog_exports/event_manifest.csv',
    }
    # Read each input once: recorded hashes describe exactly the bytes analysed.
    from io import BytesIO
    data, provenance = {}, {}
    for name, path in paths.items():
        payload = path.read_bytes()
        data[name] = pd.read_csv(BytesIO(payload), dtype={'event_id': str, 'pick_id': str, 'origin_id': str}, low_memory=False)
        provenance[name] = dict(path=str(path), sha256=hashlib.sha256(payload).hexdigest(), rows=len(data[name]))
    original = data['merged_picks'].pick_id
    assigned = pd.concat([data['retained_picks'].pick_id, data['suppressed_container'].pick_id], ignore_index=True)
    if original.isna().any() or assigned.isna().any() or not original.is_unique or not assigned.is_unique or set(original) != set(assigned):
        raise ValueError('Pick conservation failed; do not generate manuscript counts')
    if not data['groups'].event_id.is_unique or not data['origins'].origin_id.is_unique:
        raise ValueError('Duplicate group/origin identifiers')
    if set(data['groups'].event_id) != set(data['origins'].event_id):
        raise ValueError('Group and origin tables have different event memberships')
    counts = dict(NumberWaveformFiles=len(data['waveforms']),
                  NumberWaveformTraces=int(pd.to_numeric(data['waveforms'].ntraces, errors='raise').sum()),
                  NumberMergedPhasePicks=len(original), NumberRetainedPhasePicks=len(data['retained_picks']),
                  NumberPicksSuppressedMerge=len(data['suppressed_merge']),
                  NumberPicksSuppressedContainer=len(data['suppressed_container']),
                  NumberHypocenterOrigins=len(data['origins']), NumberHypocenterGroups=len(data['groups']),
                  NumberCatalogContainers=len(data['catalogue']))
    for source in ('brun', 'gold', 'pepi', 'hypo40', 'pinaall'):
        label = {'hypo40': 'HypoSummary', 'pinaall': 'Pinaall'}.get(source, source.title())
        counts['NumberPreferred' + label] = int(data['groups'].preferred_origin_source.eq(source).sum())
    summary = duplicate_origin_review(data['origins'])
    changed = summary[summary.source.eq('brun') & ~summary.identical_reported_parameters.astype(bool)]
    members = data['origins'][data['origins'].source.eq('brun') & data['origins'].event_id.isin(changed.event_id)].copy()
    members = members.merge(data['groups'][['event_id', 'preferred_origin_id']], on='event_id', validate='many_to_one')
    members['is_preferred'] = members.origin_id.eq(members.preferred_origin_id)
    dest.mkdir(parents=True, exist_ok=False)
    summary.to_csv(dest / 'same_source_duplicate_groups.csv', index=False)
    changed.to_csv(dest / 'brun_differing_groups.csv', index=False)
    members.to_csv(dest / 'brun_differing_origins.csv', index=False)
    (dest / 'counts.json').write_text(json.dumps(counts, indent=2) + '\n')
    (dest / 'counts.tex').write_text('% Review snapshot only; not a certified frozen release. See provenance.json.\n' +
                                    ''.join('\\newcommand{\\' + key + '}{' + str(value) + '}\n' for key, value in counts.items()))
    (dest / 'provenance.json').write_text(json.dumps(dict(status='review_snapshot_not_release_certification',
        inputs=provenance, script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        duplicate_comparison_fields=['origin_time', 'latitude', 'longitude', 'depth_km', 'magnitude'],
        distance_model='spherical haversine, radius 6371 km', pick_conservation_passed=True), indent=2) + '\n')
    return dict(output=str(dest), differing_brun_groups=len(changed), counts=counts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.output_root, args.out), indent=2))
