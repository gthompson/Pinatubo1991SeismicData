"""Parallel source-first QuakeML workflow; existing pipeline products are read-only."""
from __future__ import annotations
import copy
import hashlib
import json
from pathlib import Path

import pandas as pd
from obspy import UTCDateTime, read_events
from obspy.core.event import (Catalog, Event, Origin, Magnitude, Pick, Comment,
                              ResourceIdentifier, WaveformStreamID, OriginQuality)
from obspy.core.util.attribdict import AttribDict

NS = 'https://pinatubo.example.org/xmlns/catalog-provenance/1'  # namespace identifier, not a service
BASE = 'smi:local/pinatubo/'
SOURCES = {'hypo40': 'Pinatubo_all.sum', 'pinaall': 'PINAALL.DAT',
           'gold': 'GOLD.SUM', 'brun': 'b-run.sum', 'pepi': 'PinatuboRelocs.rtf'}
INPUT_KEYS = ['HYPO71_ORIGIN_INDEX', 'HYPO71_EVENT_INDEX', 'WAVEFORM_PICK_EVENT_INDEX',
              'PICK_MAP_CSV', 'MERGED_PICK_INDEX']


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def clean(value):
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    if hasattr(value, 'item'):
        return value.item()
    return value


def put(obj, values):
    """One documented JSON payload in a QuakeML namespace; no Python-only attributes."""
    obj.extra = AttribDict(provenance=AttribDict(
        value=json.dumps(values, sort_keys=True, allow_nan=False), namespace=NS))


def get(obj):
    return json.loads(obj.extra.provenance.value)


def rid(kind, key):
    return ResourceIdentifier(BASE + kind + '/' + hashlib.sha256(str(key).encode()).hexdigest())


def annotate(obj, text):
    obj.comments.append(Comment(text=text, resource_id=rid('comment', str(obj.resource_id) + text)))


def waveform_reference(row, wav_root):
    value = clean(row.get('wav_file')) or clean(row.get('waveform_file'))
    if not value:
        return None, None
    root = Path(wav_root).resolve()
    path = Path(value)
    path = path.resolve() if path.is_absolute() else (root / path).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f'Waveform is outside SEISAN/WAV: {path}')
    rel = path.relative_to(root)
    if len(rel.parts) != 4:
        raise ValueError(f'Expected database/YYYY/MM/file waveform path: {rel}')
    return rel.as_posix(), path.is_file()


def event_from_metadata(event_id, metadata):
    ev = Event(resource_id=ResourceIdentifier(event_id))
    put(ev, metadata)
    if metadata.get('waveform_file'):
        annotate(ev, 'wavfile:' + metadata['waveform_file'])
    return ev


def classify(ev):
    meta = get(ev)
    w = bool(meta.get('waveform_file')) and meta.get('waveform_exists') is True
    p, h = bool(ev.picks), bool(ev.origins)
    label = '_'.join(k for k, present in [('W', w), ('P', p), ('H', h)] if present)
    label = {'W': 'W_ONLY', 'P': 'P_ONLY', 'H': 'H_ONLY'}.get(label, label or 'EMPTY')
    ev.comments = [c for c in ev.comments if not c.text.startswith(('event_class:', 'waveform_present:',
                          'picks_present:', 'hypocenter_present:'))]
    annotate(ev, 'event_class:' + label)
    for name, value in [('waveform_present', w), ('picks_present', p), ('hypocenter_present', h)]:
        annotate(ev, f'{name}:{value}')
    return label


def source_signature(cat):
    """Compare scientific content, provenance, IDs and links across XML round trips."""
    rows = []
    for ev in cat:
        rows.append(('event', str(ev.resource_id), json.dumps(get(ev), sort_keys=True),
                     str(ev.preferred_origin_id), str(ev.preferred_magnitude_id)))
        for o in ev.origins:
            rows.append(('origin', str(ev.resource_id), str(o.resource_id), str(o.time),
                         o.latitude, o.longitude, o.depth, json.dumps(get(o), sort_keys=True)))
        for m in ev.magnitudes:
            rows.append(('magnitude', str(ev.resource_id), str(m.resource_id), str(m.origin_id),
                         m.mag, m.magnitude_type, json.dumps(get(m), sort_keys=True)))
        for p in ev.picks:
            rows.append(('pick', str(ev.resource_id), str(p.resource_id), str(p.time),
                         p.phase_hint, p.waveform_id.get_seed_string(), p.onset, p.polarity,
                         json.dumps(get(p), sort_keys=True)))
    return sorted(rows, key=repr)


def check_catalog(cat):
    ids = set()
    for ev in cat:
        for obj in [ev, *ev.origins, *ev.magnitudes, *ev.picks]:
            key = str(obj.resource_id)
            if key in ids:
                raise ValueError(f'Duplicate resource ID: {key}')
            ids.add(key)
            get(obj)  # all retained objects must carry serializable provenance
        origins = {str(o.resource_id) for o in ev.origins}
        mags = {str(m.resource_id) for m in ev.magnitudes}
        for m in ev.magnitudes:
            if str(m.origin_id) not in origins:
                raise ValueError(f'Dangling magnitude origin: {m.resource_id}')
        if ev.preferred_origin_id and str(ev.preferred_origin_id) not in origins:
            raise ValueError('Dangling preferred origin')
        if ev.preferred_magnitude_id and str(ev.preferred_magnitude_id) not in mags:
            raise ValueError('Dangling preferred magnitude')
        ev.scope_resource_ids()
    return {'events': len(cat), 'origins': sum(len(e.origins) for e in cat),
            'magnitudes': sum(len(e.magnitudes) for e in cat),
            'picks': sum(len(e.picks) for e in cat)}


def write_checked(cat, path):
    check_catalog(cat)
    cat.write(str(path), format='QUAKEML', nsmap={'pin': NS}, validate=True)
    reread = read_events(str(path))
    check_catalog(reread)
    if source_signature(cat) != source_signature(reread):
        raise AssertionError(f'QuakeML round trip changed content: {path}')
    return check_catalog(reread)


def prepare_output(path):
    path = Path(path)
    if path.exists() and any(path.iterdir()):
        raise FileExistsError(f'Choose a new empty output directory: {path}')
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_sources(paths, output_dir, *, include_shared_windows=True, candidate_bundle_dir=None):
    """Reuse step 43 groups and step 50 association rules; no new clocks or locations."""
    from lib.shared import associate_hypocenters
    out = prepare_output(output_dir)
    tables = {k: pd.read_csv(paths[k], dtype={'event_id': str, 'pick_id': str,
              'origin_id': str}, low_memory=False) for k in INPUT_KEYS}
    ho, he, spine, mapping, picks = [tables[k] for k in INPUT_KEYS]
    if set(ho.source) - set(SOURCES):
        raise ValueError('Unknown input source')
    if ho.origin_id.duplicated().any() or he.event_id.duplicated().any():
        raise ValueError('Duplicate input index IDs')
    if set(ho.event_id) != set(he.event_id):
        raise ValueError('Origin/event index memberships disagree')
    wav_root = Path(paths['SEISAN_WAV_DB']).parent
    candidate_manifest = None
    if candidate_bundle_dir is not None:
        from candidate_events import integration_tables
        spine, mapping, matches, candidate_manifest = integration_tables(paths, candidate_bundle_dir)
        inside = {}  # no time-window fallback that would bypass unresolved candidate decisions
    else:
        matches, inside = associate_hypocenters(spine, mapping, he)
    matches.to_csv(out / 'primary_associations.csv', index=False)
    primary = {str(r.event_id_h): (str(r.event_id_w), r.method)
               for r in matches.itertuples()}
    reverse = {w: h for h, (w, _) in primary.items()}
    spine_rows = spine.set_index('event_id').to_dict('index')
    source_hash = {s: digest(Path(paths['LEGACY_HYPO_DIR']) / name) for s, name in SOURCES.items()}
    # Stable IDs depend on source record identity, not ordinal event/origin indexes.
    record_keys = {r.origin_id: f'{r.source}/{r.source_file}/{int(r.source_line)}'
                   for r in ho.itertuples()}
    if len(set(record_keys.values())) != len(ho):
        raise ValueError('Duplicate source record identities')
    event_ids = {h: str(rid('event', '|'.join(sorted(record_keys[x] for x in g.origin_id))))
                 for h, g in ho.groupby('event_id')}
    metadata = {}
    candidate_decisions = {r['hypocenter_group_id']: r for r in candidate_manifest['origin_decisions']} if candidate_manifest else {}
    preferred_by_group = he.set_index('event_id').preferred_origin_id.to_dict()
    for h in he.event_id:
        w, method = primary.get(h, (None, 'unassociated'))
        if w is None and include_shared_windows and h in inside:
            w, method = str(inside[h]), 'shared_window_candidate'
        ref, exists = waveform_reference(spine_rows[w], wav_root) if w else (None, None)
        metadata[h] = {'hypocenter_group_id': h, 'association': method,
                       'association_status': 'candidate_not_independently_validated',
                       'spine_event_id': w, 'waveform_file': ref, 'waveform_exists': exists,
                       'preferred_origin_input_id': clean(preferred_by_group[h])}
        # Candidate decisions, including competing options, survive QuakeML on historical events.
    if candidate_manifest:
        for h in metadata:
            metadata[h]['candidate_decision'] = candidate_decisions[h]
    pd.DataFrame([dict(event_id=event_ids[h], **m) for h, m in metadata.items()]).to_csv(
        out / 'event_waveform_associations.csv', index=False)
    manifest = {'schema_version': 1, 'source_names': SOURCES, 'sources': {},
                'inputs': {k: {'path': str(paths[k]), 'sha256': digest(paths[k])} for k in INPUT_KEYS},
                'raw_source_sha256': source_hash, 'waveform_root': str(wav_root),
                'include_shared_windows': include_shared_windows if candidate_manifest is None else False,
                'candidate_manifest_sha256': digest(Path(candidate_bundle_dir) / 'manifest.json') if candidate_bundle_dir else None,
                'candidate_mode': candidate_manifest is not None,
                'association_parameters': {'max_first_arrival_s': 10.0, 'origin_lead_s': 10.0},
                'software_sha256': digest(__file__)}
    for source, rows in ho.groupby('source', sort=True):
        catalog = Catalog()
        for h, group in rows.groupby('event_id', sort=True):
            ev = event_from_metadata(event_ids[h], metadata[h])
            for row in group.to_dict('records'):
                key = record_keys[row['origin_id']]
                provenance = {k: clean(v) for k, v in row.items()}
                provenance['source_sha256'] = source_hash[source]
                provenance['source_record_key'] = key
                if provenance.get('raw_origin_time'):
                    provenance['applied_clock_offset_s'] = float(
                        UTCDateTime(row['origin_time']) - UTCDateTime(row['raw_origin_time']))
                origin = Origin(resource_id=rid('origin', key), time=UTCDateTime(row['origin_time']),
                                latitude=float(row['latitude']), longitude=float(row['longitude']),
                                depth=float(row['depth_km']) * 1000.)
                if clean(row.get('rms')) is not None or clean(row.get('nass')) is not None:
                    origin.quality = OriginQuality(standard_error=clean(row.get('rms')),
                        used_phase_count=int(row['nass']) if clean(row.get('nass')) is not None else None)
                # Clock allowance is kept as provenance, not asserted to be a statistical error.
                put(origin, provenance)
                annotate(origin, f"origin_source:{source}; file:{row['source_file']}; line:{row['source_line']}")
                ev.origins.append(origin)
                value = clean(row.get('magnitude'))
                if value is not None and value != 0:
                    mag = Magnitude(resource_id=rid('magnitude', key), mag=float(value),
                                    origin_id=origin.resource_id)  # magnitude type is unknown
                    put(mag, dict(provenance, magnitude_policy='reported_nonzero_untransformed'))
                    annotate(mag, 'magnitude_source:' + source)
                    ev.magnitudes.append(mag)
            # Source-only preferences are deterministic representatives, not quality claims.
            ev.preferred_origin_id = ev.origins[0].resource_id
            if ev.magnitudes:
                ev.preferred_magnitude_id = ev.magnitudes[0].resource_id
            classify(ev)
            catalog.append(ev)
        filename = source + '.xml'
        stats = write_checked(catalog, out / filename)
        expected_mags = int((rows.magnitude.notna() & rows.magnitude.ne(0)).sum())
        if stats['origins'] != len(rows) or stats['magnitudes'] != expected_mags:
            raise AssertionError('Source conservation failure')
        manifest['sources'][source] = dict(file=filename, sha256=digest(out / filename), **stats)
        print(source, stats)
    # Preserve waveform/pick-only containers and actual picks in a separate intermediate catalog.
    # Picks belong only to the primary association, never copied to secondary window candidates.
    observations = Catalog()
    by_pick = picks.set_index('pick_id')
    if by_pick.index.has_duplicates:
        raise ValueError('Duplicate pick IDs in pick index')
    pick_map = {w: g.pick_id.drop_duplicates().tolist() for w, g in mapping.groupby('event_id')}
    membership_reasons = mapping.set_index('pick_id').membership_reason.to_dict() if 'membership_reason' in mapping else {}
    envelope_by_pick = (pd.read_csv(Path(candidate_bundle_dir) / 'envelope_support.csv', dtype={'pick_id': str})
                        .set_index('pick_id').to_dict('index')) if candidate_bundle_dir else {}
    seen_picks = set()
    for w, row in spine_rows.items():
        h = reverse.get(w)
        if h is not None:
            ev = event_from_metadata(event_ids[h], metadata[h])
        else:
            ref, exists = waveform_reference(row, wav_root)
            ev = event_from_metadata(str(rid('observation', w)), {
                'spine_event_id': w, 'waveform_file': ref, 'waveform_exists': exists,
                'association': 'unassociated_observation_container',
                **({k: clean(row.get(k)) for k in ('candidate_id', 'candidate_status', 'original_window_id')} if candidate_manifest else {})})
        for pid in pick_map.get(w, []):
            if pid not in by_pick.index:
                raise ValueError(f'Mapped pick missing from index: {pid}')
            if pid in seen_picks:
                raise ValueError(f'Pick assigned to several observation containers: {pid}')
            seen_picks.add(pid)
            rowp = by_pick.loc[pid]
            seed = str(rowp.seed_id).split('.')
            if len(seed) != 4:
                raise ValueError(f'Invalid SEED ID: {rowp.seed_id}')
            pick = Pick(resource_id=rid('pick', pid), time=UTCDateTime(rowp.pick_time),
                        phase_hint=clean(rowp.get('phase')), waveform_id=WaveformStreamID(
                        network_code=seed[0], station_code=seed[1], location_code=seed[2], channel_code=seed[3]))
            pick.onset = {'I': 'impulsive', 'E': 'emergent'}.get(str(rowp.get('onset')).upper())
            pick.polarity = {'U': 'positive', 'D': 'negative'}.get(str(rowp.get('first_motion')).upper())
            pick_provenance = {k: clean(v) for k, v in rowp.to_dict().items()}
            if candidate_manifest:
                pick_provenance['candidate_membership_reason'] = membership_reasons[pid]
                pick_provenance['candidate_container_id'] = w
                pick_provenance['waveform_support_diagnostic'] = {k: clean(v) for k, v in envelope_by_pick.get(pid, {}).items()}
            put(pick, pick_provenance)
            ev.picks.append(pick)
        classify(ev)
        observations.append(ev)
    filename = 'observations.xml'
    manifest['observations'] = dict(file=filename, **write_checked(observations, out / filename))
    manifest['observations']['sha256'] = digest(out / filename)
    manifest['unmapped_pick_count'] = len(set(picks.pick_id) - seen_picks)
    manifest['located_event_count'] = len(he)
    manifest['input_origin_count'] = len(ho)
    manifest['input_magnitude_count'] = int((ho.magnitude.notna() & ho.magnitude.ne(0)).sum())
    manifest['association_counts'] = pd.Series([m['association'] for m in metadata.values()]).value_counts().to_dict()
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


def merge_sources(bundle_dir, output_dir, *, magnitude_priority=('brun', 'pinaall', 'hypo40', 'gold', 'pepi')):
    """Merge the saved source catalogs, not concatenated duplicate events; verify conservation."""
    bundle = Path(bundle_dir)
    manifest = json.loads((bundle / 'manifest.json').read_text())
    if set(magnitude_priority) != set(SOURCES) or len(magnitude_priority) != len(SOURCES):
        raise ValueError('Magnitude priority must name each source once')
    out = prepare_output(output_dir)
    events, originals = {}, []
    for entry in [*manifest['sources'].values(), manifest['observations']]:
        path = bundle / entry['file']
        if digest(path) != entry['sha256']:
            raise ValueError(f'Input bundle changed: {path}')
        cat = read_events(str(path))
        check_catalog(cat)
        for ev in cat:
            key = str(ev.resource_id)
            if key not in events:
                events[key] = event_from_metadata(key, get(ev))
            elif get(events[key]) != get(ev):
                raise ValueError(f'Conflicting event provenance: {key}')
            target = events[key]
            target.origins.extend(copy.deepcopy(ev.origins))
            target.magnitudes.extend(copy.deepcopy(ev.magnitudes))
            target.picks.extend(copy.deepcopy(ev.picks))
            if entry is not manifest['observations']:
                originals.extend((str(o.resource_id), str(ev.resource_id), get(o)) for o in ev.origins)
    catalog = Catalog(events=[events[k] for k in sorted(events)])
    rows = []
    for ev in catalog:
        wanted = get(ev).get('preferred_origin_input_id')
        if ev.origins:
            selected = next((o for o in ev.origins if get(o).get('origin_id') == wanted), None)
            if selected is None:
                raise ValueError('Preferred origin from step 43 missing from merged event')
            ev.preferred_origin_id = selected.resource_id
        if ev.magnitudes:
            chosen = min(ev.magnitudes, key=lambda m: (magnitude_priority.index(get(m)['source']),
                         get(m)['origin_time'], get(m)['source_line']))
            ev.preferred_magnitude_id = chosen.resource_id
        label = classify(ev)
        ev.scope_resource_ids()
        rows.append(dict(event_id=str(ev.resource_id), event_class=label,
                         **get(ev), origins=len(ev.origins), magnitudes=len(ev.magnitudes), picks=len(ev.picks)))
    path = out / 'catalog_combined.xml'
    stats = write_checked(catalog, path)
    if stats['origins'] != manifest['input_origin_count'] or stats['magnitudes'] != manifest['input_magnitude_count']:
        raise AssertionError('Merged source conservation failure')
    if stats['picks'] != manifest['observations']['picks']:
        raise AssertionError('Merged pick conservation failure')
    reread = read_events(str(path))
    # Verify every origin and magnitude from every individual catalog survived the disk merge.
    for source, entry in manifest['sources'].items():
        original = read_events(str(bundle / entry['file']))
        expected = {str(obj.resource_id): (str(ev.resource_id), get(obj))
                    for ev in original for obj in [*ev.origins, *ev.magnitudes]}
        actual = {str(obj.resource_id): (str(ev.resource_id), get(obj))
                  for ev in reread for obj in [*ev.origins, *ev.magnitudes] if get(obj)['source'] == source}
        if actual != expected:
            raise AssertionError(f'Merged provenance or membership changed: {source}')
    pd.DataFrame(rows).to_csv(out / 'event_manifest.csv', index=False)
    report = dict(stats, source_roundtrip_verified=True, magnitude_priority=list(magnitude_priority),
                  magnitude_scales='untransformed; source-specific; types unknown',
                  located_events=sum(bool(e.origins) for e in catalog),
                  events_with_magnitude=sum(bool(e.magnitudes) for e in catalog),
                  bundle_manifest_sha256=digest(bundle / 'manifest.json'),
                  class_counts=pd.Series([r['event_class'] for r in rows]).value_counts().to_dict(),
                  output_sha256=digest(path))
    (out / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
    return catalog, report
