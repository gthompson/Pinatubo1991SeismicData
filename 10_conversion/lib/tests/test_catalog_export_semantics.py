"""Composition survives portable exports without fabricating unlocated origins."""
import json
import tempfile
import unittest
from pathlib import Path
def _load_pipeline_notebook(filename):
    import json, types
    path = Path(__file__).resolve().parents[2] / filename
    namespace = {}
    for cell in json.loads(path.read_text())['cells']:
        if cell['cell_type'] == 'code' and 'pipeline_definitions' in cell.get('metadata', {}).get('tags', []):
            exec(compile(''.join(cell['source']), str(path), 'exec'), namespace)
    return types.SimpleNamespace(**namespace)

from unittest.mock import patch
from types import SimpleNamespace
from obspy import UTCDateTime, read_events
from obspy.core.event import Catalog, Event, Origin, Pick, Comment, WaveformStreamID
_exports = _load_pipeline_notebook('50_build_obspy_catalog.ipynb')
EVENT_CLASS_FLAGS = _exports.EVENT_CLASS_FLAGS
export_classified_catalog = _exports.export_classified_catalog
export_antelope_css3 = _load_pipeline_notebook('82_export_antelope_css3.ipynb').export_antelope_css3

class ExportSemanticsTests(unittest.TestCase):
    def catalog(self):
        result = Catalog()
        for label, (w, p, h) in EVENT_CLASS_FLAGS.items():
            event = Event(comments=[Comment(text='event_class:' + label)])
            if w:
                event.comments.append(Comment(text='wavfile:example.mseed'))
            if p:
                event.picks.append(Pick(time=UTCDateTime(1991, 6, 3), phase_hint='P',
                                        waveform_id=WaveformStreamID(seed_string='XB.PI2..EHZ')))
            if h:
                event.origins.append(Origin(time=UTCDateTime(1991, 6, 3), latitude=15, longitude=120, depth=3000))
                event.preferred_origin_id = event.origins[0].resource_id
            result.append(event)
        return result

    def test_class_manifest_and_quakeml_subsets(self):
        with tempfile.TemporaryDirectory() as tmp:
            info = export_classified_catalog(self.catalog(), tmp)
            self.assertEqual(len(info['quakeml_paths']), 8)
            rows = json.loads((Path(tmp) / 'event_manifest.json').read_text())
            self.assertEqual({r['event_class'] for r in rows}, set(EVENT_CLASS_FLAGS))
            for path in info['quakeml_paths']:
                self.assertGreater(len(read_events(path)), 0)

    @staticmethod
    def mock_datascope():
        class DB:
            def __init__(self):
                self.rows, self.ids = {}, {}
            def lookup(self, table):
                return SimpleNamespace(addv=lambda *fields: self.rows.setdefault(table, []).append(dict(fields)))
            def nextid(self, key):
                self.ids[key] = self.ids.get(key, 0) + 1
                return self.ids[key]
            def close(self):
                pass
        db = DB()
        api = SimpleNamespace(
            dbcreate=lambda path, schema, dbpath=None: Path(path).write_text('schema ' + schema),
            dbopen=lambda *args: db,
        )
        return db, patch.dict('sys.modules', {'antelope': SimpleNamespace(datascope=api)})

    def test_css_unlocated_records_have_no_invented_origin(self):
        db, antelope = self.mock_datascope()
        with tempfile.TemporaryDirectory() as tmp, antelope:
            path = Path(tmp) / 'pinatubo'
            export_antelope_css3(self.catalog(), path)
            self.assertEqual(len(db.rows['event']), 7)
            self.assertEqual(len(db.rows['origin']), 4)
            self.assertEqual(sum(row['prefor'] == -1 for row in db.rows['event']), 3)
            self.assertEqual(len(db.rows['arrival']), 4)
            self.assertTrue(all(row['timedef'] == 'n' for row in db.rows['assoc']))
            self.assertNotIn('wfdisc', db.rows)
            with self.assertRaises(FileExistsError):
                export_antelope_css3(self.catalog(), path)

    def test_css_membership_written_once_after_all_events(self):
        db, antelope = self.mock_datascope()
        writes = []
        real_write_text = Path.write_text
        def counting_write_text(self, *args, **kwargs):
            writes.append(self.name)
            return real_write_text(self, *args, **kwargs)
        with tempfile.TemporaryDirectory() as tmp, antelope, patch.object(Path, 'write_text', counting_write_text):
            path = Path(tmp) / 'pinatubo'
            info = export_antelope_css3(self.catalog(), path)
            self.assertEqual(writes.count('pinatubo_membership.json'), 1)
            rows = json.loads(Path(info['membership']).read_text())
            self.assertEqual(len(rows), 7)
            self.assertEqual({r['event_class'] for r in rows}, set(EVENT_CLASS_FLAGS))
            self.assertFalse((Path(tmp) / 'pinatubo_membership_partial.json').exists())

    def test_css_failed_export_leaves_only_partial_membership(self):
        catalog = self.catalog()
        # A pick without a station cannot become a CSS arrival: the export must stop.
        catalog[-2].picks.append(Pick(time=UTCDateTime(1991, 6, 3), phase_hint='P',
                                      waveform_id=WaveformStreamID(network_code='XB', station_code='')))
        db, antelope = self.mock_datascope()
        with tempfile.TemporaryDirectory() as tmp, antelope:
            path = Path(tmp) / 'pinatubo'
            with self.assertRaises(ValueError):
                export_antelope_css3(catalog, path)
            self.assertFalse((Path(tmp) / 'pinatubo_membership.json').exists())
            partial = json.loads((Path(tmp) / 'pinatubo_membership_partial.json').read_text())
            self.assertEqual(partial['status'], 'failed')
            self.assertEqual(partial['events_written'], len(catalog) - 2)

    def test_css_assoc_rows_share_one_remark_per_event(self):
        catalog = self.catalog()
        event = catalog[0]  # W_P_H: located, one pick
        event.picks.append(Pick(time=UTCDateTime(1991, 6, 3, 0, 0, 1), phase_hint='S',
                                waveform_id=WaveformStreamID(seed_string='XB.PI2..EHZ')))
        db, antelope = self.mock_datascope()
        with tempfile.TemporaryDirectory() as tmp, antelope:
            export_antelope_css3(Catalog([event]), Path(tmp) / 'pinatubo')
        self.assertEqual(len(db.rows['assoc']), 2)
        self.assertEqual(len({row['commid'] for row in db.rows['assoc']}), 1)


class InventoryCSSExportTests(unittest.TestCase):
    def test_real_obspy_inventory_export_and_source_preservation(self):
        from obspy.core.inventory import Inventory, Network, Station, Channel, Site
        exporter = _load_pipeline_notebook('82_export_antelope_css3.ipynb').export_stationxml_css
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            channel = Channel('EHZ', '', 15., 120., 720., 12., azimuth=0., dip=-90.,
                              sample_rate=100., start_date=UTCDateTime(1991, 6, 1))
            station = Station('PI2', 15., 120., 720., site=Site('PI2'), channels=[channel])
            inventory = Inventory([Network('XB', stations=[station])], source='test')
            source = root / 'source.xml'
            inventory.write(str(source), format='STATIONXML')
            original = source.read_bytes()
            prefix = root / 'network' / 'pinatubo'
            report = exporter(source, prefix)
            self.assertEqual(len(report['channel_epochs']), 1)
            self.assertFalse(report['response_exported'])
            row = prefix.with_suffix('.sitechan').read_text().splitlines()[0]
            self.assertAlmostEqual(float(row[48:57]), .012)
            self.assertAlmostEqual(float(row[65:71]), 0.)
            self.assertEqual(source.read_bytes(), original)
            self.assertEqual((root / 'network/pinatubo_source.xml').read_bytes(), original)
            with self.assertRaises(FileExistsError):
                exporter(source, prefix)
