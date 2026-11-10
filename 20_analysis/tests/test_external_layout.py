from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[2] / "10_conversion")]
from tempfile import TemporaryDirectory
import os,json,unittest
from unittest.mock import patch
import pinatubo_project as p

class ExternalLayoutTests(unittest.TestCase):
    def test_stage_outputs_under_one_root(self):
        root=p.settings()['results_root']
        for stage in ['10_conversion','20_analysis','testing','runtime']:
            self.assertTrue(p.output_path(stage).is_relative_to(root))
        from pinatubo_steps.config import pipeline_paths
        paths=pipeline_paths()
        for key in ['SEISAN_WAV_DB','TMP_DIR','QC_DIR','FAIR_META_DIR','WAVEFORM_INDEX','CATALOG_PREFIX','FAIR_REA_DIR','SEISAN_DIAG_DIR']:
            self.assertTrue(Path(paths[key]).is_relative_to(root),key)
        self.assertFalse(Path(paths['SUDS_TOP']).is_relative_to(root))
    def test_existing_absolute_artifact_paths_rebase(self):
        old=p.PROJECT_ROOT/'20_analysis/products/event_metrics_cache/record/version/event.json'
        dest=p.output_path('20_analysis','event_metrics_cache/record/version/event.json')
        dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text('{}')
        self.assertEqual(p.resolve_artifact(old),dest)
        waveform=p.input_path('waveform_root')/'1991/06/file'
        self.assertEqual(p.resolve_artifact(waveform),waveform)
    def test_unavailable_volume_does_not_fall_back_to_repository(self):
        with patch.dict(os.environ,{'PINATUBO_RESULTS_ROOT':'/Volumes/DOES_NOT_EXIST_PINATUBO/results'}):
            with self.assertRaises(FileNotFoundError):p.output_path('20_analysis')
    def test_runtime_and_generated_network_data_are_external(self):
        p.runtime_environment();root=p.settings()['results_root']
        for key in ['MPLCONFIGDIR','GMT_USERDIR','GMT_SESSIONDIR','IPYTHONDIR']:
            self.assertTrue(Path(os.environ[key]).is_relative_to(root))
        dest=p.network_products()/'data/waveform_header_presence.csv';dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text('external')
        self.assertEqual(p.network_input(dest.name,Path('/not/source')),dest)
        dest.unlink()
