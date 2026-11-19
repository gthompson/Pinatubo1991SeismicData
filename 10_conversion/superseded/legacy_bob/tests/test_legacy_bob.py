"""Guard routing and historical count anomalies without archive dependencies."""
import sys,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from legacy_bob import counts
from legacy_bob.runner import run

class LegacyBOBTests(unittest.TestCase):
    def test_count_quality_and_source_preservation(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);source=root/'source';source.mkdir()
            data='13 JUL 19 06:30 3.\n19 JUL 91 13:30 4.\n19 JUL 91 13:30 8.\n20 SEO 91 04:30 1.\n21 AUG 91 21:30 OUT OF ORDER\n22 AUG 91 01:30\n'
            for name in ['CAB_CNT.DAT','CAB_HR.DAT']:(source/name).write_text(data)
            (source/'PIE_CNT.DAT').write_text('29 APR 91 00 00 -999.0\n03 JUN 91 00.00 35.0\n')
            result=counts.run(source,root/'output')
            self.assertEqual(result['CAB_CNT.DAT']['status_counts'],{'unexpected_year':1,'numeric':2,'invalid_datetime':1,'unparsed':1,'missing_value':1})
            self.assertEqual(result['PIE_CNT.DAT']['status_counts'],{'sentinel_minus999':1,'numeric':1})
            self.assertEqual((source/'CAB_CNT.DAT').read_text(),data)
    def test_preflight_does_not_create_outputs(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            paths={'LEGACY_TOP':str(root/'missing'),'OUTPUT_ROOT':str(root/'output')}
            with patch('legacy_bob.runner.pipeline_paths',return_value=paths),patch('legacy_bob.runner.load_conversion_config',return_value={}):
                with self.assertRaises(FileNotFoundError):run(products=['counts'])
            self.assertFalse((root/'output').exists())
    def test_unknown_product_rejected(self):
        with patch('legacy_bob.runner.pipeline_paths',return_value={}),patch('legacy_bob.runner.load_conversion_config',return_value={}):
            with self.assertRaises(ValueError):run(products=['unknown'])
if __name__=='__main__':unittest.main()
