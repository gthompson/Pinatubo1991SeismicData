import sys,tempfile,unittest,zipfile,csv
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from legacy_bob.rain import run
class RainTests(unittest.TestCase):
    def test_zip_formats_missing_and_duplicate_preserved(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);z=root/'rain.zip'
            with zipfile.ZipFile(z,'w') as f:
                f.writestr('RAIN/28JUL91.DAT','DATA CONVERTED FROM GMT TO LOCAL TIME\n28 JUL 91 00 43 -998. 1. 2. 3. 4. 5. 6. 7.\n28-JUL-91 00:43 -998. 1. 2. 3. 4. 5. 6. 7.\n')
                f.writestr('__MACOSX/._28JUL91.DAT','ignored')
            s=run(z,root/'out');self.assertEqual(s['records'],2);self.assertEqual(s['duplicate_timestamp_rows'],2)
            with (root/'out/rain_records.csv').open() as f:rows=list(csv.DictReader(f))
            self.assertEqual(rows[0]['value_01_raw'],'-998.0');self.assertEqual(rows[0]['value_01_missing'],'True')
            self.assertEqual(rows[0]['recorded_time'],'1991-07-28T00:43:00')
            self.assertEqual(rows[0]['time_basis'],'unresolved')
if __name__=='__main__':unittest.main()
