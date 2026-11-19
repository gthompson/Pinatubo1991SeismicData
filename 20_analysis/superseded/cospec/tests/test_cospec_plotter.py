import importlib.util
from pathlib import Path
import tempfile
import unittest

PATH=Path(__file__).resolve().parents[1]/'cospec_plotter.py'
spec=importlib.util.spec_from_file_location('cospec_plotter_under_test',PATH)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class CospecTests(unittest.TestCase):
    def file(self,text):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        p=Path(t.name)/'sample.dat';p.write_bytes(text.encode('cp1252'));return p
    def test_adjacent_value_and_duplicate_times_are_preserved(self):
        p=self.file('COSPEC DATA\r\n(TIME IN GMT)\r\n05 JUN 91 00:00 1341.0\r\n05 JUN 91 00:00  263.0\r\n10 JUN 91 00:0013361.0\r\n\x1a')
        before=p.read_bytes();r=module.read_cospec(p)
        self.assertEqual(r.cospec_value.tolist(),[1341,263,13361])
        self.assertEqual(r.duplicate_timestamp.tolist(),[True,True,False])
        self.assertEqual(r.recorded_time_gmt.dt.year.tolist(),[1991]*3)
        self.assertEqual(r.source_line.tolist(),[3,4,5]);self.assertEqual(p.read_bytes(),before)
    def test_four_digit_year_and_fortran_exponent(self):
        r=module.read_cospec(self.file('10 JUN 1991 00:00 1.3D+4\n'))
        self.assertEqual(r.cospec_value.iloc[0],13000)
    def test_bad_dates_and_rows_raise(self):
        for text in ['32 MAY 91 00:00 42\n','10 XXX 91 00:00 42\n','10 JUN 91 00:00 42\n11 JUN bad\n']:
            with self.subTest(text=text):
                with self.assertRaises(ValueError):module.read_cospec(self.file(text))
    def test_missing_days_and_repeated_values_not_invented(self):
        r=module.read_cospec(self.file('03 JUN 91 00:00 1341\n05 JUN 91 00:00 1341\n'))
        self.assertEqual(len(r),2);self.assertFalse(r.duplicate_timestamp.any())
    def test_header_definitions_are_descriptive_only(self):
        p=self.file('PINATUBO EARTHQUAKES\r\r\nPINATUBO_CHEM:COSP.DAT COS_PEC.DAT (I2,1X,A3,3(1X,I2),F6.1)\x1a')
        d=module.read_bob_plot_definitions(p)
        self.assertEqual(len(d),1);self.assertEqual(d.source_file.iloc[0],'COS_PEC.DAT')
    def test_no_records_is_an_error(self):
        with self.assertRaises(ValueError):module.read_cospec(self.file('COSPEC HEADER\n\x1a'))

if __name__=='__main__':unittest.main()
