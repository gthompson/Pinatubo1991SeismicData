"""Scientific and metadata integrity checks, using the complete generated products."""
from pathlib import Path
import unittest
import numpy as np
from obspy import read_inventory,UTCDateTime
from obspy.io.stationxml.core import validate_stationxml
from network_history.reconstruct import epochs,nominal_response,load
ROOT=Path(__file__).parent/'products'
class ReconstructionTests(unittest.TestCase):
 def test_schema_and_unknown_electronics(self):
  for name in ['pinatubo_provisional.xml','pinatubo_nominal_voltage_models.xml']:
   valid,errors=validate_stationxml(str(ROOT/name));self.assertTrue(valid,errors)
  inv=read_inventory(str(ROOT/'pinatubo_provisional.xml'))
  self.assertEqual(len(inv[0]),13)
  self.assertTrue(all(c.response is None for s in inv[0] for c in s))
  self.assertEqual(len(inv.select(station='PI2',channel='ELZ')[0][0]),1)
  self.assertIsNone(inv.select(station='PI2',channel='ELZ')[0][0][0].start_date)
  self.assertEqual(len(inv.select(channel='WA?')[0].get_contents()['channels']),4)
 def test_calibration_response(self):
  for name in ['L4-C','L22-D']:
   a=nominal_response(name,18);b=nominal_response(name,24)
   # Evaluate stages, independently of the descriptive total sensitivity.
   av=a.get_evalresp_response_for_frequencies([5.],output='VEL')[0]
   bv=b.get_evalresp_response_for_frequencies([5.],output='VEL')[0]
   self.assertAlmostEqual(abs(bv/av),10**(-6/20),places=10)
   self.assertAlmostEqual(abs(av),a.instrument_sensitivity.value,places=8)
   self.assertEqual(a.instrument_sensitivity.output_units,'V')
 def test_epoch_integrity_and_low_gain(self):
  rows=epochs()
  for a in rows:
   if a['start'] and a['end']:self.assertLess(UTCDateTime(a['start']),UTCDateTime(a['end']))
   for b in rows:
    if a is b or (a['station'],a['channel'])!=(b['station'],b['channel']):continue
    if a['start'] and b['start'] and UTCDateTime(a['start'])<UTCDateTime(b['start']):
     self.assertIsNotNone(a['end']);self.assertLessEqual(UTCDateTime(a['end']),UTCDateTime(b['start']))
  self.assertTrue(all(r['attenuation_db'] is None for r in rows if r['sensor']=='unknown'))
 def test_coordinate_provenance(self):
  for s in load('stations.json'):
   self.assertTrue(14<s['latitude']<16);self.assertTrue(120<s['longitude']<121)
   self.assertNotIn(s['code'],['DFR','NCT']);self.assertTrue(s['coordinate_source'])
if __name__=='__main__':unittest.main()
