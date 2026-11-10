from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pinatubo_project import input_path, output_path, runtime_environment
runtime_environment()
from obspy import read
from flovopy.enhanced.stream import EnhancedStream
source=next(iter(sorted((input_path('waveform_root')/'1991/06').glob('*'))))
st = read(str(source))

for tr in st:
    tr.stats.units = "counts"
print(st)
est = EnhancedStream(st)

est.ampengfft(
    allow_uncalibrated=True,
    compute_sam=True,
    compute_sem=True,
    compute_spectral=True,
    compute_bandratios=True,
    bands=(0.8, 4.0, 4.0, 20.0),
)

for tr in est:
    print(tr.id)
    print(tr.stats.metrics)
from flovopy.enhanced.serialization import atomic_json
atomic_json(output_path('testing','enhanced_stream',create=True)/'metrics.json',
    {'source':str(source),'traces':[{'id':tr.id,'metrics':tr.stats.metrics,
      'spectral':tr.stats.get('spectral',{})} for tr in est]})
