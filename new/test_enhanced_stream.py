from obspy import read
from flovopy.enhanced.stream import EnhancedStream

st = read("/Users/glennthompson/Library/CloudStorage/Box-Box/thompsong/DATA/25_028.Pin1991Fair.DATA.FAIR.mseed/SEISAN/WAV/1991/06/1991-06-01-0017-11M.PNTBO_008")

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