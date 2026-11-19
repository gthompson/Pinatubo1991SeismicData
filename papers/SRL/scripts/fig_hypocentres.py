"""Hypocenter figure for the SRL paper: map, latitude-depth, longitude-depth and depth-time.

Reads the step 43 event index (preferred origins, UTC) and writes
figures/fig_hypocentres.{pdf,png}. Symbols scale with magnitude; magnitudes
of 0.0 and below, and missing magnitudes, are drawn at the smallest size.

    python scripts/fig_hypocentres.py [path/to/43_hypo71_event_index.csv]
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "figures"
SRC = Path(sys.argv[1]) if len(sys.argv) > 1 else (
    Path.home() / "work/Pinatubo1991Processing/utc/pipeline_tmp/43_hypo71_event_index.csv")

SUMMIT = (120.35, 15.13)
LON, LAT, DEP = (120.20, 120.50), (15.00, 15.30), (-3.0, 25.0)


def size(m):
    m = np.where(np.isfinite(m) & (m > 0), m, 0.0)
    return 1.5 * 4.0 ** m


plt.rcParams.update({"font.size": 8, "font.family": "sans-serif"})
e = pd.read_csv(SRC).dropna(subset=["preferred_latitude", "preferred_longitude"])
t = pd.to_datetime(e.preferred_origin_time, utc=True, format="mixed").dt.tz_convert(None)
e = e.assign(t=t, s=size(e.preferred_magnitude.to_numpy(float))).sort_values("t")
c = mdates.date2num(e.t)
kw = dict(c=c, s=e.s, cmap="viridis", lw=0.15, edgecolors="k", alpha=0.7)

fig = plt.figure(figsize=(7.2, 7.0))
gs = fig.add_gridspec(2, 2, width_ratios=[2.2, 1.4], height_ratios=[2.2, 1.1],
                      hspace=0.08, wspace=0.08, left=0.10, right=0.98, top=0.95, bottom=0.16)
amap = fig.add_subplot(gs[0, 0])
alon = fig.add_subplot(gs[1, 0], sharex=amap)
alat = fig.add_subplot(gs[0, 1], sharey=amap)
atim = fig.add_subplot(gs[1, 1], sharey=alon)

sc = amap.scatter(e.preferred_longitude, e.preferred_latitude, **kw)
amap.plot(*SUMMIT, "^", ms=8, mfc="none", mec="r", mew=1.2)
amap.set(xlim=LON, ylim=LAT, ylabel="Latitude (°N)")
amap.tick_params(labelbottom=False)

alon.scatter(e.preferred_longitude, e.preferred_depth_km, **kw)
alon.set(ylim=DEP[::-1], xlabel="Longitude (°E)", ylabel="Depth (km)")

alat.scatter(e.preferred_depth_km, e.preferred_latitude, **kw)
alat.set(xlim=DEP, xlabel="Depth (km)")
alat.xaxis.set_label_position("top")
alat.tick_params(labelleft=False, labelbottom=False, labeltop=True, top=True)

atim.scatter(e.t, e.preferred_depth_km, **kw)
atim.tick_params(labelleft=False)
atim.xaxis.set_major_locator(mdates.MonthLocator())
atim.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
atim.set_xlabel("Origin time (UTC), 1991")

for a in (amap, alon, alat, atim):
    a.grid(lw=0.3, alpha=0.4)
for a in (alon, atim):
    a.axhline(0, color="0.5", lw=0.5)

cax = fig.add_axes([0.10, 0.075, 0.43, 0.013])
cb = fig.colorbar(sc, cax=cax, orientation="horizontal")
cb.ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[5, 6, 7, 8, 9, 10, 11]))
cb.ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
cb.set_label("Origin time (UTC), 1991")

for m in (0, 0.5, 1.0, 1.5):
    atim.scatter([], [], s=size(np.array([m]))[0], c="0.6", edgecolors="k", lw=0.2,
                 label=("≤0 or none" if m == 0 else f"{m:g}"))
fig.legend(*atim.get_legend_handles_labels(), title="Magnitude", ncol=4, frameon=False,
           loc="lower right", bbox_to_anchor=(0.98, 0.0), fontsize=7, title_fontsize=7)

off = (~e.preferred_longitude.between(*LON) | ~e.preferred_latitude.between(*LAT)).sum()
deep = (e.preferred_depth_km > DEP[1]).sum()
fig.savefig(OUT / "fig_hypocentres.pdf")
fig.savefig(OUT / "fig_hypocentres.png", dpi=200)
print(f"n={len(e)} off_map={off} deeper_than_{DEP[1]:g}km={deep} "
      f"with_mag={(e.preferred_magnitude > 0).sum()}")
