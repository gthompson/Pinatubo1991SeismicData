"""Worked example for the SRL paper: 1-15 June 1991 on one UTC time axis.

Panels: (a) frequency index and (b) spectral centroid of each triggered event
window at PIEZ and GRNZ; (c) HYPO71 preferred magnitudes; (d) RSAM at PIEZ and
GRNZ; (e) SSAM bands BN01-BN10 (0.5-10.5 Hz).

Inputs (defaults are the local processing products):
  window_metrics.csv    analysis run (20_analysis), one row per trace window
  43_hypo71_event_index.csv
  BOB rsam_records.csv and ssam_records.csv (UTC)

    python scripts/fig_june_frequency.py
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path.home() / "work/Pinatubo1991Processing"
RUN = ROOT / "analysis/notebook_workflow/45e939498e85"
UTC = ROOT / "utc"
OUT = Path(__file__).resolve().parent.parent / "figures"

T0, T1 = pd.Timestamp("1991-06-01"), pd.Timestamp("1991-06-16")
STATIONS = {"PIE": "#1b7837", "GRN": "#762a83"}
SNR_DB = 6.0
ROLL = "12h"  # centred running-median window (needs >= 5 events)
# Eruption times (UTC) from Wolfe and Hoblitt (1996): first large explosive
# eruption 12 June 08:51 PHT; climactic eruption 15 June ~13:42 PHT.
MARKS = [(pd.Timestamp("1991-06-12 00:51"), "12 Jun"), (pd.Timestamp("1991-06-15 05:42"), "15 Jun")]


def utc(s):
    return pd.to_datetime(s, utc=True, format="ISO8601").dt.tz_convert(None)


cols = ["window", "station", "channel", "recorded_starttime", "metric_flags",
        "event_to_noise_rms_db", "sam.log2_hf_lf_ratio", "psd.centroid_hz"]
w = pd.read_csv(RUN / "window_metrics.csv", usecols=cols, low_memory=False)
w = w[(w.window == "event") & w.station.isin(STATIONS) & (w.channel.str[-1] == "Z")]
w = w.assign(t=utc(w.recorded_starttime))
w = w[(w.t >= T0) & (w.t < T1)]
n_all = len(w)
clipped = w.metric_flags.str.contains("clipping", na=False)
w = w[~clipped & (w.event_to_noise_rms_db > SNR_DB) & np.isfinite(w["sam.log2_hf_lf_ratio"])]
w = w.sort_values("t")

h = pd.read_csv(UTC / "pipeline_tmp/43_hypo71_event_index.csv")
h = h.assign(t=utc(h.preferred_origin_time))
h = h[(h.t >= T0) & (h.t < T1)]
hm = h[h.preferred_magnitude > 0]

r = pd.read_csv(UTC / "FAIR/BOB/rsam/rsam_records.csv",
                usecols=["series", "recorded_time", "value_01"], low_memory=False)
r = r.assign(t=pd.to_datetime(r.recorded_time, errors="coerce"))
r = r[(r.t >= T0) & (r.t < T1) & r.series.isin([s + "Z" for s in STATIONS])]

band = [f"value_{i:02d}" for i in range(1, 11)]
s = pd.read_csv(UTC / "FAIR/BOB/ssam/ssam_records.csv",
                usecols=["series", "recorded_time"] + band, low_memory=False)
s = s.assign(t=pd.to_datetime(s.recorded_time, errors="coerce"))
s = s[(s.t >= T0) & (s.t < T1)].sort_values("t")
# One SSAM station at a time: BUR1 until it stops, then CAB1.
s = pd.concat([s[(s.series == "BUR1")], s[(s.series == "CAB1") & (s.t > s[s.series == "BUR1"].t.max())]])

plt.rcParams.update({"font.size": 8, "font.family": "sans-serif",
                     "axes.spines.top": False, "axes.spines.right": False})
fig, ax = plt.subplots(5, 1, figsize=(7.2, 8.4), sharex=True,
                       gridspec_kw=dict(height_ratios=[1.2, 1.2, 0.9, 0.9, 1.0]))

for key, a, lab in [("sam.log2_hf_lf_ratio", ax[0], "Frequency index\nlog$_2$(4–20 Hz / 0.8–4 Hz)"),
                    ("psd.centroid_hz", ax[1], "Spectral centroid (Hz)")]:
    for sta, c in STATIONS.items():
        d = w[w.station == sta]
        a.scatter(d.t, d[key], s=1.5, color=c, alpha=0.25, lw=0)
        med = d.set_index("t")[key].rolling(ROLL, center=True, min_periods=5).median()
        gap = med.index.to_series().diff() > pd.Timedelta("6h")
        med[gap] = np.nan  # break the line across recording gaps
        a.plot(med.index, med.values, color=c, lw=1.2, label=f"{sta}Z (n = {len(d):,})")
    a.set_ylabel(lab)
ax[0].set_ylim(-3, 3)
ax[1].set_ylim(0, 20)
ax[0].legend(loc="lower left", frameon=False, ncol=2)

ax[2].scatter(hm.t, hm.preferred_magnitude, s=4, color="k", lw=0, alpha=0.6)
ax[2].set_ylabel("Magnitude\n(HYPO71)")

for sta, c in STATIONS.items():
    d = r[r.series == sta + "Z"]
    ax[3].plot(d.t, d.value_01, color=c, lw=0.7, label=f"{sta}Z")
ax[3].set_yscale("log")
ax[3].set_ylabel("RSAM\n(10 min, counts)")
ax[3].legend(loc="upper left", frameon=False, ncol=2)

z = np.log10(s[band].where(s[band] > 0).to_numpy(float)).T
edges = np.arange(0.5, 11.0, 1.0)
tnum = mdates.date2num(s.t)
tedge = np.r_[tnum, tnum[-1] + 1 / 1440]
pc = ax[4].pcolormesh(tedge, edges, z, shading="flat", cmap="magma",
                      vmin=np.nanpercentile(z, 2), vmax=np.nanpercentile(z, 99))
ax[4].set_ylabel("SSAM band (Hz)")
cax = ax[4].inset_axes([1.01, 0.0, 0.012, 1.0])
fig.colorbar(pc, cax=cax, label="log$_{10}$ amplitude")

for a in ax:
    for tm, lab in MARKS:
        a.axvline(tm, color="r", lw=0.6, ls="--")
for tm, lab in MARKS:
    ax[0].text(tm, 3.1, lab, color="r", ha="center", va="bottom", fontsize=7)
ax[-1].set_xlim(T0, T1)
ax[-1].xaxis.set_major_locator(mdates.DayLocator(interval=2))
ax[-1].xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
ax[-1].set_xlabel("Date (UTC), 1991")
for a, l in zip(ax, "abcde"):
    a.text(-0.08, 1.0, f"({l})", transform=a.transAxes, fontweight="bold", va="top")
fig.align_ylabels(ax)
fig.tight_layout()
fig.savefig(OUT / "fig_june_frequency.pdf")
fig.savefig(OUT / "fig_june_frequency.png", dpi=200)

print(f"windows {n_all} (Z, PIE+GRN) -> kept {len(w)}; clipped {int(clipped.sum())}; "
      f"per station {w.station.value_counts().to_dict()}")
print(f"hypocenters {len(h)}, with magnitude {len(hm)}; last waveform {w.t.max()}; "
      f"SSAM {s.t.min()} to {s.t.max()} ({s.series.unique().tolist()})")
for sta in STATIONS:
    d = w[w.station == sta]
    early, late = d[d.t < "1991-06-06"], d[d.t >= "1991-06-09"]
    print(sta, "FI median 1-5 Jun", round(early["sam.log2_hf_lf_ratio"].median(), 2),
          "9-14 Jun", round(late["sam.log2_hf_lf_ratio"].median(), 2),
          "| centroid", round(early["psd.centroid_hz"].median(), 1), round(late["psd.centroid_hz"].median(), 1))
