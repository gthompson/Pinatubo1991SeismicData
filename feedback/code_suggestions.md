# Code suggestions for the validation work package
# Based on reading the full archive: notebooks, lib/, clock_regimes.csv, TIMING_AND_CLOCKS.md

These are concrete, targeted suggestions for each of the seven Code items. They are written
against the actual code — not generic advice. Where a function already exists and needs only
a wrapper or new notebook cell, that is noted.

---

## 1. Clock anchor residual plot and additional anchors (step 72)

**What already exists:** `72_external_clock_anchors.ipynb` is complete and well-structured.
It downloads ISC/USGS, matches local events, checks SSAM, and plots analyst-reviewed records.
The `REVIEW` DataFrame in section 8 already has the six confirmed anchors with their offsets.

**What is missing:**

### 1a. Residual-vs-date plot
Add a new cell after section 8 that plots the six anchor offsets on a timeline:

```python
import matplotlib.pyplot as plt
import pandas as pd

review_valid = review[review.onset_after_predicted_p_s.notna()].copy()
review_valid['date'] = pd.to_datetime(review_valid.origin).dt.normalize()

fig, ax = plt.subplots(figsize=(9, 3.5))
ax.axhline(0, color='k', lw=0.5, ls='--')
ax.axhspan(-25, 25, color='steelblue', alpha=0.10, label='±25 s nominal uncertainty')
ax.axhspan(-60, 60, color='orange', alpha=0.07, label='±60 s pre-reset')

# Mark the reset date
reset = pd.Timestamp('1991-05-26')
ax.axvline(reset, color='gray', lw=1, ls=':')
ax.text(reset, ax.get_ylim()[0] + 2, '26 May reset', ha='left', fontsize=8, color='gray')

for _, r in review_valid.iterrows():
    color = 'tab:red' if r.date < reset else 'tab:blue'
    ax.scatter(r.date, r.onset_after_predicted_p_s, color=color, s=50, zorder=5)
    ax.annotate(f"{r.record}\n({r.verdict})", (r.date, r.onset_after_predicted_p_s),
                textcoords='offset points', xytext=(4, 4), fontsize=7)

ax.set_ylabel('Apparent clock offset (s)\n(positive = clock fast)')
ax.set_xlabel('Date (1991)')
ax.set_title('Clock anchors: observed arrival minus predicted arrival')
ax.legend(fontsize=8)
fig.tight_layout()
fig.savefig(OUT / 'clock_anchor_residuals.png', dpi=150)
plt.show()
```

### 1b. Mark observed arrivals on the existing anchor plots
In `plot_records()`, the red line marks the PREDICTED first P. Add a green line for the
OBSERVED first arrival, using the `onset_after_predicted_p_s` from the REVIEW table:

```python
# In plot_records(), after drawing the predicted-phase lines, add:
# (pass review_row to the function, or look it up by record ID)
if hasattr(r, 'onset_after_predicted_p_s') and pd.notna(r.onset_after_predicted_p_s):
    obs = p + r.onset_after_predicted_p_s
    ax.axvline(obs, color='green', lw=1.5, ls='-')
    ax.text(obs, len(st) - 0.3, 'obs.', color='green', fontsize=7)
```

### 1c. Additional anchors for October–December
The existing `match_records()` function already finds candidates. The issue is that after
October most of the records are outside the period with hypocenter data. Run the existing
section 7 with `DOWNLOAD=True` to refresh the catalogue, then lower the magnitude threshold
in `shown` to 4.0 to get more candidates for analyst review:

```python
shown = matches[(matches.mag >= 4.0) & matches.predicted_p_in_record_s.between(-60, 75)].sort_values('origin')
```

Add the reviewed results to the REVIEW DataFrame with their dates. Even 1–2 post-October
anchors would substantially improve the temporal coverage of the residual plot.

---

## 2. Hypocenter grouping sensitivity (step 43)

**What already exists:** `43_associate_hypocenters.ipynb` reads `TIME_TOL_S` and
`DIST_TOL_KM` from `conversion_config.json`. The `run()` function is fully parameterized.

**Suggested addition — a sensitivity notebook `43b_grouping_sensitivity.ipynb`:**

```python
import itertools, pandas as pd
from pathlib import Path

# Read inputs once
sources = { ... }  # same as step 43 SETTINGS

results = []
for time_tol, dist_tol in itertools.product([2, 5, 10], [5, 10, 15, 20, 30]):
    ev, orig = run(
        hypo40=..., hypo41=..., extra_sources=sources,
        time_tol=time_tol, dist_tol=dist_tol,
        origin_priority=('pepi','brun','gold','hypo40','pinaall'),
        preferred_magnitude_sources=('pinaall','hypo40'),
        one_origin_per_source=False, zero_magnitude_is_missing=True,
        out_event_csv=None, out_origin_csv=None, emit_diagnostics=False,
    )
    n_groups      = len(ev)
    n_singletons  = (ev.n_origins == 1).sum()
    n_multi_same  = (ev.sources.str.count(r'\+') > 0).sum()  # groups with >1 source
    # Groups with >1 origin from the same source (duplicates within b-run.sum)
    n_brun_multi  = (orig.groupby('event_id').apply(
                       lambda g: (g.source=='brun').sum()) > 1).sum()
    # Maximum temporal separation within any group
    max_dt = orig.groupby('event_id').apply(
        lambda g: (pd.to_datetime(g.origin_time, utc=True).max() -
                   pd.to_datetime(g.origin_time, utc=True).min()).total_seconds()).max()
    results.append(dict(time_tol=time_tol, dist_tol=dist_tol, n_groups=n_groups,
                        n_singletons=n_singletons, n_multi_same_src=n_multi_same,
                        n_brun_multi=n_brun_multi, max_within_group_dt_s=max_dt))

df = pd.DataFrame(results)
print(df.to_string())
# Save and plot as a heatmap of n_groups vs (time_tol, dist_tol)
```

**Transitive chaining check:** The current algorithm is greedy (anchored on the earliest
unassigned origin). It does NOT use transitive chaining — group membership is determined
solely by proximity to the first member. This should be stated explicitly in the manuscript
and is worth verifying:

```python
# After the sensitivity run at the current settings (5s, 15km):
# Check: are there any groups where non-anchor members are >5s or >15km from each other
# (i.e., only within tolerance of the anchor, not of each other)?
def check_transitive(ev_df, orig_df, time_tol, dist_tol):
    from obspy.geodetics import locations2degrees, degrees2kilometers
    import pandas as pd
    issues = []
    for eid, group in orig_df.groupby('event_id'):
        times = pd.to_datetime(group.origin_time, utc=True)
        anchor_t = times.min()
        for i, row_i in group.iterrows():
            for j, row_j in group.iterrows():
                if i >= j: continue
                dt = abs((times[i] - times[j]).total_seconds())
                dist = degrees2kilometers(locations2degrees(
                    row_i.latitude, row_i.longitude,
                    row_j.latitude, row_j.longitude))
                if dt > time_tol or dist > dist_tol:
                    issues.append(dict(event_id=eid, dt_s=round(dt,1), dist_km=round(dist,1)))
    return pd.DataFrame(issues)
```

---

## 3. Association sensitivity and negative control (steps 30–35 / 50)

**What already exists:** The association logic is in `lib/shared.py`
(`associate_hypocenters`). The waveform–pick association is in steps 30 and 32.

**Suggested approach — tolerance sweep in step 50:**

The `associate_hypocenters` function signature accepts a tolerance parameter.
Add a cell to `50_build_obspy_catalog.ipynb` that re-runs the hypocenter-to-event
association with varying tolerances and reports matched/ambiguous/unmatched counts:

```python
# Step 50 already builds the catalog. For the sensitivity sweep, only the
# hypocenter association step needs to vary; waveform and pick associations are fixed.

from lib.shared import associate_hypocenters

for tol_s in [5, 10, 20, 30, 60]:
    result = associate_hypocenters(events=event_index, origins=origin_index,
                                   time_tol_s=tol_s)
    matched   = (result.linked_event_id.notna()).sum()
    ambiguous = (result.clock_status.str.contains('ambiguous', na=False)).sum()
    unmatched = result.linked_event_id.isna().sum()
    print(f"tol={tol_s:3d}s: matched={matched}, ambiguous={ambiguous}, unmatched={unmatched}")
```

**Negative control (time-shifted):**
Shift all origin times by +30 minutes before running the association, then report
the number of "matches." This estimates chance associations at the current event rate:

```python
import pandas as pd

origin_shifted = origin_index.copy()
origin_shifted['origin_time'] = (
    pd.to_datetime(origin_shifted.origin_time, utc=True) + pd.Timedelta(minutes=30)
).dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

result_shifted = associate_hypocenters(events=event_index, origins=origin_shifted,
                                       time_tol_s=10)
chance_matches = result_shifted.linked_event_id.notna().sum()
print(f"Chance associations (30-min shift, tol=10s): {chance_matches}")
print(f"True associations (tol=10s): {matched_10s}")
```

---

## 4. Travel-time residuals (new notebook `47b_travel_time_residuals.ipynb`)

**What already exists:** The HYPO71 velocity model is in
`10_conversion/hypo71_settings_199106.txt`. The merged pick index has P and S arrivals.
The hypocenter origin index has origin times and locations. ObsPy TauP can compute
predicted arrivals.

**Suggested notebook structure:**

```python
# 1. Load the 1D model from hypo71_settings_199106.txt
layers = [(0.0, 3.30), (1.0, 5.00), (4.0, 5.70), (15.0, 6.70), (25.0, 8.00)]
# Build a custom TauPy model, or use a lookup table with the HYPO71 approximation.
# TauPy doesn't directly accept arbitrary 1D models in the CLI interface,
# but can via obspy.taup.taup_create or via VelocityModel.

from obspy.taup import TauPyModel
# Use iasp91 as an approximation for regional distances <50 km,
# or build the model properly with taup_create.

# 2. For each event with both picks and an origin:
picks = pd.read_csv(P['MERGED_PICK_INDEX'], low_memory=False)
origins = pd.read_csv(P['HYPO71_ORIGIN_INDEX'])

# Join: picks -> origins on event_id
# For each P or S pick at a known station, compute predicted travel time
# using origin lat/lon/depth and station coordinates from the StationXML.

# 3. O-C = observed_pick_time - (origin_time + predicted_travel_time)
# Plot O-C by: station, clock_regime, month, source (individual vs monthly)
# Flag |O-C| > 3s as potential errors.
```

**Key point from the code review:** The velocity model in `hypo71_settings_199106.txt`
used `Vp/Vs = 1.78` (not 1.73 as in Ramos 1999). The control card uses HYPO71's
`TEST(2)` etc. Station coordinates are in the control cards and also in the StationXML.
Use the step-70 StationXML station list for coordinates.

---

## 5. b-run.sum duplicate and regional origin diagnostics (step 43 output)

**What already exists:** Step 43 produces `out_origin_csv` with all origins including
their source and coordinates. The `one_origin_per_source=False` setting means b-run.sum
duplicates are both in this file.

**Suggested additions to `42_compare_hypocenter_indexes.ipynb`:**

```python
# a. Duplicate pairs within b-run.sum
brun = origins[origins.source == 'brun'].copy()
brun_sorted = brun.sort_values('origin_time')
# Find rows in same event group with the same source
brun_dupes = origins[(origins.source == 'brun') &
                     origins.duplicated(subset=['event_id'], keep=False)]
brun_dupe_pairs = (brun_dupes.groupby('event_id')
    .apply(lambda g: pd.Series({
        'dt_s': abs((pd.to_datetime(g.origin_time, utc=True).max() -
                     pd.to_datetime(g.origin_time, utc=True).min()).total_seconds()),
        'dlat': (g.latitude.max() - g.latitude.min()),
        'dlon': (g.longitude.max() - g.longitude.min()),
        'ddepth_km': abs(g.depth_km.max() - g.depth_km.min()),
        'dmag': abs(g.magnitude.max() - g.magnitude.min()),
    })))
print(f"b-run.sum duplicate pairs: {len(brun_dupe_pairs)}")
print(brun_dupe_pairs.describe())

# b. Regional origins (>80 km from summit)
from obspy.geodetics import locations2degrees, degrees2kilometers
summit = (15.130, 120.350)
origins['dist_km'] = origins.apply(lambda r: degrees2kilometers(
    locations2degrees(summit[0], summit[1], r.latitude, r.longitude)), axis=1)

regional = origins[origins.dist_km > 80].copy()
print(f"\nOrigins >80 km from summit: {len(regional)}")
print(regional.source.value_counts())

# Check if any regional brun origin has a co-grouped local HYPO71 origin
regional_brun = regional[regional.source == 'brun']
regional_with_local = regional_brun[
    regional_brun.event_id.isin(
        origins[(origins.source.isin(['hypo40','pinaall'])) &
                (origins.dist_km < 80)].event_id
    )
]
print(f"\nRegional b-run origins co-grouped with a local HYPO71 origin: {len(regional_with_local)}")
# These are the most problematic for preferred-origin selection
```

---

## 6. Post-failure channel inspection

**What already exists:** `71_validate_network_history.ipynb` checks that all 152,933
archived traces fall inside a channel epoch. The StationXML has `label_after_documented_end`
epochs for BUR, GRN, PIE, PPO, UBO from ~15–21 June onward.

**Suggested addition to `11_waveform_timeseries_diagnostics.ipynb`:**

```python
# Select traces from the 'label_after_documented_end' epochs
stationxml = read_inventory(P['STATIONXML_PATH'])
after_end_channels = [
    (net.code, sta.code, cha.code)
    for net in stationxml
    for sta in net
    for cha in sta
    if cha.extra and 'label_after_documented_end' in str(cha.extra)
]

# For each such channel, pick 3 random waveform files and compare:
# a. PSD against a known-operational channel (e.g., BUGZ) on the same day
# b. Inter-station coherence with an operational channel
# c. Amplitude distribution (a disconnected input is typically near-flat)

for net, sta, cha in after_end_channels[:5]:  # start with first 5
    # Find files containing this channel after its documented end
    candidate_files = waveform_index[
        waveform_index.channel_list.str.contains(f'{sta}{cha[-1]}', na=False) &
        (pd.to_datetime(waveform_index.starttime) > pd.Timestamp('1991-06-25'))
    ].sample(min(3, len(...)))
    # Read and plot spectra
    for _, row in candidate_files.iterrows():
        st = obspy.read(row.wav_file)
        # ... compute and plot PSD
```

---

## 7. Waveform-only record sampling

**What already exists:** The waveform index has event class (W_ONLY, W+P, etc.).
The 13,597 waveform-only containers are identifiable from `50_build_obspy_catalog.ipynb`.

**Suggested addition — a new cell in `33_plot_event_association_diagnostics.ipynb`
or a standalone sampling notebook:**

```python
import random

# Stratified sample: ~50 events from each of 4 strata
# 1. May (before eruption, low seismicity)
# 2. 10-16 June (preeruptive swarm, busy)
# 3. July-August (post-eruption, moderate)
# 4. September-November (late, sparse)

wonly = events[events['class'] == 'W_ONLY'].copy()
wonly['month'] = pd.to_datetime(wonly.starttime, utc=True).dt.month

strata = {
    'May': wonly[wonly.month == 5],
    'June10-16': wonly[pd.to_datetime(wonly.starttime, utc=True).between(
        '1991-06-10', '1991-06-17')],
    'JulAug': wonly[wonly.month.isin([7, 8])],
    'SepNov': wonly[wonly.month.isin([9, 10, 11])],
}

rng = random.Random(42)
sample = pd.concat([
    df.sample(min(50, len(df)), random_state=42)
    for df in strata.values()
])
print(f"Sample: {len(sample)} waveform-only events across strata")
print(sample.groupby('month').size())

# For each sampled event: load waveform, bandpass 1-10 Hz, 
# and classify by eye (or by simple criteria):
# - impulsive HF onset -> VT earthquake
# - emergent LF signal -> LP or hybrid
# - sustained uniform signal -> tremor or noise
# - flat / single-channel artifact
# Write classifications to a CSV for the paper.

# Automated pre-screening (not a substitute for visual review):
for _, ev in sample.iterrows():
    st = obspy.read(ev.wav_file)
    for tr in st:
        # Simple frequency index: ratio of energy above/below 2 Hz
        from scipy.signal import welch
        f, pxx = welch(tr.data, fs=tr.stats.sampling_rate, nperseg=256)
        fi = pxx[f > 2].sum() / (pxx.sum() + 1e-9)
        # fi > 0.7 -> likely HF/VT; fi < 0.3 -> likely LF/noise
```

---

## Summary of what does and does not need new data products

| Item | Pipeline change needed? | New notebook? | Output needed for paper |
|------|------------------------|---------------|------------------------|
| Clock residual plot | No — add cell to 72 | No | `clock_anchor_residuals.png` for supplement |
| Mark observed arrivals | Modify `plot_records()` in 72 | No | Updated `candidate_records.png` |
| Grouping sensitivity | No — parameterized `run()` | Yes: `43b` | Table of n_groups vs thresholds |
| Transitive chaining check | No | Yes: `43b` | Count of non-pairwise-within-tolerance groups |
| Association tolerance sweep | No — add cells to 50 | No | Table + text in manuscript |
| Negative control | No — add cell to 50 | No | Chance-match count |
| Travel-time residuals | Need 1D model setup | Yes: `47b` | O-C histogram by station/month |
| b-run diagnostics | No — add cells to 42 | No | Duplicate-pair stats table |
| Post-failure inspection | No | Add cells to 11 | PSD comparison figure |
| Waveform-only sampling | No | Add cells to 33 | Classification table |

**Files that need outputs attached to the manuscript or supplement:**
1. `clock_anchor_residuals.png` — Figure S4b or new figure in supplement Section S4
2. Grouping sensitivity table — supplement Section S7 (catalog checks)
3. Association negative-control result — one sentence in Limitations
4. O-C residual summary — supplement Section S7
5. b-run duplicate-pair statistics — one sentence in Limitations
6. Waveform-only classification summary — one parenthetical in the linked catalog section

---

## Numbers to verify before the manuscript is finalized

The reviewer flagged that some numbers in the response letters did not match the code.
Here are the ones that need confirming from actual pipeline outputs:

| Claim | Where stated | What to check |
|-------|-------------|---------------|
| 1,413 step-32 suppressed duplicates | Main TeX, R2 response | `32_waveform_pick_event_map_suppressed_duplicates.csv` row count |
| 593 step-22 suppressed duplicates | Main TeX | `22_merged_picks_suppressed.csv` row count |
| 100% of 153 Jones et al. raw times match HYPO71 | Main TeX | `clock_regimes.csv` comment: "PEPI 153 of 153" — confirmed |
| 7,167 hypocenter groups | Main TeX | `43_hypo_event_index.csv` row count |
| 21,923 total catalog events | Main TeX + abstract | `50_catalog_events.csv` row count |
| 5 s and 15 km grouping thresholds | Main TeX | `conversion_config.json` TIME_TOL_S and DIST_TOL_KM |
| 97.3% hypocenters inside waveform | Main TeX | Reconfirm from step 43/50 output after any rerun |
