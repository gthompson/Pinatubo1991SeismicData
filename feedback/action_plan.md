# Action plan — based on the ChatGPT code audit
# Cross-referenced against Archive.zip

This supersedes the earlier code_suggestions.md. Work from this document.
Items are grouped by what they unblock: some must be done before any validation
results are trusted; others can proceed in parallel.

---

## Immediate corrections before running any new analysis (P0 bugs)

These produce wrong results silently. Fix them first, freeze the corrected state,
then run all subsequent analyses against that frozen baseline.

---

### Fix 1 — `merge_catalogue()` in step 72: duplicate external events

**Bug:** The same ISC event can appear in both the `regional` and `global` query results.
If the two ISC catalogue entries for the same `event_id` have locations more than 1.5°
apart (which happens for some large regional events with multiple ISC origin solutions),
the existing proximity dedup does not merge them and they both appear in the output.
The audit confirmed that `smi:ISC/evid=329289` appears twice in the saved merged catalogue.

**Minimal fix** — add two lines at the end of `merge_catalogue()`, before `return`:

```python
# Deduplicate by external event_id: the proximity merge above handles near-identical
# locations; this catches the same evid at widely separated ISC solutions.
result = result.sort_values('source', key=lambda s: s.map({'ISC': 0, 'USGS': 1}))
result = result.drop_duplicates(subset='event_id', keep='first').reset_index(drop=True)
return result
```

**Test to add** in a new cell or in the existing test suite:
```python
# Two ISC rows with same event_id at >1.5 deg separation should produce one output row.
raw = pd.DataFrame({
    'source': ['ISC','ISC'], 'query': ['regional','global'],
    'time': ['1991-06-15T00:00:00','1991-06-15T00:00:00'],
    'lat': [15.10, 16.75], 'lon': [120.30, 120.30],
    'depth_km': [10., 10.], 'mag': [5.0, 5.0], 'magtype': ['mb','mb'],
    'event_id': ['smi:ISC/evid=329289','smi:ISC/evid=329289'],
})
result = merge_catalogue(raw)
assert len(result) == 1, f"Expected 1 row, got {len(result)}"
assert result.iloc[0]['event_id'] == 'smi:ISC/evid=329289'
```

---

### Fix 2 — `compare_local()` in step 72: mislabelled origin source

**Bug:** Section 4 of step 72 passes `P['HYPO71_ORIGIN_INDEX']` to `compare_local()`.
That index is the step-43 five-source output table (all sources combined), not just
HYPO71. The audit found 4 BRUN-source origins among the 16 matched external events,
so the printed comparison labelled "nearest HYPO71" is sometimes actually the nearest
BRUN or GOLD origin. The timing residuals are therefore from the wrong source.

**Fix** — filter to HYPO71 sources before passing to `compare_local()`:

```python
# In section 4, replace:
origins_df = pd.read_csv(P['HYPO71_ORIGIN_INDEX'])

# With:
origins_all = pd.read_csv(P['HYPO71_ORIGIN_INDEX'])
origins_hypo71 = origins_all[origins_all['source'].isin(['hypo40', 'pinaall'])].copy()
local = compare_local(cat, origins_hypo71, SETTINGS['local_max_km'], SETTINGS['local_match_window_s'])
```

Also update the printed summary to say "HYPO71 (pinaall/hypo40)" explicitly.

---

### Fix 3 — `pair_table()` in step 47: pairs counted as events

**Bug:** The function joins sources by `event_id`, creating one row per origin pair.
When a group has two BRUN origins (193 such groups exist, 36 with differing solutions),
those contribute two rows to the join. The variable `events = len(d)` therefore
counts pairs, not unique earthquake groups. The saved HYPO71-summary/BRUN comparison
has 3,650 pairs from 3,462 groups; the PINAALL/summary comparison has 3,550 pairs
from 3,498 groups.

**Fix** — deduplicate the join before computing group-level statistics:

```python
# After the join, add:
# Count unique events (groups), not pairs
n_unique_events = d['event_id'].nunique()
n_pairs = len(d)
print(f"  {n_unique_events} groups ({n_pairs} origin pairs)")

# For statistics use a deduplicated view — take the preferred origin per source
# (first by sort order, as step 43 does):
d_dedup = d.sort_values('origin_id').drop_duplicates(subset=['event_id', 'source_x'])
```

The histograms and magnitude-offset statistics should be recomputed from the
deduplicated view. Where multiple BRUN origins exist for one event, the same
event would otherwise appear multiple times with different offsets.

---

### Fix 4 — step 47 histograms: `.clip()` moves outliers into endpoint bins

**Bug:** `.clip(*lim)` on the difference arrays before plotting assigns out-of-range
values to the boundary bins, inflating them. The displayed median is computed from
clipped values. This means the stated median differences and the visual distribution
are both distorted.

**Fix** — compute statistics before clipping, then mark overflow counts explicitly:

```python
# Before clipping:
n_overflow = int((np.abs(diff) > lim[1]).sum())
median_unclipped = np.nanmedian(diff)

# For the plot, use clip only for display limits, not for the histogram data:
ax.hist(diff, bins=50, range=lim)  # range= trims without clipping
ax.set_title(f'... median={median_unclipped:.2f}, '
             f'n={len(diff)}, overflow={n_overflow} ({100*n_overflow/len(diff):.1f}%)')
```

---

### Fix 5 — step 72, analyst review: anchor identity keyed on record only

**Bug:** The cell `review = REVIEW.merge(matches.drop_duplicates('record'), ...)` picks
an arbitrary candidate when a record has multiple external event matches. The audit
confirmed one such case in the saved matches. The residual then belongs to whichever
candidate happens to survive `drop_duplicates`.

**Fix** — key on `(record, external_event_id)` explicitly. The REVIEW DataFrame already
has `record` but not `event_id`. Add it:

```python
REVIEW = pd.DataFrame([
    dict(record='9105300K', external_event_id='<ISC_ID>', ...),
    ...
])
review = REVIEW.merge(
    matches.drop_duplicates(['record','origin']),  # 'origin' is the external event time
    on='record', how='left'
)
```

For the one ambiguous case, add it as a second row in REVIEW with its own decision
(confirmed match, ruled out, etc.) rather than silently picking one.

---

## Corrections to manuscript and response letters from the audit

These are factual issues the audit resolved. Update the manuscript TeX and
response letters before the next submission.

### Numbers that have changed between pipeline versions

The audit confirmed the **current** catalog contains:
- **15,692 origins** in **7,167 groups** — not 8,233 origins as cited from an earlier run
- **21,923 catalog containers** — matches what the manuscript currently says ✓
- **593** step-22 pick suppressions (cross-source duplicates) ✓
- **1,413** step-32 pick suppressions (within-event duplicates) ✓

**Action:** Freeze the pipeline at a tagged commit. Regenerate the following counts
from that frozen state and confirm they match the manuscript:
- Total origins across all sources
- Hypocenter groups (7,167 ✓)
- Preferred origins by source (brun/gold/pepi/hypo40/pinaall)
- Total catalog containers (21,923 ✓)
- Waveform files (21,459 ✓)
- Phase picks (58,068 ✓)

### Step references in the response letters

The R2 round-2 response letter says "steps 23 and 35" for pick suppression.
The correct steps are **22 and 32**. Update the response letter.

### Regional origins denominator

The manuscript says "107 origins 49–694 km from the volcano." The audit found:
- **117 origin rows** beyond 49 km
- **107 of those are preferred origins**

The manuscript should say either "107 preferred origins" or "117 origin records."
The current wording is ambiguous. The audit also confirmed that none of the 107
regional preferred origins has a competing local HYPO71 origin in the same group —
the feared case of a regional solution displacing a local one was **not observed**.
This is a positive result worth stating explicitly in the manuscript.

---

## P1 fixes: implement before the validation analyses

### Fix 6 — step 10: propagate `clock_uncertainty_s` to waveform index

The `waveform_time_correction()` callback returns `(offset, label)` but the
waveform index has no `clock_uncertainty_s` column. The uncertainty is in
`clock_regimes.csv` for every regime, so it can be looked up at conversion time:

```python
# In lib/clock.py, waveform_time_correction():
def correct(path, event_id, starttime, header=None):
    marker = (header or {}).get('localtime')
    info = table.lookup('waveform', starttime, event_id, marker)
    # Return uncertainty alongside offset and regime:
    return info['offset_s'], info['regime'], info.get('uncertainty_s')
```

Then step 10 writes `clock_uncertainty_s` to `10_waveform_index.csv`.
This propagates downstream to the QuakeML `time_errors` field, which should already
be reading it from the hypocenter clock; the waveform clock uncertainty needs the
same treatment.

### Fix 7 — step 50 `build_pick()`: propagate interpretable pick fields

The function currently emits only time, phase, waveform_id, resource_id.
The parsed pick record has onset, first_motion, weight, and clock_regime/uncertainty.
Add these as `extra` or as standard ObsPy pick attributes:

```python
# In 50_build_obspy_catalog.ipynb build_pick():
pick.onset = row.get('onset')       # 'e' or 'i'
pick.polarity = row.get('first_motion')  # 'positive', 'negative', 'undecidable'
# Keep historical weight as a comment, not as uncertainty (they are not equivalent):
pick.comments.append(Comment(text=f"historical_weight:{row.get('weight','')}"))
pick.comments.append(Comment(text=f"clock_regime:{row.get('clock_regime','')}"))
```

### Fix 8 — step 81: expose silently skipped files

```python
# In 81_seisan_rea_diagnostics.ipynb run():
# Replace silent exception handling with:
unreadable = []
for spath in s_files:
    try:
        ev = read_events(spath)
        ...
    except Exception as exc:
        unreadable.append({'path': spath, 'error': str(exc)})

if unreadable:
    pd.DataFrame(unreadable).to_csv(OUT / '81_unreadable_sfiles.csv', index=False)
    print(f"WARNING: {len(unreadable)} S-files could not be read — see 81_unreadable_sfiles.csv")
```

### Fix 9 — `lib/shared.py` `associate_hypocenters()`: no-phase KeyError

```python
# Replace:
primary_picks = merged_picks.groupby('event_id').apply(
    lambda g: g[g['phase'].str.upper().str[0] == 'P' if 'phase' in g.columns else False]
)

# With:
if 'phase' not in merged_picks.columns:
    primary_picks = pd.Series(dtype=object)
else:
    p_mask = merged_picks['phase'].astype(str).str.upper().str[0] == 'P'
    primary_picks = merged_picks[p_mask].groupby('event_id')
```

Add a regression test with a picks DataFrame that has no `phase` column.

---

## P0 validation: grouping and preferred origins (after fixes above)

### Add a grouping diagnostic driver around `run()` in step 43

The `run()` function is already parameterized. Add a new script
`validation/hypocenters/grouping_sensitivity.py` that calls it
across a 3×5 grid of (time_tol, dist_tol):

```python
for time_tol in [2, 5, 10]:
    for dist_tol in [5, 10, 15, 20, 30]:
        events, origins = run(time_tol=time_tol, dist_tol=dist_tol, ...)
        record_metrics(time_tol, dist_tol, events, origins)
```

For each combination record:
- `n_groups`, `n_singletons`, `n_multi_source`, `n_brun_multi`
- `n_groups_changed` relative to the baseline (5s, 15km) — requires comparing
  by origin_id, not by sequential event_id
- Maximum within-group time separation (seconds)
- Maximum within-group distance (km)
- Count of preferred origins from each source

**On transitive chaining:** the audit confirmed step 43 uses greedy earliest-anchor,
not connected components. This means members need only be within tolerance of the
anchor, not of each other. The diagnostic above should include:

```python
# Check: are any non-anchor members outside tolerance of each other?
def check_pairwise(events_df, origins_df, time_tol, dist_tol):
    ...  # (code from code_suggestions.md section 2)
```

### Investigate the 36 differing BRUN duplicate pairs

The audit found 193 groups with multiple BRUN origins; 36 have differing solutions
with a maximum depth spread of 12.65 km. Generate a diagnostic CSV:

```python
brun_multi = origins[(origins.source == 'brun')].copy()
brun_multi = brun_multi[brun_multi.duplicated('event_id', keep=False)]
brun_diffs = brun_multi.groupby('event_id').apply(lambda g: pd.Series({
    'n_origins': len(g),
    'dt_s': abs((pd.to_datetime(g.origin_time, utc=True).max() -
                 pd.to_datetime(g.origin_time, utc=True).min()).total_seconds()),
    'ddepth_km': g.depth_km.max() - g.depth_km.min(),
    'ddist_km': ...,  # epicentral distance between solutions
    'dmag': abs(g.magnitude.max() - g.magnitude.min()),
}))
brun_diffs[brun_diffs.ddepth_km > 0].to_csv('brun_duplicate_diffs.csv')
```

The 36 non-identical cases need manual review. Only after that review should
the first-record selection rule be accepted as "scientifically inconsequential."

---

## LaTeX macro approach (high value, low effort)

The audit recommends generating LaTeX macros from pipeline output to prevent
manuscript/pipeline drift. This is good advice and straightforward to implement.

Create `validation/reproducibility/generate_latex_macros.py`:

```python
import pandas as pd, json
from pathlib import Path

# Read from frozen pipeline outputs
events      = pd.read_csv('utc/pipeline_tmp/step50_catalog_events.csv')
origins     = pd.read_csv('utc/pipeline_tmp/43_hypo_origin_index.csv')
picks       = pd.read_csv('utc/pipeline_tmp/22_merged_pick_index.csv')
wav_index   = pd.read_csv('utc/pipeline_tmp/10_waveform_index.csv')
suppressed22 = pd.read_csv('utc/pipeline_tmp/22_merged_picks_suppressed.csv')
suppressed32 = pd.read_csv('utc/pipeline_tmp/32_waveform_pick_event_map_suppressed_duplicates.csv')

macros = {
    'NumberWaveformFiles':     len(wav_index),
    'NumberWaveformTraces':    int(wav_index.n_traces.sum()) if 'n_traces' in wav_index else '?',
    'NumberPhasePicks':        len(picks),
    'NumberPicksSuppressed22': len(suppressed22),
    'NumberPicksSuppressed32': len(suppressed32),
    'NumberHypocenterOrigins': len(origins),
    'NumberHypocenterGroups':  origins.event_id.nunique(),
    'NumberCatalogEvents':     len(events),
    # Preferred origin breakdown
    'NumberPreferredBrun':     int((events.preferred_origin_source == 'brun').sum()),
    'NumberPreferredGold':     int((events.preferred_origin_source == 'gold').sum()),
    'NumberPreferredPepi':     int((events.preferred_origin_source == 'pepi').sum()),
    'NumberPreferredHypo':     int(events.preferred_origin_source.isin(['hypo40','pinaall']).sum()),
}

# Write as LaTeX \newcommand macros
with open('manuscript/pinatubo_counts.tex', 'w') as f:
    for name, val in macros.items():
        f.write(f'\\newcommand{{\\{name}}}{{{val}}}\n')

print("Generated pinatubo_counts.tex")
```

Then in the manuscript preamble: `\input{pinatubo_counts}` and in the text:
`\NumberCatalogEvents{} catalog events` instead of the hardcoded number.
This eliminates the class of error where manuscript and pipeline drift apart.

---

## Recommended order of work

1. **Apply P0 bug fixes** (Fixes 1–5) and add regression tests → ~1 day
2. **Freeze pipeline at a tagged commit;** regenerate counts; update manuscript macros → 0.5 days
3. **Apply P1 fixes** (Fixes 6–9) → 0.5 days
4. **Run grouping sensitivity** and investigate 36 differing BRUN duplicates → 1–2 days
5. **Build association validation** (tolerance sweep + negative control) → 1–2 days
6. **Travel-time residuals** (new `47b`) → 2–3 days
7. **Strengthen step 72** (fix anchor identity, residual-vs-time plot) → 1 day
8. **Post-failure channel QC** → 1–2 days
9. **Waveform-only sampling** → 1 day
10. **Regenerate all exports** from frozen, corrected baseline; update manuscript → 1 day

The LaTeX macro approach (item 2) pays for itself immediately by ensuring that
items 4–9 feed directly into the manuscript without manual transcription.

---

## What the audit resolves from the review cycle

| Reviewer concern | Resolution |
|---|---|
| Regional origins displacing local HYPO71 | Not observed in current output — confirmed negative |
| 593 vs 1,413 discrepancy | Two different suppression stages — confirmed correct |
| "107" regional origins denominator | 107 preferred origins; 117 origin rows — clarify in manuscript |
| Transitive chaining | Not present — greedy earliest-anchor, not connected components |
| b-run duplicates consequential? | 36 of 193 differ (max 12.65 km depth spread) — needs review |
| Step 47 pair counting | Real bug — pairs counted as events — fix required |
| Clock uncertainty in waveform index | Missing — fix required |
| Step 72 anchor identity | Real bug — fix before trusting residual values |
| Jones et al. 100% match | Confirmed in clock_regimes.csv comment: "PEPI 153 of 153" |
