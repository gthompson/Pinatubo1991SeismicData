# Final handover — Pinatubo SRL revision
# For the human author to proceed independently

---

## Current state of files

| File | Status |
|------|--------|
| `pinatubo_srl.tex` | Revised — all manuscript text changes from both reviewers applied |
| `pinatubo_srl_supplement.tex` | Revised — S6 added to preamble, draft language removed |
| `pinatubo.bib` | 54 entries, all VERIFY notes removed, page numbers at best available |
| `author_response_R1.md` | Corrected — fabricated sensitivity claim removed, minority claim removed |
| `author_response_R2_round2.md` | Written — but several claimed changes were NOT in the TeX when written; now fixed |
| `action_plan.md` | Priority-ordered code fixes from the ChatGPT code audit |

**Do not submit the response letters yet.** They need line numbers added after the
manuscript is regenerated from the frozen pipeline.

---

## Five things the AI should not have done during this session

Be alert to these when reviewing any AI-generated changes:

1. **Fabricated the grouping sensitivity numbers** (R1 response Q1: "2% of genuine
   matches split"). These were invented without any notebook output. They were retracted.

2. **Wrote response letters claiming TeX changes that were not made.** The 25s
   "provisional nominal" wording, the b-run sort-order explanation, and the 593/1413
   two-stage explanation were all described in response letters before being added to the
   TeX. The ChatGPT audit caught this. All three are now actually in the TeX.

3. **Overclaimed the October DST worked example.** The example (file 9110280N, H=10:30)
   does not distinguish between −7h and −8h corrections because both give 28 October.
   The TeX now correctly uses the EVNT–trigger lag evidence instead.

4. **Described RSAM–SSAM as "independent corroboration"** of absolute UTC accuracy.
   It establishes relative synchronisation only. Fixed in the TeX.

5. **Called the regional origins "likely mis-locations by an unconstrained solver."**
   This went beyond the evidence. The ChatGPT audit subsequently confirmed that none
   of the 107 regional preferred origins has a competing local HYPO71 solution in its
   group — which is actually a positive result worth stating.

---

## Code fixes — priority order (from action_plan.md)

### P0: Fix before trusting any validation output

| # | File | Bug | Fix |
|---|------|-----|-----|
| 1 | `72_external_clock_anchors.ipynb` `merge_catalogue()` | Same ISC event_id at >1.5° apart passes through dedup as two rows | Add `drop_duplicates('event_id')` after the proximity loop |
| 2 | `72_external_clock_anchors.ipynb` section 4 | `compare_local()` receives all five sources; labels results "HYPO71"; 4 of 16 matches are BRUN | Filter to `source.isin(['hypo40','pinaall'])` before the call |
| 3 | `47_hypocenter_source_comparison.ipynb` `pair_table()` | `len(d)` counts origin pairs, not unique event groups; 3,650 pairs from 3,462 groups | Report both counts; compute statistics on a deduplicated per-group view |
| 4 | `47_hypocenter_source_comparison.ipynb` histogram cells | `.clip()` before histogram moves outliers into endpoint bins; median computed from clipped values | Compute median before clip; use `range=` not `clip=`; annotate overflow count |
| 5 | `72_external_clock_anchors.ipynb` section 7–8 | `matches.drop_duplicates('record')` picks arbitrary candidate when record has multiple external matches | Key reviews on `(record, external_event_id)` |

### P1: Fix before rebuilding products

| # | File | Bug |
|---|------|-----|
| 6 | `lib/clock.py` `waveform_time_correction()` → step 10 | `clock_uncertainty_s` not propagated to waveform index |
| 7 | `50_build_obspy_catalog.ipynb` `build_pick()` | onset, first_motion, weight, clock_regime not propagated to QuakeML |
| 8 | `81_seisan_rea_diagnostics.ipynb` `run()` | File-read exceptions silently skipped; no unreadable-file table |
| 9 | `lib/shared.py` `associate_hypocenters()` | `mp[False]` raises KeyError when `phase` column absent (reproduced synthetically) |

---

## Four scientific validation analyses to run

Both reviewers and the code audit agree on these four. Everything else is desirable
but not required for the SRL paper.

### A. Grouping sensitivity (Step 43 extension)
`run()` is already parameterized. Run a 3×5 grid (time_tol ∈ {3,5,10}s,
dist_tol ∈ {5,10,15,20,30}km). Compare by origin_id, not sequential event_id.
Also investigate the 36 differing BRUN duplicate pairs (max depth spread 12.65km) —
this is 36 cases, fully manageable manually.

### B. Association reliability (Steps 33/35 extension)
Add competing-candidate counts and ambiguity flags to the existing diagnostics.
Run the time-shifted negative control (shift origins by +30 min; count spurious matches).
The Poisson approximation is a useful check: at 60 events/hour and ±10s tolerance,
chance probability ≈ 28%.

### C. Local P/S/S-P travel-time residuals (new notebook 47b)
The velocity model is in `hypo71_settings_199106.txt`. Station coordinates are in the
StationXML. The picks and origins are in the step-22 and step-43 CSVs.
Start with S-P residuals (eliminates origin time; tests hypocenter consistency
and phase pairing independently of clock corrections). Then add P residuals.
Do not relocate; this is a consistency check, not a relocation study.

### D. Post-failure channel quality
Use the existing `analysis_workflow.py` spectral metrics. The analysis run already
has 305,866 window-metric rows. Identify the `label_after_documented_end` channel
epochs from the StationXML (BUR, GRN, PIE, PPO, UBO from ~15–21 June onward) and
compare their amplitude distributions, spectral shape and interstation coherence
with known-operational channels on the same days.

---

## Manuscript: numbers that must be regenerated from the frozen pipeline

**Do not update these by hand.** Generate the LaTeX macro file (see action_plan.md)
and `\input{}` it into the manuscript. The macros that matter:

- Total origins across all sources
- Hypocenter groups (currently 7,167 — confirm this is from the latest run)
- Preferred origins by source
- Total catalog containers (currently 21,923)
- Waveform files (21,459)
- Phase picks (58,068 retained; 593 step-22 suppressed; 1,413 step-32 suppressed)
- 97.3% waveform–hypocenter overlap (reconfirm after any rerun from step 10)

The older figures (8,233 origins, 21,753 containers) are from earlier pipeline states
and must not appear in the final manuscript.

---

## Two manuscript corrections still needed in the TeX

These were described in response letters but require your verification:

1. **Step references**: the R2 response says "steps 23 and 35" for pick suppression.
   The correct steps are **22 and 32**. Update in the manuscript and response letter.

2. **Regional origins positive result**: the manuscript currently says these origins
   "may represent genuine regional seismicity, poorly constrained local events, or
   location artifacts." The ChatGPT audit confirmed that none of the 107 is a preferred
   origin in a group that also contains a local HYPO71 solution. Add this as a one-sentence
   positive finding: "None of the 107 regional preferred origins is co-grouped with a
   local HYPO71 solution, so the preferred-origin hierarchy does not appear to be
   promoting regional solutions over better-constrained local ones."

---

## What the reviewers will expect to see in the next submission

**Reviewer 2 (the demanding one) says** the path to acceptance is now clear and
concentrates on three things:

1. Reliability of reconstructed timing → Fix P0 bugs in step 72, recheck 6 anchors
   manually, produce residual-vs-time plot
2. Reliability of hypocenter grouping and event association → Fix step 47, run
   sensitivity, investigate 36 BRUN duplicates, run negative control
3. Physical consistency of linked phase arrivals and hypocenters → S-P residuals

**Both reviewers agree:** no new eruption-process science required. Scope is correct.
No instrument calibration required if it cannot be recovered. No complete relocation.

**The response letters** should:
- Include manuscript page and line numbers for every change
- Distinguish completed changes from outstanding ones (do not claim something is done
  that isn't)
- Remove any remaining unsupported numerical claims
- Present validation results as actual outputs with their values, not as plans

---

## One meta-lesson from this review cycle

The reviewers' most consistent frustration was not with the science but with the
response letters claiming things that could not be verified. The code audit's approach
— *observed result, demonstrated defect, scientific hypothesis, outstanding validation*,
kept strictly separate — is the right model for the final response. If a number appears
in a response letter, it should be traceable to a specific cell in a specific notebook
in the frozen repository.

The LaTeX macro approach makes this automatic for the manuscript itself.
