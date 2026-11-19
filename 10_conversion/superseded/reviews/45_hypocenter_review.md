# Step 45 — Hypocenter stages 40–44: review and decisions

Review snapshot: 11 November 2026. This Markdown companion is not executable and does not refresh its statistics automatically.

Evidence: saved notebook outputs and configured 40/41/43 CSVs. The numerical results below describe the saved run reviewed before the magnitude-policy update. No production tables were regenerated during that review. Supporting pair-level audits and figures are retained in the project review workspace; their locations are listed below.

## Ingestion and coverage

- Pinatubo_all.sum: 4,632 origins, May 7–August 19, 82 observed dates, zero logged unparsed lines.
- Original PINAALL.DAT: 3,601 origins, May 10–August 19, 76 observed dates, zero logged unparsed lines.
- The original PINAALL branch was used. Annotated-clock classifications/corrections were not applied by this run.
- No missing core numeric fields in either index. Parsing success does not independently establish numeric meaning or UTC convention.
- Both indexes contain June 11 origins (10 each); both then lack June 12–28 origins and resume June 29. This is distinct from the waveform and phase-pick coverage.

## Exact comparison versus candidate association

Step 42 removes four duplicate five-field summary records and three PINAALL duplicates. It finds 244 exact shared records including magnitude among 4,628/3,598 unique records. Excluding magnitude gives 757 exact time/location/depth matches; excluding depth as well gives 762 time/location matches. Rounding numeric fields to 10, 6 or 4 decimals does not increase that match count. Thus floating-point precision alone does not explain the discrepancy.

Step 43 uses 5-second / 15-horizontal-km greedy grouping and produces 4,690 candidate groups containing all 8,233 source origins exactly once: 3,500 groups with both sources, 1,105 summary-only, 85 PINAALL-only. Conservation and source reconciliation are useful; these groups are not yet a validated unique-earthquake catalogue.

There are 3,480 clean one-origin-per-source groups. Their monthly median PINAALL-minus-summary time differences and horizontal separations are:

| Month | Pairs | Median time difference (s) | Median separation (km) |
|---|---:|---:|---:|
| May | 262 | +0.01 | 0.111 |
| June | 805 | 0.00 | 0.000 |
| July | 1,537 | +0.53 | 0.780 |
| August | 876 | +0.48 | 0.928 |

Different times and coordinates plausibly indicate different location solutions/catalogue revisions, but provenance is unresolved. Do not shift the clocks to remove these differences. These statistics are conditioned on the current association thresholds and say nothing about a possible offset among unmatched origins.

## Greedy over-grouping risk: concrete evidence

27 groups contain more than one origin from at least one source. Total membership distribution: 1,183 singletons, 3,487 pairs, four triples, 16 quadruples.

Group 907 combines origin times June 7 06:22:16.21 and 06:22:19.92. Each time/location solution appears separately in BOTH source catalogues. Both times fall within the five-second window, so the algorithm groups four origins together. This is strong evidence of a potential conflation of separately catalogued events, rather than simply two alternative solutions. Confirm with picks/waveforms; these are not assigned to waveform files at this stage.

Group 1598 combines two summary-only origins 0.11 seconds apart with different locations/depths; group 2854 combines two summary origins and one PINAALL origin. Such cases may represent duplicate/revised solutions or separate events. A blanket deletion/split would be premature.

Recommended refinement: identify within-source duplicate/revision relationships explicitly, then match sources one-to-one using time and location, preserving competing candidates instead of automatically collapsing all nearby records. Do not alter the algorithm solely to improve match percentage. For now retain all original origins and flag the 27 groups for review.

## Depth and magnitude caveats

Summary depths range −3.00 to 31.22 km (533 negative values); PINAALL ranges −1.80 to 32.38 km (142 negative values). Negative depths can depend on the reference datum or location constraints; do not reject them before confirming conventions.

Among clean one-to-one source pairs, 79 have depth differences exceeding 5 km. Depth is not a matching criterion. Inspect these and the most separated horizontal matches (up to 13.62 km) rather than treating membership as proof of equivalence.

Magnitude-column ranges are −0.57 to 1.88 and −0.57 to 1.87. June summary values are zero for 753/851 records, compared with 229/820 PINAALL records. The median magnitude-column difference in the clean June pairs is +0.44 (PINAALL minus summary). Zero may be a missing/default value in some records, but this is not established here. Verify the source magnitude convention and missing-value encoding before plotting magnitude-frequency relations, energies, completeness thresholds or selecting a preferred magnitude. The reviewed run selected magnitude together with the preferred summary solution. The updated step-43 policy now selects magnitude independently; rerun steps 43 and 50 to apply it.

## Next actions

1. Preserve the current run as a provenance baseline; zero parse errors is encouraging.
2. Flag/review the 27 same-source-multiple groups, prioritizing group 907. Avoid propagating 4,690 as a definitive unique-event count.
3. Establish source revision history, depth datum and magnitude/zero conventions with original documentation/colleagues.
4. Inspect preferred-source choice and individual discrepant solutions rather than applying a general timing correction.
5. Continue waveform/pick/hypocenter linkage with source origins preserved. Event multiplicity and uncertainty need to remain visible in waveform review.


## Exact-match clarification

244 unique five-field records match including magnitude. Excluding magnitude gives 757 exact time/latitude/longitude/depth matches; excluding depth as well gives 762 exact time/latitude/longitude matches. “244 exact hypocenters” was too imprecise: magnitude disagreement accounts for many nonmatches.

Figures use the 3,480 groups containing exactly one origin from each source, avoiding arbitrary pairing of the multi-origin cases. Signed differences are PINAALL minus summary. Daily medians do not interpolate across absent days. These are threshold-selected matches, not a test for arbitrary large offsets among unmatched records.

Artifacts: `origin_differences_through_time.png`/PDF, `spatial_and_timing_differences.png`, `multi_origin_groups_timeline.png`, `clean_counterpart_differences.csv`, and `multi_origin_groups_dated.csv`.

## When and what the 27 groups mean

1 group occurs June 7, 16 in July, 10 in August. None occurs June 8–11; neither source contains June 12–28 origins, so clustering immediately before June 15 cannot be assessed. Four of 27 groups are explained by exact within-source duplicates. The remaining 23 contain differing records within at least one source; they are not automatically separate earthquakes or confirmed repetitive-event sequences.

- Group 907: June 7 06:22:16.21 and 06:22:19.92, each represented with identical matching solution in both sources. Likely distinct catalogue entries merged by the five-second rule; confirm signals/picks.
- Group 1904: July 9 summary times 09:26:02.29 and 09:26:04.22; corresponding PINAALL times 09:26:03.05 and 09:26:04.40. Depths are 24.84 km for the earlier pair and 12.96/15.63 km for the later pair. Could be two close earthquakes or a processing/solution duplication. A swarm/repetitive-source interpretation requires waveforms.
- Group 1547: July 3 has two summary solutions at the exact same time 08:45:35.87 but depths 2.84/4.49 km and magnitudes 0.58/0.48; PINAALL has nearby solutions at 08:45:36.30 and .38, with corresponding magnitudes but different depths. This may indicate processing variants/near-coincident entries, not automatically two earthquakes.
- Group 2045: July 11 two summary solutions at the identical time 02:45:00.59, with slightly different coordinates/depths. Cannot interpret the multiplicity as repetitive seismicity from timestamps alone.

The event IDs here are algorithm-created origin clusters. They are not independently established physical earthquakes or waveform IDs.

## What is time-corrected?

Existing differences do not establish which catalogue is later or corrected. Relocation, changed arrival picks, velocity model, station delays or revisions can change both origin time and hypocenter. A uniform absolute shift in all picks can change origin time without changing location; the observed coordinate changes mean a pure clock shift is not a complete explanation, although timing corrections could be part of reprocessing.

The current FLOVOpy conversion applies no explicit absolute start-time clock correction: MiniSEED uses the recorded DMX timestamps. It changes sample-rate metadata to the configured 100 Hz (affecting sample-time spacing and end time), IDs and the 2048-count offset; this is not absolute-time calibration. A timestamp already corrected before these surviving DMX files were created would be retained. Thus describe the waveform timing as “as recorded in surviving SUDS files; previous correction history unknown,” rather than declaring it either raw clock time or corrected UTC. Reused MiniSEED files also retain their pre-existing timing; checking a representative raw-to-converted header sample remains useful.

Discrimination requires matching original PHA/HYP records and their pick residuals/velocity models to each solution, checking whether station arrival offsets are common or station-specific, and examining original timing-channel/calibration documentation where available. Source file age or more complete magnitudes alone cannot establish the more accurate clock.

## Magnitude evidence and provisional preference

The discrepancy exists in the preserved raw lines, not just the parsed CSV:

Summary line 68: `91 520 1318 32.55 15n 9.29 120E19.34   2.77   0.00  9          0.07`
PINAALL line 9: `91 520 1318 32.55 15n 9.29 120e19.34   2.77   0.52`

The same origin/location/depth has zero in summary and 0.52 in PINAALL. The summary parser directly reads the numeric magnitude field rather than inventing a default zero. PINAALL has therefore been adopted as the preferred magnitude source, with the policy below. Preserve both source values and flag suspected missing/default zeros. Treating all reported zeros as missing is the adopted provisional policy and remains reversible; do not infer that PINAALL's origin times/locations are superior from its magnitude completeness. Scientific preferred origin and preferred magnitude can have different source provenance; the updated step-43 and step-50 implementations now preserve that distinction.

## Adopted magnitude policy and rerun status

Glenn requested PINAALL-first magnitudes and treatment of reported 0.0 values as likely nulls. The implemented policy is:

1. Choose a nonmissing PINAALL magnitude within each associated origin group.
2. If none is available, choose a nonmissing summary-file magnitude.
3. If neither source has a usable value, leave the preferred magnitude missing.
4. Provisionally treat reported 0.0 (including negative zero) as missing. Retain negative nonzero magnitudes. This is an explicit archival interpretation, not a claim that true magnitude-zero earthquakes cannot occur.
5. Preserve the original source indexes unchanged. Step 43 writes normalized origin magnitudes alongside `reported_magnitude` and `magnitude_missing_reason`. Preferred magnitude provenance is recorded as `preferred_magnitude_source` and `preferred_magnitude_origin_id`. Missing CSV numeric values are empty fields/pandas NaN, not strings such as `NULL` or `None`.
6. Select preferred hypocenter time/location/depth independently using the existing origin preference. More complete PINAALL magnitudes do not establish more accurate PINAALL times or locations.
7. Step 50 exports the selected magnitude to ObsPy, links it to its source origin and records source provenance. Missing magnitudes are omitted rather than exported as zero. No unverified magnitude type is assigned.

The step-43 settings are `preferred_magnitude_source='pinaall'` and `zero_magnitude_is_missing=True`. Within a group containing multiple valid PINAALL magnitudes, the first eligible value wins; this does not resolve the 27 potentially conflated groups. Review those candidates before scientific interpretation.

**Implemented, not yet reflected in the saved-run counts:** rerun 43, then 50 (and later exports as needed) to regenerate outputs with this policy. Rebuilding steps 40/41 is not required solely for magnitude normalization because source indexes intentionally retain reported zeros. The 753/851 June summary zeros above remain a valid description of the raw source field; they must not be interpreted as the number of magnitude-zero earthquakes.

Controlled checks passed for PINAALL preference, summary fallback, both-sources-missing, preservation of reported zeros, negative magnitudes and catalogue export. Full production output regeneration was left to the user. Refresh this review after that run.

## Supporting review artifacts

The figures and CSVs are retained under:

`/Users/thompsong/.codex/.chatgpt-projects/g-p-67c7944842ac81919d734f0b72c39c9a/review/hypocenter-stage-2026-11-11/`

- `origin_differences_through_time.png` and `.pdf`: matched-origin time, horizontal, depth and magnitude differences.
- `spatial_and_timing_differences.png`: horizontal displacement and its relationship to origin-time differences.
- `multi_origin_groups_timeline.png` and `multi_origin_groups_dated.csv`: dates and spans of the 27 same-source-multiple groups.
- `clean_counterpart_differences.csv`: the 3,480 one-origin-per-source comparisons.
- `cross_source_origin_pairs.csv`: all within-group cross-source pair combinations, including ambiguous groups; not a one-to-one matched catalogue.
- `origin_source_multiplicity.csv`: source membership counts per algorithm-created group.

These are review artifacts, not required pipeline inputs. Their absence on another computer does not prevent pipeline execution. For reproducible publication figures, incorporate equivalent plotting into an analysis notebook and regenerate from the selected output version.
