# Step 24 — Preliminary review of phase stages 20–23

Review snapshot: 11 November 2026. Revisit after waveform association; counts describe this run, not automatically refreshed results.

Evidence: current saved step-20/21/22 indexes, suppression CSV, step-20/21 logs, step-23 diagnostic CSVs/QC JSON and cross-source reports under `/Users/thompsong/work/Pinatubo1991Processing/converted/pipeline_tmp/`. Notebook outputs were inspected but the index files are the basis for the counts below. No production files or parser behavior were changed in this review.

## Overall assessment

The stages produce a useful provenance-preserving candidate pick archive. Proceed to waveform association so that arrival picks can be reviewed against the actual signals. This preliminary review records structural issues and hypotheses; it is not a requirement to resolve every pick before association. Existing QC flags alone cannot establish scientific validity or completeness.

| Product | Pick rows | Source groups | Recorded date coverage |
|---|---:|---:|---|
| Individual | 1,330 | 119 | June 3, 10, 11 and 12, 1991 |
| Monthly | 57,042 | 8,201 | May 1–August 21, 1991; 95 observed pick dates |
| Merged | 57,780 | 8,317 | May 1–August 21, 1991; 97 observed pick dates |

Dates are stored timestamp labels, not independently verified UTC. The 116,152 picks / 16,637 groups printed by step 23 sum overlapping input tables and must not be used as archive totals.

## Individual files and date provenance

141 distinct paths have 119 distinct basenames. The 22 repeated-basename pairs have identical parsed records (including raw line text, line number, identity, phase, time and weight). Step 22 suppresses 211 duplicate primary rows. These are archival duplicate copies, not extra arrivals.

The date audit identifies 280 mismatching pick rows: 211 folder-vs-filename/pick mismatches, 64 folder-and-filename-vs-pick mismatches, and five folder-vs-pick mismatches. Raw records confirm examples such as `9106101R.PHA` contain timestamps beginning `910611`; this is not evidence the parser invented the next day. Preserve folder, filename and raw pick dates rather than shifting them to agree. TRIAL2 also carries June 12 picks. No issues were logged by the completed individual parser; this does not assess scientific validity or silently filtered picks.

## Monthly parser exceptions

The error log contains 169 entries: May 4, June 36, July 68, August 61. Examples include minute fields of 60, seconds >=60, XXXX station placeholders, and embedded NUL characters. Ten entries contain XXXX and seven contain NULs; categories are not exclusive. These are not all necessarily unrecoverable: overflow-style time fields may admit justified normalization, while placeholders/corrupt records need separate handling. Inspect examples before changing the shared parser; preserve raw records and an explicit repair/rejection audit. The current logs are entries, not a measured count of lost P/S arrivals.

Monthly June picks cover June 1–10 and June 27–30. Individual files supply June 11/12 arrivals despite the monthly gap, so describe coverage separately by source.

## Merge accounting and interpretation

1,330 primary inputs −211 within-group duplicates =1,119 primary outputs.
57,042 secondary inputs −134 within-group duplicates −247 primary-matching picks =56,661 secondary outputs.
Together: 57,780 retained picks; suppression report: 592 rows. No invalid-time rows were counted in this run.

Cross-source suppression uses full normalized SEED ID, phase and inclusive 0.5-second tolerance; it does not identify a shared physical event or reconcile group IDs. Most merged picks are secondary. Retaining unmatched parts of a monthly group after suppressing its primary-matching arrivals can fragment group membership and change group-level P/S pair counts. Subsequent association must resolve identities; 8,317 is not a distinct-earthquake count.

## P/S diagnostics

This run uses a 15-second maximum delay, not the generic function default of 60 seconds.

| Source | P/S pairs | Median S−P (s), all pairs | Outside 0–15 s |
|---|---:|---:|---:|
| Individual | 51 | 0.98 | 1 |
| Monthly | 12,628 | 2.23 | 3 |
| Merged | 12,578 | 2.24 | 4 |

Individual station support is sparse: BUG 3, BUR 3, GRN 10, PIE 3, PPO 27, UBO 5 pairs. BUR's median is 0 seconds. Do not treat the individual boxplots as robust station-population estimates, or their difference from monthly results as proof of a clock problem.

The strongest identified anomaly is GRN in `91060310`: raw `GRNZ P 4 910603171748.23       48.22 S 3`. P is 17:17:48.23; the reader rolls S=48.22 into the next minute, generating S−P=59.99 seconds. This is a parser rollover interpretation triggered by a 0.01-second reversal in the source, not evidence of a real 60-second delay. Preserve and flag it; avoid automatic correction without a documented rule.

Other screened delays: CAB 17.65 s (`pinaug91_block0920`), FNG 15.19 s (`pinaug91_block0984`), FNG 17.31 s (`pinjul91_block1080`). Exceeding 15 s alone does not prove these are erroneous. Source pairing is by station, not full channel, and follows existing source-group IDs.

All outlier-fraction flags are false, including individual 1/51=1.96% immediately below the 2% threshold. The existing zero-event-day flag cannot detect absent days because its input only contains observed days. False flags do not demonstrate completeness or correctness. Weight codes (including 4/5) are retained; counts are not restricted to picks accepted for location.

## Cross-source comparison

The existing comparison is restricted to June 3 and June 10, not the full archive. June 3 individual matches: 259/736 (35.2%) at 0.05 s and 351/736 (47.7%) at 1 s. June 10: zero matches for 525 individual picks against only 15 monthly picks, at either tolerance. Sparse/nonoverlapping coverage may explain this; it does not establish a global offset.

For mutually unique matched pairs: 125 pairs at 0.05 s, 169 at 1 s; median signed monthly-minus-individual offset is 0 s for both. This is consistent with common timing for matched records, but selection by tolerance cannot rule out offsets among unmatched records. Duplicate individual copies account for some apparent ambiguity. Compare after identifying duplicate copies, retain full ID/phase identity, and examine a few overlapping source groups/raw lines before widening tolerances or shifting clocks. Nearest unmatched counterparts minutes away are diagnostic candidates, not event associations.

## PI2L evidence changes the acquisition hypothesis

Monthly parsed records contain 1,832 ELZ picks; 1,826 survive in the merged index across 1,178 monthly source groups, June 27 01:03:27.31–August 21 15:51:47.30 (record labels). Raw provenance explicitly contains PI2L station labels. This is evidence that low-gain arrivals were picked; it is not a count of low-gain waveform files.

The previous no-PI2L raw-DMX daily sample and Glenn's no-PI2L SSAM inspection remain valid observations. The stronger routing hypothesis that PI2L was absent from event acquisition is now uncertain. Possibilities include another picking medium, lost/unsampled low-gain waveforms, alternate acquisition/conversion histories, or upstream labeling differences. Ask colleagues what medium supplied the PI2L picks and search matching event times in raw DMX/provenance before concluding it was RSAM-only. The step-15 routing note should be revised in a later documentation update.

## Agreed next workflow: associate, then review against waveforms

Scientific pick review requires the waveforms. Continue the association stages with the current candidate indexes, preserving provenance and unresolved cases. The issues above guide that review rather than blocking association.

After association, generate a review plot for **every waveform event file with associated picks**. This is a proposed next implementation, not an existing output of this Markdown file. Each plot should:

- Show the recorded station/channel traces and associated P/S markers using full SEED IDs; label the original source and phase. Display missing-channel or outside-window picks explicitly rather than silently omitting them.
- Distinguish individual-file and monthly-file picks, retained/suppressed status where available, and competing associations. Preserve the original source group IDs and raw-line references in accompanying tables.
- Identify partial DMX recoveries, clipping/flat traces and uncertain clock conventions. A missing arrival in a short triggered window does not prove the pick is wrong.
- Use time relative to a stated waveform reference, while retaining recorded timestamps. Do not silently shift picks or waveforms to improve apparent agreement.
- Save figures and a machine-readable review index beneath the configured external output root. Record waveform path, associated pick IDs, association confidence/reasons and eventual review outcome.

Prioritize representative associations first to check the plotting and matching logic, then generate the full plot collection. Include explicit reports for picks without waveforms and waveforms without picks; successful matches alone do not describe archive completeness.

### Questions to revisit with the plots

1. Are the monthly time-overflow entries safely normalizable, or do they reflect corruption? Keep raw records and explicit repair/rejection decisions.
2. Do the GRN 59.99-second rollover and zero-delay examples fit visible arrivals? Avoid correcting timestamps solely because a delay looks implausible.
3. Do source mismatches, especially June 10, reflect different recordings, missing coverage, source grouping, phase labels or timing conventions?
4. Do PI2L picks correspond to a surviving low-gain trace, another channel, an absent recording medium or unmatched data? Qualify the step-15 routing hypothesis accordingly.
5. Are filename/folder date discrepancies harmless archival organization or relevant to association? Do not overwrite documentary dates.

### Scope of this review file

This is a human-readable, version-controlled notebook companion, not an executable pipeline stage. It does not change picks, require a kernel, rerun diagnostics or approve associations. Refresh its counts and conclusions after the next association/review run. The detailed step-23 diagnostics and their explanatory notebook remain the computational source for the preliminary results.
