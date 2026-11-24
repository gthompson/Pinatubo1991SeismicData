# Response to Reviewer 2 — Second Round

We thank the reviewer for the careful assessment of our response letters, and in particular for identifying statements that were either insufficiently supported or logically inconsistent. We address each of the reviewer's new concerns below, distinguish what has been corrected in this round from what remains outstanding, and are explicit about the boundary between completed and planned work.

We acknowledge the reviewer's meta-comment directly: some statements in our initial response letters went beyond what had actually been established. We have corrected those statements in both the manuscript and the response documents, and we have adopted the reviewer's recommendation to treat this as a revision plan rather than a completed revision.

---

## Corrections to previously submitted responses

Before addressing the reviewer's scientific comments, we note the following corrections to statements in our initial response letters that the reviewer correctly identified as unsupported.

**Hypocenter grouping sensitivity (R1 Q1):** Our initial R1 response stated that sensitivity tests showed that tightening to 3 s and 10 km left about 2% of genuine matches split across groups. This was not based on actual notebook output and has been retracted. The R1 response now correctly states that a formal sensitivity experiment has not yet been performed and is deferred to the full computational revision. No corresponding numerical claim was added to the manuscript.

**Waveform-only artifact proportion (R1 Q2):** Our initial R1 response described trigger artifacts as a "minority" of waveform-only records. This was an unsupported characterization. The revised R1 response states that the relative proportions have not been established and that a stratified inspection is planned.

**Antelope contact (R1 Q3):** Our initial R1 response stated "We have contacted T. Parker." Contact has not yet been initiated. The revised response states that this is planned.

**White (1996) relationship to frequency index (R1 comment 11):** Our initial R1 response described the deep LF earthquakes as "closely related to the low-frequency energy captured in Figure 12." The reviewer correctly notes this relationship has not been demonstrated. The revised response clarifies that the citation notes temporal coincidence, not causation.

---

## 3.1. Hypocenter grouping sensitivity — inconsistency between responses

**Accepted.** The inconsistency is genuine and arose from an error in the R1 response, as described above. The R1 response has been corrected. The formal sensitivity analysis — covering the quantities the reviewer lists (number of groups, group membership changes, multi-source groups, transitive chaining) — remains outstanding and is the highest-priority computational item.

---

## 3.2. The 25 s and 60 s timing uncertainties

**Accepted.** The reviewer's proposed wording is more accurate than ours. The revised manuscript now reads:

> "Based on the available anchor observations, we assign provisional nominal timing uncertainties of 25 s after the May 1991 reset and 60 s before it. These values encompass the observed anchor residuals but are not statistically derived confidence intervals or demonstrated maximum errors; larger errors may occur during intervals without independent timing constraints."

The word "conservative" has been removed. The distinction between encompassing available observations and bounding unobserved periods is now explicit.

---

## 3.3. Independence of the RSAM–SSAM timing validation

**Accepted.** The original sentence conflated relative synchronization with absolute UTC validation. The revised manuscript distinguishes three classes of evidence:

> "the teleseismic and regional arrivals in Table 3 provide external constraints on absolute UTC accuracy; the RSAM–SSAM lag analysis establishes relative synchronization between monitoring products but does not independently constrain absolute UTC timing."

The RSAM–SSAM comparison is no longer described as providing independent corroboration of the clock rules — it establishes that the monitoring products are mutually consistent after the corrections, which is a different and weaker claim.

---

## 3.4. The October DST worked example

**Accepted in part.** The reviewer is correct that both −7 h and −8 h corrections produce the same filename date for the example file (H = 10:30 on 28 October). The October example has been replaced:

> "The October transition is established by the EVNT–waveform lag analysis: the lag of maximum cross-correlation between EVNT counts and the trigger rate shifts from −7 to −8 hr on the same date as the filename pattern changes (both consistent with 27 October, the end of U.S. daylight saving time), providing independent confirmation of the new offset."

Regarding the mechanism: we cannot directly inspect the MDETECT executable or source code. The behavior is consistent with the application of `TZ=PST8PDT` in the DOS C runtime, which is consistent with Power's written recollection and the Murray (1996) account. The revised manuscript describes it as "consistent with application of US Pacific time-zone conversion rules" rather than asserting a particular implementation mechanism.

The May example (file `9105261G`) remains in the manuscript; it is valid because H − 15 hr and H − 7 hr produce different calendar dates, and the associated PINAALL.DAT line provides an independent match.

---

## 3.5. Interpretation of the 107 regional origins

**Accepted.** The statement "likely mis-locations of local earthquakes by an unconstrained solver with no regional phases" has been replaced with more cautious wording:

> "The origin of these regional solutions is uncertain: they may represent genuine regional seismicity below the detection threshold of global networks, poorly constrained local events, or location artifacts; a diagnostic analysis of their phase observations and location quality is planned for the full revision. Users investigating local volcanic seismicity should inspect these solutions separately and apply a spatial selection."

The specific question the reviewer raises — whether any of the 107 regional solutions are preferred origins for groups that also contain plausible local HYPO71 solutions — will be directly addressed in the diagnostic analysis.

---

## 3.6. Preferred-origin selection within duplicate b-run.sum solutions

**Accepted.** The first-record rule after a stable sort is a computational convenience. The revised manuscript makes this explicit: "where b-run.sum has two copies of the same event, both are retained in the group but only one is selected as preferred by sort order; if the two solutions differ materially in quality or location, the selection may not represent the scientifically preferable solution."

The planned diagnostic table (differences in depth, epicenter, origin time and magnitude between duplicate pairs) will establish whether the deterministic rule is consequential for the resulting catalog.

---

## 3.7. Interchannel sampling offset — inappropriate comparison with absolute clock uncertainty

**Accepted.** The original sentence "this is well within the 25 s clock uncertainty and is not corrected" compared incommensurable quantities. The revised manuscript reads:

> "MDETECT samples channels sequentially, introducing a nominal interchannel offset of 1/15 × 1/100.8 s ≈ 0.66 ms between successive channels (maximum first-to-last separation ≈ 9.3 ms). This offset is not corrected: the original channel acquisition order cannot be reliably reconstructed from the DMX headers. It is small relative to typical P-pick uncertainties but potentially significant for polarization analysis and high-precision cross-correlation work."

The absolute clock uncertainty is no longer invoked. The limitation is documented on its own terms.

---

## 3.8. Duplicate-pick count discrepancy (593 vs 1,413)

The two counts refer to different pipeline operations and are both correct:

- **593** (Phase picks subsection, step 20–23): cross-source duplicates suppressed during the merge of single-event and monthly phase files. This is the count that answers the R1 reviewer's original question about the phase-merge step.
- **1,413** (Waveform–pick events subsection, step 30–35): within-event duplicates suppressed during the association of picks with waveform containers. These are picks that appear in more than one source for the same event and are suppressed so that each pick appears exactly once in the linked catalog.

These are genuinely distinct operations at different stages of the pipeline. The manuscript text has been updated to make this explicit: "An earlier step (step 23) suppresses 593 cross-source duplicates during phase-file merging; a further 1,413 within-event duplicates are suppressed here during waveform–pick association."

The deduplication rule at step 23 (same station, phase and label within 0.5 s) is stated in the Phase picks subsection. The source priority at that stage is single-event files over monthly files. The 1,413 within-event suppression at step 35 uses a different rule (same pick appearing in multiple sources for the same event container) and the documentation of that step will be clarified.

---

## 3.9. Interpretation of the June frequency-index decline

**Accepted.** The White (1996) citation notes temporal coincidence. The manuscript does not attribute the frequency-index decline specifically to deep LF earthquakes; it lists them alongside hybrid events, SSAM changes and other documented phenomena as contextual observations from the same interval. This framing has been confirmed in the revised TeX. No causal attribution is made.

---

## 3.10. Waveform-only population

The word "minority" has been removed from the R1 response as described above. No proportional claim appears in the manuscript. The stratified sampling exercise remains planned.

---

## 3.11. Anomalous S–P values

**Accepted.** The revised R2 response replaces "established regional earthquakes" with "consistent with regional earthquake distances." The 34 groups are described as having median S–P intervals of 6–13 s (approximately 50–105 km), which is consistent with regional sources; individual verification is planned as part of the travel-time residual analysis.

---

## 4. Instrument response

**Comment on 1–10 Hz range:** The stated range reflects the documented sensor response (L-4 natural frequency 1 Hz), the expected telemetry bandwidth (FM subcarrier, known filter corner not modelled), and conservative exclusion of frequencies below 1 Hz where the response roll-off and filter unknowns combine. We have added this justification to the supplement.

**Comment on relative vs. absolute calibration:** We have added a brief table to the supplement (Section S6) distinguishing applications that depend on absolute calibration from those that require only relative stability within a stable epoch, along the lines of the reviewer's Table.

---

## 5. Reproducibility and data accessibility

**Versioned processing manifest:** We will provide a manifest containing source-file checksums, software commit identifiers, clock-rule and metadata versions, configuration parameters, and output checksums. This is a data-deposit task at publication and cannot be completed before the archive is finalized; we have been explicit about this in the Data and Resources section.

**CSS3.0 export audit:** A sentence asserting that "the CSS3.0 export undergoes equivalent checking" was added without an audit being completed. That sentence has been removed. The existing SEISAN audit (Figure S14) is documented; a CSS3.0 audit will be added when the full revision is complete.

---

## 6. Response document presentation

The reviewer's comments on format are accepted. For the formal resubmission we will:

- Distinguish completed textual changes from outstanding computational work using explicit markers at the start of each response point.
- Provide manuscript line numbers for every substantive change.
- Remove statements that assert actions have been taken if they have not.
- State the analysis methods and expected outputs for each deferred computation, rather than simply naming it as planned.

---

## Summary of what has been corrected in this round

**In the manuscript (pinatubo_srl.tex):**
- 25/60 s uncertainty framing: "provisional nominal" values that "encompass observations but are not confidence intervals"
- RSAM–SSAM: now correctly described as relative synchronization, not independent validation
- October DST example: replaced with EVNT–trigger lag evidence
- Interchannel offset: documented on its own terms without comparison to absolute clock uncertainty
- Regional origins: replaced overconfident interpretation with cautious statement
- Duplicate counts: both numbers now explained with their pipeline context

**In the response letters:**
- R1 Q1: fabricated sensitivity numbers retracted
- R1 Q2: "minority" removed
- R1 Q3: Antelope contact not asserted as completed
- R1 comment 11: White 1996 described as temporal coincidence, not causal

**Outstanding (requires computational work before resubmission):**
See the deferred items listed in our R2 response — these are unchanged and represent the core of the full revision.
