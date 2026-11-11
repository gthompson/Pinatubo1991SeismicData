# Step 34 — Preliminary review of waveform/pick association

Snapshot: 11 November 2026. Based on saved stage-30/32 outputs and the current stage-10/22 indexes. This is a human-readable review, not an executable step or approval of pick associations. Refresh after rerunning stages 30–33.

## First finding: saved association outputs are stale

The current step-10 index has **21,449** waveform rows, whereas step 32 represents **19,785** waveform IDs. All older waveform IDs remain in the current index; **1,664 current waveform IDs are absent from the saved associations**. Step-30/32 files were last written around 21:33–21:35 UTC on 10 November; the waveform index was written at 22:09 UTC. Thus the figures must not be interpreted as the latest archive completeness.

**Next action:** rerun 30, then 32, then 33 using the latest index. Do not combine old association totals with the new waveform denominator. Step 33 now exports daily waveform-count differences to make this mismatch visible. The notebook consolidation preserves step-30/32 matching behavior; it does not rerun production association automatically.

## Saved-run results

| Product / class | Containers or source groups | Pick rows |
|---|---:|---:|
| Step 30 individual source groups successfully enclosed | 118 of 119 | 1,109 mapped primary picks |
| Step 32 WAV+PICKS | 2,925 | 18,975 |
| Step 32 WAV_ONLY | 16,860 | 0 |
| Step 32 PICKS_ONLY | 5,325 | 38,805 |
| Step 32 total | 25,110 | 57,780 |

All 57,780 step-22 pick IDs occur exactly once in the saved map, with no missing or extra IDs. This is a useful provenance/conservation result, **not a claim that every pick is linked to a waveform**. Waveform-linked picks comprise 1,104 primary and 17,871 secondary rows. Pick-only rows comprise 15 primary and 38,790 secondary rows.

The sole step-30 unresolved group is `91060310` (`no_enclosing_waveform`), also the group containing the GRN 59.99-second P/S rollover anomaly documented in step 24. The anomaly may contribute to lack of enclosure; inspect the waveform before changing the pick. Step-30 mapped totals and final primary totals differ because step 32 additionally deduplicates within each waveform container. Picks left unassigned enter pick-only containers, so this is not simple pick loss.

## Matching assumptions that require waveform review

- Step 30 requires exactly one file-wide interval enclosing the entire primary source-group pick span. It does not use filename equality or channel membership.
- Step 32 attaches secondary picks individually by file-wide time inclusion, expanded by 0.5 seconds. In overlapping windows, first input-order assignment wins. There is no nearest-arrival score or explicit competing-window report.
- Timestamp reconstruction from step-30 offsets reproduces the original pick timestamp. It does not fix independent PC clocks, drift, or PHA time parsing.
- Source groups can split across waveform and pick-only containers. Class counts therefore do not equal distinct earthquakes.
- Event-local duplicates that are not marked assigned can reappear as pick-only records. Conservation alone does not imply all resulting containers are independent events.

## Channel-presence screen

Joining saved waveform-linked picks to the current waveform index gives **127 picks whose station is absent** from the indexed station list and **181 whose channel code is absent** from the indexed channel list. These categories can overlap; do not add them. **179 linked PI2L/ELZ picks** have no ELZ in the indexed file channel list.

This screen is based on the index's separate station/channel lists. Absence is useful evidence, but presence in both lists does not prove that the exact station/channel combination exists, or that the matching trace spans the pick time. Full SEED-ID and per-trace time checks require waveform headers and plots. Do not automatically delete, rename or shift the picks.

The phase archive establishes that PI2L arrivals were picked; the sampled raw DMX archive and SSAM inspection did not find that channel. The source medium and missing-waveform question remain unresolved. Time-only PI2L associations must not be described as recovered low-gain waveform matches.

The detailed screen is retained in the project review workspace as `review/association-stage-2026-11-11/linked_pick_channel_screen.csv`; it is not a required pipeline input.

## Step-33 plot changes and limits

Time-series line plots are replaced by full-width daily bars. WAV+PICKS/WAV_ONLY are stacked without stacking their total again. The class chart stacks all three mutually exclusive container classes. Count plots use a full daily calendar with explicit zero counts; mean picks and primary fractions retain undefined days as blank rather than zero.

The original mean-picks calculation assigned dates by pandas row index after a separate merge; that could misalign dates and values. It now joins explicitly on event ID. Primary fraction includes pick-only rows and uses each pick's own date, not waveform start date. These plot changes do not alter associations.

## Next review stage

1. Rerun stages 30–33 after the current conversion; refresh the totals here.
2. Generate a plot for every waveform file with associated picks. Use full SEED IDs and per-trace time coverage; explicitly list unmatched-channel and outside-window markers rather than silently omitting them.
3. Distinguish primary/secondary sources, retain original group IDs and raw-line provenance, and flag partial waveform recovery, flat/clipped traces and clock uncertainty.
4. Prioritize `91060310`, PI2L associations, absent stations/channels and overlapping windows. Review a representative sample of ordinary matches as well.
5. Retain unmatched picks and waveform-only files as reviewable archive products. Scientific pick review proceeds with waveforms; the findings here do not require all ambiguities to be resolved before association.

No waveform-plus-pick plotting tool is implemented by this review file. No clock corrections or scientific pick edits were made as part of this consolidation.

## Follow-up: multiple picked events and low-gain marker targets

The saved map has 159/2,925 waveform-with-pick containers with more than one source group (at most three). Among individual picks, three waveform files contain multiple individual groups; among monthly picks, 119 files contain multiple monthly groups. Mixed-source group multiplicity alone can represent duplicate descriptions, so these are candidate multi-event files, not a measured earthquake count.

Strong examples: `91060317` contains individual groups `91060317` and `910603A7`, beginning 18:30:30.23 and 18:31:04.45; `9106031E` contains groups `9106031E` and `910603AE`, beginning 19:52:34.15 and 19:53:07.04 on June 3. Preserve group distinctions on waveform plots and confirm the corresponding signals. Detailed group timing is retained as `review/association-stage-2026-11-11/multiple_pick_groups_per_waveform.csv` in the review workspace.

Step 32 now adds waveform-overlay target columns while preserving original pick IDs, source SEED IDs and times. The rule recognizes PI2L as ELZ (current archive mapping) or LHZ (alternate user label). In the saved run, 102/179 associated low-gain picks share a file with PI2Z picks and are omitted from overlays; 77 target `XB.PI2..EHZ`. This file-wide preference intentionally may omit another event's low-gain pick; inspect multi-event recordings before adopting more selective per-group/phase rules. Scientific exporters still use original identities; future review plotting must explicitly honor `waveform_seed_id` and `include_waveform_overlay`. Rerun step 32 to populate these columns.
