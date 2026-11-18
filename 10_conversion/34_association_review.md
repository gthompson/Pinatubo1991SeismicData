# Step 34 — Review of waveform/pick association

Snapshot: 18 November 2026, from the UTC run (output root `utc/pipeline_tmp`). This is a human-readable review, not an executable step or approval of pick associations. Refresh it after rerunning steps 30–33.

The 11 November snapshot found the saved association outputs stale (step 32 covered 19,785 of 21,449 waveforms). That is resolved: steps 20–53 were rerun on UTC on 17 November. Step 10 was rerun on 18 November (only its logging of DMX files and dropped traces changed), and steps 30 and 32 still match the new index exactly: the same 21,459 waveform IDs, start times and WAV files, with no missing or extra waveforms.

## Results

| Product / class | Containers or source groups | Pick rows |
|---|---:|---:|
| Step 30 individual source groups enclosed by one waveform | 119 of 119 | 1,118 mapped primary picks |
| Step 32 WAV+PICKS | 7,715 | 54,922 |
| Step 32 WAV_ONLY | 13,744 | 0 |
| Step 32 PICKS_ONLY | 232 | 1,733 |
| Step 32 total | 21,691 | 56,655 |
| Step 32 suppressed duplicates (`*_suppressed_duplicates.csv`) | — | 1,413 |

All 58,068 step-22 pick IDs appear exactly once: 56,655 in the pick map and 1,413 in the suppressed-duplicates table, with no missing or extra IDs. This is a provenance/conservation result, **not a claim that every pick is linked to a waveform**. Waveform-linked picks are 1,113 primary (individual-file) and 53,809 secondary (monthly-file) rows; all 1,733 pick-only rows are secondary.

Compared with 11 November, WAV+PICKS rose from 2,925 to 7,715 and PICKS_ONLY fell from 5,325 to 232. This follows from the UTC clock model (monthly picks from 27 June are on the seismic PC's GMT clock, not the DMX header clock) and from attaching whole monthly blocks (below). Step 30 enclosed 5 primary picks that step 32 then drops as event-local duplicates (1,118 versus 1,113).

`91060310`, the only unresolved step-30 group on 11 November, is now enclosed by waveform `91060310` (9 picks). Its GRN S pick had S−P = 59.99 s because the S minute rolled over the wrong way; FloVoPy now skips an S earlier than P instead of moving it a minute later.

## Matching rules

- Step 30 requires exactly one file-wide interval enclosing the whole primary source-group pick span. It does not use filename equality or channel membership.
- Step 32 attaches each monthly block (one block = one event) as a whole, to the record whose window (expanded by 0.5 s) contains most of its picks; ties go to the earlier start, then the lower ID. Picks outside that record are kept and flagged `outside_record=True`. No source group is now split between a waveform container and a pick-only container (11 November: 98 groups, 336 picks).
- Event-local duplicates (same normalized SEED ID and phase within 0.5 s) are marked assigned and written to the suppressed-duplicates table, so they no longer resurface as pick-only events.
- Timestamps reconstructed from step-30 offsets reproduce the original pick time. They do not correct clocks or drift; see `TIMING_AND_CLOCKS.md` and `clock_regimes.csv`.
- A waveform container can hold several earthquakes, and distinct group IDs can describe the same earthquake in different source files. Class counts therefore do not equal distinct earthquakes.

## Channel-presence screen

Joining the 54,922 waveform-linked picks to the waveform index gives **190 picks whose station is absent** from that file's station list (CAB 138, PI2 20, BUG 15, CRW 8, PPO 4, BUR 2, PIE 2, QAD 1) and **1,706 whose channel is absent** from its channel list (ELZ 1,686, EHN 19, EHE 1). The two categories can overlap; do not add them. All **1,686 linked PI2L/ELZ picks** are in files with no ELZ channel.

The screen uses the index's separate station and channel lists. Absence is useful evidence, but presence in both lists does not prove that the exact station/channel combination exists, or that the trace spans the pick time. Full SEED-ID and per-trace time checks need waveform headers and plots. Do not automatically delete, rename or shift these picks.

The phase archive shows that PI2L arrivals were picked; the sampled raw DMX archive and SSAM inspection did not find that channel. The source medium of the low-gain waveforms is unresolved. Time-only PI2L associations must not be described as recovered low-gain waveform matches.

## Multiple picked groups and low-gain overlay targets

221 of the 7,715 WAV+PICKS containers hold more than one source group (at most three): 3 files hold more than one individual-file group and 180 hold more than one monthly block. Group multiplicity can also come from duplicate descriptions of one event, so these are candidate multi-event files, not an earthquake count.

Examples (UTC): waveform `91060317` holds individual groups `91060317` (11:30:30.23–11:30:38.28) and `910603A7` (11:31:04.45–11:31:11.27) on 3 June, plus monthly block `pinjun91_block0181` (11:31:06.38), which is probably `910603A7` again. Waveform `9106031E` holds `9106031E` (12:52:34.15–12:53:00.15), `910603AE` (12:53:07.04–12:53:22.59) and `pinjun91_block0188` (12:53:06.96). Keep group distinctions on waveform plots and confirm the signals.

Step 32's overlay columns keep each pick's source identity and time. Of the 1,686 linked low-gain picks, 704 target `XB.PI2..EHZ` (`include_waveform_overlay=True`) and 982 are omitted because their file already has PI2Z picks. This file-wide rule may omit another event's low-gain pick; revisit it with a group/phase-aware rule after waveform inspection. Scientific exporters still use the original identities; waveform plotting must honour `waveform_seed_id` and `include_waveform_overlay`.

## Step-33 plots

Daily bars on a full calendar: WAV+PICKS and WAV_ONLY are stacked without stacking their total again, and the class chart stacks the three exclusive classes. Mean picks and primary fraction leave days with no data blank rather than zero. Mean picks joins on event ID (the original joined by row index and could misalign dates). Primary fraction includes pick-only rows and uses each pick's own date. Step 33 reports no days where its waveform count differs from the index. These plot rules do not alter associations.

## Next review stage

1. Plot every waveform file with associated picks, using full SEED IDs and per-trace time coverage; list unmatched-channel and `outside_record` picks explicitly rather than omitting them.
2. Distinguish primary and secondary sources, keep original group IDs and raw-line provenance, and flag partial DMX recovery, flat or clipped traces and clock uncertainty.
3. Prioritise PI2L associations, absent stations and channels, multi-group files and overlapping windows; review a representative sample of ordinary matches as well.
4. Keep unmatched picks and waveform-only files as reviewable archive products.

No waveform-plus-pick plotting tool is implemented by this file, and no clock corrections or pick edits were made for this review.
