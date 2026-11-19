# 10_conversion code review — 17 Nov 2026

Review of notebooks 10–69, `lib/`, `legacy_bob/` and the relevant FloVoPy readers, checked against the legacy archive and the outputs in `~/work/Pinatubo1991Processing/converted`. The review itself changed nothing. Fixes made afterwards are recorded in the status table below and in each section. "Verified" means reproduced independently during the review, not only reported by one reviewer.

## Status (end of 17 Nov 2026)

**Done.** Committed in Pinatubo `b1e68b3`, `5879a32` and FloVoPy `e19c814`, `4fbe942`:
- **Clocks (§1).** Every seismic time and the RSAM/EVNT/SSAM labels are converted to UTC (the other BOB products followed on 18 Nov; see below). UTC is the default configuration; outputs are in `~/work/Pinatubo1991Processing/utc`. See `TIMING_AND_CLOCKS.md`.
- **Waveforms (§2).**
  - §2.1: the original sampling rate is kept, snapped to 3125/31 Hz, with the raw value in the index.
  - §2.5: the index is current and has `partial_recovery` columns. Ten files are partial recoveries.
- **Picks (§3).** All phase-card decoding is in FloVoPy, and `pha_parser` has tests. This fixed:
  - §3.1 (S-only minute), §3.3 (trimming logged), §3.5 (S quality kept) and §3.6 (minute 60 rolled over);
  - the blank-component channel bug from §3.8.
- **Catalogue (§4.6).** Steps 23–53 were rerun on the UTC data. The QuakeML now has magnitudes (3,414 events), and the step-51 table has a P_H column.
- **Hypocentre linking (§4.1).** Step 50 links hypocentres through their picks first (first P 0–10 s after the origin), then by record window (10 s before the start to the record end), one-to-one.
  - The policy is `picks_then_window`, in `lib.shared.associate_hypocenters`.
  - 4,628 of 4,690 hypocentres are now linked (previously 3,076): 4,446 through picks, 182 by window.
  - Only 62 remain H-only; 46 of those lie inside a record already linked to another hypocentre.
- **Preferred origin (§4.2).** Step 43 now records `preferred_origin_id` and `preferred_origin_source`, and step 50 marks that origin as preferred. Each event gets a `preferred_origin:` comment; steps 51–52 already read `preferred_origin()`.
  - The preferred origin changed in 608 of the 3,507 two-source events.
  - 134 of those have a different time, by up to 1.5 s.
  - 85 events have only a `PINAALL.DAT` origin.
- **S earlier than P (§3.4).** FloVoPy no longer moves an S reading one minute later when its seconds are below the P seconds.
  - None of the 34 such cards in the 1991 files is a genuine minute wrap. Each S lies 0.01–5.6 s before its P, and the forward roll gave S−P of 54–60 s.
  - Analysts wrote S times past the minute as seconds ≥ 60 (506 cards), following the HYPO71 convention.
  - Such S readings are now skipped and logged ("S … earlier than P; S skipped"), and the P is kept.
  - Effect:
    - 3 S picks dropped: 2 from the single-event PHA files, including event 91060310. The other ~31 had already been trimmed as outliers by the block median filter, and are now logged as skipped instead.
    - The largest station S−P in the merged picks fell from 59.99 s to 17.65 s.
    - Event 91060310 is now a single waveform+picks event; pick-only events dropped from 482 to 481.
- **Step 32 splits and resurfacing duplicates (§3.7).**
  - Monthly picks are now attached by whole source block. Each block goes to the record containing most of its picks.
  - Picks dropped as duplicates are marked as used and written to `32_waveform_pick_event_map_suppressed_duplicates.csv`, with the container they duplicate.
  - The real cause was duplicates, not window edges. 249 of the 481 pick-only events consisted entirely of copies of picks already in a waveform event: repeated copies of the same event in the monthly files, and overlapping single-event PHA files.
  - Results:
    - Suppressed duplicates: 1,403 picks from 250 source groups.
    - Split blocks: 78 → 0.
    - Pick-only events: 481 → 232; picks with no waveform: 3,130 → 1,733.
    - Step 50 classes: W+P+H 4,234 → 4,352; P+H 239 → 121; P only 242 → 111.
    - Every pick ID now appears exactly once, in the pick map or the suppressed table.
- **Blocks missing a separator (§3.2).** A monthly block is now split where consecutive picks are more than 20 s apart and each piece has at least 3 stations (FloVoPy `split_picks_by_time_gap`).
  - Smaller stray pieces stay with the nearest event and are trimmed as before.
  - The second event gets the ID `<file>_blockNNNN_2`; the first keeps its existing ID.
  - Each split is logged in the step-21 log as "holds 2 events".
  - Effect:
    - 12 blocks split: 4 in August, 8 in July, with gaps of 49 s to 16 h.
    - 67 picks recovered that were previously trimmed as outliers.
    - Monthly events 8,218 → 8,230.
    - Step 32: 8 of the new events have a waveform (WAV+PICKS 7,707 → 7,715).
- **Nordic export (§4.3–4.5).** FloVoPy `flovopy.seisanio.rea_export` now repairs ObsPy 1.5's header writer:
  - **Origin times** in type-1 (`SS.s`) and high-accuracy H (`SS.sss`) lines are rewritten. They are rounded half-up in integer nanoseconds, carrying into the minute, hour and day as needed. ObsPy had written the first digit(s) of the microsecond integer, so 58.08 s became "58.8".
  - **Magnitude type:** a magnitude with no type is written with a blank type instead of "W".
  - **RMS:** step 43 now carries the HYPO71 `rms` and `nass` from `Pinatubo_all.sum`, and step 50 puts them in `origin.quality`, so the Nordic RMS column is filled. 3,799 of 4,690 located events have it; events with only a `PINAALL.DAT` origin have no RMS to give.
  - **Writer version:** the export fingerprint now includes it, so earlier exports are not reused unchanged.
- **Step 52 had been failing silently.** On every rerun after the first UTC run, all events failed with "Event/settings changed; explicit overwrite required". The notebook only printed the failures, so the REA files on disk stayed those of the first UTC run.
  - Step 52 now raises an error if any event fails.
  - Same-second events get a shifted S-file *name* (`collision="shift"`, 4 events) instead of failing; origin and pick times are unchanged.
  - The stale folder was moved to `FAIR/SEISAN/REA_stale_first_utc_run_20261117`, and all 21,753 S-files were regenerated.
- **Step 51 reporting windows (§4.9).** The windows are now derived from the UTC catalogue instead of typed in.
  - Rule: a day is covered when it has at least one event with a waveform record, one with picks and one with a hypocentre. Covered days with gaps of at most 3 uncovered days form one window. Edge days where any source is below 25% of the window's daily median are trimmed.
  - Result: 8 May–10 Jun and 29 Jun–18 Aug 1991 (previously 8 May–9 Jun and 30 Jun–18 Aug, chosen on recorded times). 11,753 events fall in the windows (11,552 with the old dates).
  - The trimmed edges: 6–7 May (1–2 hypocentres a day), 11 Jun (1 pick event) and 19 Aug (7 hypocentres).
  - Days inside a window that lack a source are listed and kept: six May days with no hypocentre, and 6 July with no monthly picks or hypocentres.
  - New output `51_daily_source_coverage.csv` gives W, P and H counts for every day. The daily-counts CSV now has a row for every window day, including days with no events.
  - The waveform-only days 12–28 Jun and from 20 Aug are outside the windows by design, since they have no picks or hypocentres.
- **Magnitude 0.00 (§4.7).** HYPO71 writes 0.00 when no magnitude was computed. All three step-40 parsers (summary, original PINAALL, annotated PINAALL) now write it as an empty `magnitude`, keep the value in `reported_magnitude` and set `magnitude_missing_reason`, using `lib.shared.mark_missing_magnitudes`.
  - 1,806 of 4,632 `Pinatubo_all.sum` rows and 960 of 3,601 `PINAALL.DAT` rows.
  - Step 42 applies the same rule, so it also handles indexes written before the fix. Its mismatch counts are unchanged (3,354 and 4,384), because 0.00 had matched 0.00 just as missing now matches missing.
  - Step 43 reads `reported_magnitude` and keeps its `zero_magnitude_is_missing` switch. Its outputs are byte-identical, so steps 50–53 did not need rerunning.
- **All-zero SSAM rows (§5.1).** Step 62 flags a row with 0 in all 16 bands as `qc_all_zero` (a dropout) and leaves it out of coverage, the daily counts, the value plots and the spectrograms. The rows stay in the records export.
  - 36,037 of 635,862 SSAM rows and 1,188 of 41,632 DATA rows. Usable SSAM rows: 635,862 → 599,825.
  - Station days: DON1 38 → 30, BUR1 2 → 1. BUR1, PPE1 and PPO1 are 83–84% zeros.
  - The step-69 network total is unchanged (SSAM 135 days), because other stations cover the days DON1 and BUR1 lose.
  - 4,233 rows (DON1 and PI21) have every band 0 except BN01 = 1 or 2. They look like dead channels; they are flagged `qc_near_zero` and counted in `review.json`, but kept.
- **BOB products (§5.2–5.7).** One change to the helper cell shared by notebooks 61–68, plus product-specific fixes:
  - **RSAM/EVNT window labels.** Each 10-minute value is labelled at the END of its window. EVNT counts match the waveform triggers in the 10 minutes before each label (correlation peaks at 0–60 s lag; the 10 minutes after the label correlate less). RSAM, EVNT and TILT exports now have `window_start` and `window_end` columns. TILT is assumed to follow RSAM (same BOB software and annual layout). The SSAM `TIME_ANCHOR` in nb 62 is a separate question: SSAM was recorded on the MDETECT PC clock, so it cannot be tested against RSAM at the 1-minute level, and it stays an explicit display convention.
  - **Rain carried forward.** Fields 2–7 are now named from `BOB_QPLO.TXT` (CUAD, BUGZ, PIEZ, BALR, FNGZ, SACR). Values equal to the previous row in the same file are flagged `<field>_carried` (CONVERT.FOR repeats every other channel's last value). `field_quality.csv` has a `fresh_values` column, and the plots show only fresh values. For example, CUAD has 3,464 values but 1,300 fresh; PIEZ 8,737 but 5,305 fresh. An unchanged fresh reading looks the same as a carried one, so these are lower bounds on fresh readings.
  - **Tilt saturation.** Values of 8192 (2^13, the digitizer limit) are masked in `value_01` and flagged `value_01_saturated`: 64 samples (46 UBOR, 18 UBOT) from the 12 June 23:00 label. The mudflow maxima 8016 and 8010 are single values, not a repeated limit, so they are left alone.
  - **Mudflow skipped files.** Files that are not `.DAT` are listed in `skipped_inputs.csv` with a reason (10 files). `PROGS/10AUG91.DA2`–`11AUG91.DA5` are GMT copies of MARL, ABAC, USAC and GUMA written by CONVERT.FOR: 36–58 of each file's 36–59 records appear in the station file 8 h later. For hours 16–23 the DA files keep the local date (one day late); the station files have the correct dates. No data are lost. `GUMA/GMT_2LOC.BAS`, a time-conversion program, is also listed.
  - **Auxiliary sources.** TEST arrays and BUG1/TEMP stay in the records export but no longer count as coverage. The step-69 network totals are unchanged, as expected.
  - **Tests (§5.7).** `tests/test_bob_notebooks.py` now joins the notebook cell source, so both BOB tests pass. New `tests/test_bob_products.py` covers the window columns, auxiliary sources, rain flags and the mudflow file list.
- **Other BOB clocks (§1).** Each product was tested against RSAM/EVNT, which are already verified in UTC (scripts `bobclk_*.py` in the session scratchpad). New rules in `clock_regimes.csv`, applied by `apply_bob_clock`. Each clock is named after the PC that acquired the data (PVO system diagram, after Murray): RSAM/EVNT on a Tandy Model 100 (stream `tandy_rsam`, clock R, which this review first called "B"), SSAM on WILLIE, tilt on SLEEPY, lahar on RICKY. BOB ran on the aggregation PC (PORKY or DAVE/ANDY) and its clock stamped no data.
  - **Mudflow and rain** (`ricky_lahar`, clock L: RICKY; rain is assumed to share it, since it matches mudflow at 0 h throughout): the rule comes from the data, whatever the "converted from GMT to local" headers say. Mudflow matches RSAM at −8 h until 12 Sep (r = 0.4–0.7 for USAC, GUMA, MARL) and at 0 h from 15 Sep; rain matches RSAM at −7/−8 h and mudflow at 0 h. Hours 00–07 carry the correct date, so the `CONVERT.FOR` rollover bug did not reach these files. The GMT rule runs to 14 Nov; the new-format files from 15 Nov have nothing to test against and keep their labels (`unverified`).
  - **Hourly CAB counts** (`cab_hourly_counts`, clock C; entered by hand, probably on the BOB PC): despite the "TIME IN GMT" header, they match EVNT at −8 h until 13 Sep (weekly r up to 0.49) and at 0 h after (r up to 0.64). Each hh:30 label is the centre of its hour (best match with EVNT summed over label ± 30 min), so `window_start`/`window_end` are set for CAB.
  - **Tilt** (`sleepy_tilt`, SLEEPY, −8 h, 1 h uncertainty): the files were rewritten to local time on 18 Nov 91, and PBAT solar charging starts at the 07:00 label and peaks at 11:00–14:00 every day, so the labels are PHT.
  - **COSPEC, daily PIE counts and radio maps** were entered by hand and carry dates only; they keep them (`clock_status` `date_only`). COSPEC dates are taken as Philippine days (daylight measurements), with `window_start`/`window_end` from 16:00 UTC the day before. Whether the PIE counts use Philippine or UTC days cannot be told (r = 0.64 vs 0.62 against EVNT).
  - **RSAM against summed SSAM (step 69).** SSAM bands BN01–BN10 summed give a 1-minute RSAM proxy; the daily lag per station pair is in `overview/ssam_rsam_lag_by_day.csv`. Of 412 reliable station-days (r ≥ 0.5), all but six lie within 4 min, with a sawtooth drift of about 1 min/day between the Tandy and WILLIE. On 10 Nov all five pairs were exactly 8 h apart, and EVNT vs waveform triggers agrees: the RSAM PC (Tandy) ran on PHT for that block (labels 01:20–22:20), now rule `R_PHT_10Nov`. After the rerun, 411 of 412 station-days are within 4 min; the exception is FNG1 on 19 Oct (−58 min, r = 0.52), seen on no other station.
  - Labels that no rule covers now keep their recorded value with `clock_status` `unverified` instead of becoming invalid.
  - Effect in step 69 (UTC days): tilt 13 → 14 days (from 31 May), rain 120 → 122 (from 15 Jul), counts 176 → 178. Tilt now has 432 `outside_expected_dates` rows: the empty padded first 8 h of 1 Jan, moved into 1990.
- **Waveform bookkeeping (§2.2–2.3).** FloVoPy `convert_dmx_to_seisan` now accounts for every DMX file and every trace it does not write:
  - `10_file_log.csv` has one row per DMX file (21,475): 21,459 `existing`, 11 `failed`, 5 `empty`. The converter refuses to finish if the rows do not add up to the files found.
  - The 5 `empty` files: `9105080Y` and `91050813` hold an IRIG trace and five flat channels; `9106133O`, `9107077R` and `9108071V` are header-only (1.3–1.5 kB, traces with no samples).
  - Dropped traces are provenance rows with `trace_status` `flat`, `irig` or `no_samples` (no `fixed_id`), and the index has `dropped_traces`/`dropped_trace_ids`: 16,586 flat traces in 12,292 files, 514 IRIG, 25 with no samples. DONZ is flat in 9,543 saved files, then CABZ 1,188, BURZ 940 and BUGZ 854.
  - The index and the kept provenance rows are unchanged (21,459 files; 152,933 traces). Step 11 now uses only kept traces; it had not been rerun since the UTC conversion, so its daily counts are now on UTC dates.
- **StationXML epochs on UTC (§2.4).** Step 15 rebuilds every channel epoch on UTC:
  - Field-note dates and times are Philippine local time and are converted: a date-only bound is local midnight (16:00 UTC the day before), ends are exclusive. E.g. PIE ends 15 Jun 16:00 UTC, the CAB attenuation change is 5 May 04:45 UTC.
  - Post-eruption identities start at their first surviving UTC header; CAB EHN now starts 3 Jul 07:51 UTC (it had inherited CAB's conventional 16 June). CAB/BUG Z continue from 15 Jun 16:00 UTC.
  - The archive decides the rest. UBO data precede its documented installation, so its start moves to 30 Apr 12:10 UTC. BUR, GRN, PIE, PPO (Z/N/E) and UBO still carry their labels for 6–10 days after their documented ends (to 21–25 Jun), so each gets a `label_after_documented_end` epoch with an unknown sensor; the signal source of those traces is unverified.
  - Epoch sample rates are 3125/31 Hz (were 100 Hz). 33 epochs (were 26), schema-valid; the only overlaps are shared endpoints, removed in the selection-safe copies.
  - All 152,933 archived traces fall inside an epoch of their ID; the build fails otherwise. Step 16 has a test for the UTC conversion and the coverage rule.
  - Steps 61–69 were rerun, and their saved outputs were refreshed.

- **FloVoPy REA manifest (§4.10).** `export_catalog_to_rea` saves its manifest once per run, with 60 s checkpoints, instead of after every S-file: a full step-52 rewrite takes 38 s instead of 33 min, with identical output. Repeated ObsPy writer warnings are summarised once with a count.
- **Tests and docs (§6, §3.8).** `NOTEBOOKS_README.md`, the top-level `README.md`, `CONFIGURATION.md` and `34_association_review.md` now describe the current UTC pipeline. A stale configuration test is corrected. Notebooks still showing outputs from the old root were rerun with byte-identical results.
- **Analysis follow-ups and `20_analysis` cleanup (18 Nov).**
  - Notebook 20 now has the earliest-onset histogram and the frequency and Hilbert plots for several example files; the old pilot notebook is retired. The analysis reads the UTC archive; its output root is `~/work/Pinatubo1991Processing/analysis`.
  - Analysis notebook 10 used to re-execute step 15's code and overwrite the canonical step-15 products, but not their audit, selection-safe copies or provenance. It is now read-only (copies the products and records their hashes). Steps 15 and 16 were rerun: every CSV is identical to the overwritten files, the XML differs only in its creation time, and `chronology.md` gets back its correct `source_metadata/` link. A layout test that wrote into the step-15 folder now uses a temporary one.
  - Analysis inputs now come from `conversion_config.json` (UTC WAV database, step-15 products); `wav_root` is only an override. The unused `network_history_products` setting and its helpers, the Box pre-UTC waveform fallback, the `pilot` sampler and a hard-coded hash check are removed; the two config-backup copies are merged.
  - Time labels say UTC (they said recorded, uncorrected clocks), in metric provenance (`time_basis`, `epoch_status`), captions, axes and notebook text.
  - Retired to `20_analysis/superseded/` (with a README): the old command-line pipeline and its configs, READMEs and smoke outputs; the COSPEC plotter and notebook 70, which duplicated step 66 and labelled dates GMT midnight (its stray output under `utc/legacy_bob/` was removed); and `legacy_rsam_analysis.ipynb`, which read raw RSAM and pre-UTC waveforms (step 61 replaces it). `pytest.ini` limits collection to `tests/`. The unused `FIX_FS` path key is removed from `lib/config.py`.
  - Two faults found by rerunning: `analysis_workflow` read cached CSVs with default types, so a hex `record_id` such as `…e123` became `inf` and notebook 20 failed on reuse; ID columns are now always read as text (`read_ids`). Notebook 60 kept re-copying a draft selection for an earlier run; an unapproved draft for another run is now kept as `publication_selection.draft_<run>.json` and replaced, while an approved one still stops the notebook.

- **`legacy_bob` retired (18 Nov).** The old BOB command-line converter and its two tests moved to `superseded/legacy_bob/`; steps 61–69 replace it. The `legacy_bob` block stays in `conversion_config.json` (step 66 reads its COSPEC units); only its runner-only `products` list was removed.

- **Full metric export (18 Nov).** `all_window_metrics.csv` (~29 GB, ~1 h) is replaced by `all_window_metrics.parquet`, written by the new FloVoPy `EnhancedCatalog.export_sidecars_parquet`; `exports.all_window_metrics: none` skips it. Stale CSV/Parquet exports are removed on each export.

- **DPSS code moved to FloVoPy (18 Nov).** `flovopy.processing.spectrograms` now has `dpss_tapers`, `multitaper_psd` and `compute_multitaper_spectrogram_result` (density-normalised, equal-weight, gap-rejecting); `matched_psds` and notebook 40 use them, with bit-identical results on real traces. The unused dB-only `_multitaper_spectrogram` is removed. Two leftover "no UTC correction" labels (metric provenance, pretrigger figure footnote) now say UTC.

- **Engine tests moved to FloVoPy (18 Nov).** The Hilbert and window-strategy tests in `20_analysis` duplicated FloVoPy's `test_pretrigger_diagnostics.py` and `test_pretrigger_windows.py` and were deleted; the frequency-check and single-trace tests moved to FloVoPy `test_pretrigger_trace_analysis.py`. `20_analysis/tests/test_pretrigger_qc.py` now tests only the batch adapter, and `pretrigger_qc` no longer re-exports FloVoPy private functions.

- **Quick cleanups (19 Nov).**
  - Stale annotated-parser outputs (`pinatubo_hypocenters.csv`, `pinatubo_annotations.csv`, `pinatubo_parse_errors.csv`, `pinatubo_parse_summary.txt`) removed from `10_conversion`.
  - `pinatubo_steps/` renamed `lib/` throughout (notebooks find it by `lib/clock.py`). `dmx_recovery.py` and step 10's candidate-saving section removed; the unused `parse_individual_pha_file` and the re-exports of FloVoPy's `parse_seed_id`/`get_comment_value` removed (steps 50–52 import them from FloVoPy).
  - Network history and StationXML moved from steps 15/16 to **70/71**, after the BOB steps, so they are built once every dataset is on UTC. Outputs are now `step70_provenance.json` and `validation/71_*.json`; the analysis checks the new name. Rerun: products identical apart from timestamps and hashes; 71's checks pass.
  - Retired to `superseded/` (with a README): the early SSAM/mudflow coverage scripts and their README, `refactor_manifest.json` and `unpack_bob_archives.py` (a command-line copy of step 60). `FLOVOPY_MIGRATION_REVIEW.md` was rewritten as a current note on what lives in FloVoPy, what stays in `lib/`, and candidates to move, then folded into `NOTEBOOKS_README.md` ("FloVoPy and `lib/`").
  - The external test runner (`tests/run_external_tests.py`) failed in its sandbox because `build_all` expects a header scan there (this predated the move). It is removed: its rebuild only repeated running step 70, and its other suites already run under pytest. Step 71's controlled reconstruction checks, which nothing else ran, now run with the conversion tests (`tests/test_network_reconstruction.py`, 5 tests).

**Remaining**, roughly in priority order:

| # | Item | Section | Notes |
|---|---|---|---|
| 1 | Clock drift and residual clock questions | §1 | RSAM 2–9 May (−15 h) still has low confidence, and rain/mudflow from 15 Nov are unverified. The IRIG generator was free-running (no GOES or GPS clock at PVO), so IRIG gives no absolute UTC. Absolute anchors need earthquakes outside the volcano: none of the 8,233 HYPO71 origins is beyond 49 km (largest magnitude 1.88), but 34 pick groups (27 Jun – 21 Aug) have median S−P of 6–13 s (≈50–105 km), four with 2–4 consistent S picks. None of these is in ISC/USGS. Step 72 (19 Nov; see `TIMING_AND_CLOCKS.md` §5) checks against ISC/USGS. SSAM shows nothing at distant arrivals, and local catalogue events rule out minute-level errors after the reset. Six analyst-reviewed anchors: −40 s on 21 May (before the reset); +12 s twice on 30 May, −23 s on 8 Aug, +4 s (relative to pP, to be confirmed) on 15 Aug and +9 s on 5 Sep. Drift, not a systematic error, so no correction. The nominal uncertainty after the reset is raised from 5 s to 25 s in `clock_regimes.csv` (19 Nov); it reaches the indexes and QuakeML at the next rerun from step 10. |
| 2 | Publication examples | — | Notebook 60 now writes a fresh draft selection for the UTC run; the examples still need choosing. |

## 1. Clocks (cross-cutting; affects the paper's timing claims)

| Data | Observation | Status |
|---|---|---|
| DMX waveform headers vs. hypocenter summaries (`Pinatubo_all.sum`) | Within ±30 s of origin + 7 h: 4,334 of 4,632 origins have a waveform start (median waveform start −9 s relative to origin + 7 h). At offsets of 0, ±8 h only ~520 match (chance level). Holds before and after 30 May. | Verified |
| DMX filename date vs. header start date | Equal until ~25 May; from ~26 May the filename date matches (header − 8 h) for ~94% of files, 0 h for ~6% | Reported (stage 10–16) |
| Monthly picks | Before ~12 June on the waveform clock; from 29 June on the hypocenter clock | Reported (stage 40–54) |
| BOB RSAM / EVNT / TILT | Labels are Philippine local time (UTC+8), and each RSAM sample is labelled at the end of its 10-min window (eruption onsets 12 June 08:51, 22:52 and 15 June 13:42 PHT appear at the 09:00, 23:00 and 13:50 labels). TIME.TXT says "phased over 48" samples = 8 h. | Reported (stage 60–69) |
| BOB COUNTS / COSPEC | Headers say "TIME IN GMT" | Reported |
| BOB RAIN / MUDFLOW | Most files say "converted from GMT to local", but 15–22 Nov 1991 files don't. CONVERT.FOR does `hr-8` (GMT−8, the wrong direction for the Philippines) and never rolls the date. | Reported |

### Follow-up analysis (same day): likely origin of the 7 h offset

These checks used the converted index (`10_waveform_index.csv`) and the hypocenter indexes. The analysis script is in the session scratchpad (`offs.py`).

- **Hypocenter offset, week by week**: 82–100% of origins in every week from 6 May to 19 August, in both `Pinatubo_all.sum` and `PINAALL.DAT`, have a waveform starting 0–20 s before origin + 7 h. Offsets of 0, 6, 8, 9 and −8 h give 0–7%, which is chance level.
- **The annotated PINAALL classes don't change this.** LOCAL (10–26 May), LOCAL_PLUS_1MIN (21–23 May) and UTC (from 30 May) all give the same +7 h offset and the same median residual (−10.0, −9.9 and −9.5 s). No 8 h step at 30 May and no 60 s step appear relative to the waveforms. So hypocenters and waveforms share one PC clock: picks were made on these waveforms. The annotations describe that shared clock against true time, not a difference between the two.
- **DMX header vs. filename date**: (header start − filename-date midnight) is exactly 7.00–31.0 h for files dated 26 May–27 Oct 1991, and 8.0–32.0 h from 28 Oct 1991 onward. Before 26 May the filename date equals the header date, so the naming changed then, not the header clock. The hypocenter offset is already 7 h before that.
- **27 October 1991 is when US Pacific Daylight Time ended.** Microsoft C runtime libraries default to `TZ=PST8PDT` when `TZ` is unset. The most likely origin: the acquisition or demux software converted the PC wall-clock time to a Unix epoch with `mktime()`, assuming US Pacific time. That adds 7 h during PDT and 8 h during PST. ObsPy reads that epoch as UTC.
- **Implied correction**:
  - Waveform header − 7 h (until 27 Oct 1991 02:00 PDT) or − 8 h (afterwards) recovers the PC wall clock that the hypocenters and later picks use.
  - Whether that wall clock was Philippine local time or GMT on a given date is a separate question. It needs the annotations, Lockhart et al., and IRIG/TIMECORRECT evidence.
  - Monthly picks before ~12 June appear to be on the header (+7 h) clock rather than the wall clock. This needs checking.

### Clock model (17 Nov, after checking picks and BOB)

The data use three clocks:
- **H**: the DMX header epoch, as ObsPy reads it.
- **W**: the seismic PC's wall clock.
- **B**: the BOB labels. [Corrected 18 Nov: these are the RSAM labels from the Tandy Model 100 RSAM PC, now clock R; no data were stamped by the BOB PC.]

The scripts are in the session scratchpad (`pickclock.py`, `bobxc.py`).

**H → W.** W = H converted to America/Los_Angeles local time:
- 7 h until 27 Oct 1991 02:00 PDT, 8 h afterwards.
- Evidence: the filename date matches (H − 7 h) from 26 May to 27 Oct and (H − 8 h) from 28 Oct. EVNT cross-correlation steps from −7 h to −8 h on the same date.
- This is consistent with DOS `mktime()` with the default `TZ=PST8PDT`.

**W was Philippine time (PHT) until 26 May, then GMT.**
- On 26 May the PC clock was set back 8 h.
  - In legacy file order, `91052617` (H 15:41:33) is followed by `9105261G` (H 08:40:26).
  - PINAALL file order shows the same reset: line 217 is 08:31:25, line 218 is 01:40:38.
  - 01:40:38 + 7 h matches `9105261G`.
  - The reset happened at about W 08:41 PHT = 00:41 UTC on 26 May.
- EVNT and RSAM cross-correlation, with B in PHT (verified against eruption onsets):
  - B = H − 7 h for 10–26 May, which gives W = PHT.
  - B = H + 1 h for 26 May to mid-September, which gives W = PHT − 8 h = UTC.
- The DMX headers mark the same reset. The SUDS trace descriptor field `localtime` is 0 in every file up to `91052617`, and -25200 (-7 h, in seconds) from `9105261G` onward. It is -25200 in all files checked to December, so it does not follow the 27 Oct DST change. The acquisition software (or its setup) changed at the reset. `timecorrect` and `rate_correct` are 0 throughout.
- MDETECT documentation (Lee, Tottingham & Ellis 1988, OFR 88-751):
  - The PC/AT clock is read when MDETECT starts. After that, time is kept by counting A/D samples ("good to about 2 seconds per day").
  - The digitized time signal is not used.
  - Absolute header times can therefore drift by seconds between restarts; the IRIG trace is the only way to correct this.
  - SUDS (OFR 89-188) defines all times as GMT seconds since 1970.
- The PINAALL annotation "LOCAL until 91 530, then GMT" describes this reset, but the waveforms put it on 26 May, not 30 May.
- No other 8 h jump appears in the waveform sequence for the rest of 1991.
- Implied UTC:
  - H − 15 h before the reset.
  - H − 7 h from the reset to 27 Oct.
  - H − 8 h after 27 Oct.
- The PHT→GMT change itself (26 May) lies between consecutive files, so file sequence decides which side a waveform is on. Wall-clock times from about 00:41 to 08:41 on 26 May occur twice.

**Which clock each stream uses** (checked by fraction inside waveform windows, and against origins):

| Stream | Clock |
|---|---|
| Waveform headers | H |
| Single-event PHA files (3–16 June) | H (100% inside waveform windows at offset 0) |
| Monthly PHA, 1 May – 10 June 00:06 | H (100%); hypocenters ≈ picks − 7 h |
| Monthly PHA, 27 June onward | W (88–100% at +7 h). The switch falls in a gap in `pinjun91.pha` (10–27 June) that contains new HYPO71 header/control cards at lines ~5632–5666 |
| Hypocenters, `Pinatubo_all.sum` and `PINAALL.DAT`, all dates | W. `Pinatubo_all.sum` is in time order, so records in the duplicated 26 May window must be resolved by matching |

**RSAM labels ("B" here; clock R from 18 Nov):**
- PHT from 10 May to about 13 September. This matches the RSAM eruption-onset check.
- Equal to H − 7 h (= UTC) from about 14 September, and H − 8 h (= UTC) after 28 October, so the RSAM PC switched to GMT around 14 September.
- 2–10 May: B = H, not yet explained. The seismic clocks are consistent there (hypocenters = H − 7 h), so this is on the RSAM side.
- Tilt PPO `TIME.TXT`: "phased over 48 … from GMT to local, 18 Nov 91".

**Other findings:**
- Some files are out of sequence (`91051014`–`17`, `9105161 0`–`14`). Their H is hours earlier than their neighbours, with no lasting clock change.
- The annotated "LOCAL_PLUS_1MIN" records (21–23 May) show no 60 s step relative to the waveforms. If the PC clock was 1 min off, it affected all seismic streams equally.

Consequences (status in brackets):
- [Fixed: UTC run, and step 50 now links through picks, then by record window] Step 50 matches waveform events to hypocenters with ±10 s and no clock offset, so almost no waveform–hypocenter pairs are found before ~21 May. After a 7 h correction, the ~9 s pretrigger still leaves only ~2,900 of ~4,500 pairs within ±10 s.
- [Avoided: the UTC run uses the original parser and the clock table] The annotated PINAALL parser's local→UTC switch (`LOCAL_UTC_OFFSET=8`) would move pre-30-May origins to 15 h from the waveforms.
- [Removed 19 Nov: these were stale outputs of the annotated parser, not used by the pipeline] The committed `pinatubo_hypocenters.csv` contains −8 h times, but `pinatubo_parse_summary.txt` says offset None, so the two files come from different runs.
- [Fixed: every BOB product is on UTC since 18 Nov, except rain/mudflow from 15 Nov, flagged `unverified`] Notebook 69 overlays series that are on different time bases.

### UTC run (17 Nov)

**Implementation.**
- Rules: `clock_regimes.csv`, applied through `lib/clock.py` and FloVoPy `flovopy.core.clock` (`ClockRegimes`, `dos_mktime_wall_clock`).
- FloVoPy's `convert_dmx_to_seisan` gains a `time_correction=` hook.
- Now the default in `conversion_config.json` (`conversion_config.utc.json` was retired); outputs go to `~/work/Pinatubo1991Processing/utc`.
- Raw times are kept in `raw_*` columns and the applied rule in `clock_*` columns.
- Step 10 was re-converted from the DMX files once, which also applies the original sampling rate (3125/31 Hz).

**Rules applied:**

| Item | Count by rule |
|---|---|
| Waveform files | H_UTC_PDT 18,905; H_UTC_PST 1,274; H_PHT 1,247; H_PHT_seq 19; H_UTC_seq 14. 11 read failures; no clock failures |
| PINAALL.DAT | W_UTC 3,374; W_PHT 210; plus 17 resolved by line order |
| Pinatubo_all.sum, 18 records in the repeated 26 May window | 13 matched to PINAALL (±1 s, ±0.05°); 5 placed by the waveform file whose header window contains raw + 7 h |
| Picks | 0 unresolved |

**Results:**

| Check | Recorded run | UTC run |
|---|---|---|
| PINAALL origins inside a waveform window (May–Aug) | 6–14% | 96–99.6% |
| Pinatubo_all.sum origins inside a waveform window (May–Aug) | 6–14% | 95–99.6% |
| Picks inside a waveform window, Jul–Aug | 10–14% | 95–98% |
| Picks inside a waveform window, Jun | 77% | 99.4% |
| Step 32 WAV+PICKS / WAV_ONLY / PICKS_ONLY | 2,928 / 18,521 / 5,339 | 7,712 / 13,747 / 482 |
| Picks in PICKS_ONLY events | 38,997 | 3,141 |
| Step 50 hypocenters associated with waveform/pick events (±10 s start-time rule) | — | 3,076 of 4,690 |
| BOB EVNT vs waveform trigger rate | — | best lag 0 in every window from May to December (one weak window in October, r = 0.08) |

**Not converted:**
- [Fixed later: see the status list] Tilt, rain, mudflow, COSPEC, counts and radio labels were kept as recorded and flagged `unverified`.
- BOB 2–9 May (`R_ahead`, −15 h) is based only on the cross-correlation and has low confidence.
- The exact BOB switches are uncertain by about ±3 h: 9 May ~06:00 and 13 Sep ~09:00 (label time).

**Step 54** (CSS3) needs an Antelope Python environment and was not run natively; it has been run on the UTC catalogue with a mock Datascope (§4.8).

**Update (later on 17 Nov): header marker, uncertainty, default.**
- J. Power (email to R. White, 29 Oct 2026) confirms the model:
  - MDETECT "always assumed that the computer was in California and corrected the time to GMT".
  - The PC was rebooted daily.
  - Harlow installed a new MDETECT version and set the clock to GMT "on or about May 25".
  - The published account is *Fire and Mud*, p. 227 ("local versus GMT").
- Step 10 now records the SUDS descriptor fields `dmx_localtime` and `dmx_timecorrect` in the waveform index.
- The waveform rules in `clock_regimes.csv` require both the header marker and the file order:
  - `localtime` 0 and an ID up to `91052617` gives the old version on PHT.
  - `localtime` −25200 and an ID from `9105261G` gives the new version on GMT.
  - A file where the two disagree gets no rule and fails visibly.
- Each rule carries a nominal `uncertainty_s`, written to `clock_uncertainty_s` in every index and to the QuakeML origin `time_errors` (step 50):
  - 5 s after the reset: MDETECT keeps time by counting samples, about 2 s/day, and the PC was rebooted daily. [Raised to 25 s on 19 Nov after step 72's anchors.]
  - 60 s before the reset.
  - 300 s for RSAM labels; 3 h for the 2–9 May RSAM rule.
- The analysis config (`20_analysis/analysis_workflow.json`) now reads the UTC MiniSEED and network history.
- **SSAM correction fixed.** SSAM and SSAM DATA ran on the MDETECT PC (WILLIE), not the RSAM PC. Their labels are 8 h behind RSAM until 13 Sep and match it afterwards (station-matched r = 0.72–0.99), so they are GMT throughout. The first UTC run had applied the BOB rule and put SSAM 8 h early for 11 Jun–13 Sep. SSAM now has its own `ssam` rule (0 h); in the UTC products SSAM matches RSAM at zero lag.
- Summary for the paper: `TIMING_AND_CLOCKS.md`.
- Clock drift within a day is not corrected. The IRIG traces are still removed in step 10; decoding them first would give a measured offset per file.

## 2. Waveforms (10, 11, 15, 16)

**Status:** all fixed. Items 1 and 5 by the UTC run (17 Nov); item 6 on 19 Nov (see the status list).

1. [Fixed: 3125/31 Hz kept, raw value in the index] **The header sample rate is overwritten.** Raw DMX = 100.80645 Hz (12500/124), verified on `9105010W.DMX`. `waveform.sampling_rate: 100.0` → FloVoPy `fix_sampling_rate` sets 100 Hz, which stretches each record by 0.8% (~0.3 s over a 4,096-sample record). The original rate isn't kept in the index, provenance or MiniSEED. Settle which rate is true and record the raw one.
2. [Fixed: dropped traces are provenance rows; see the status list] **Flat (constant) channels are dropped before provenance is built** (`trace_utils.remove_empty_traces`), so they appear nowhere. 98 of 143 sampled files lost at least one channel. This contradicts the "counted, not silently removed" text in nb 10 cell 13.
3. [Fixed: `10_file_log.csv`] **5 of 21,475 DMX files are unaccounted for**: 2 contain only flat or IRIG traces (`status=empty`, not saved), and 3 truncated files (9106133O, 9107077R, 9108071V) are missing from the failures CSV.
4. [Fixed: epochs rebuilt on UTC and checked against every archived trace] **StationXML epochs end before the data** for BUR, PIE, PPO, UBO and GRN (12–25 June; 336 station-channel-days). Separately, nb 15 cell 6 starts CAB EHN on 06-16, but its first data is 07-03.
5. [Fixed: the UTC run has `partial_recovery` columns] **Outputs are older than the config.** The index and failures CSVs were written 10 Nov, but `recover_partial: true` was added 11 Nov, so there are no `partial_recovery` columns.
6. [Fixed 19 Nov] Minor:
   - `pinatubo_steps/dmx_recovery.recover_file` is more permissive than FloVoPy's `recover_dmx_prefix`. [Removed with step 10's candidate-saving section; production recovery is FloVoPy's.]
   - nb 15 cell 30 depends on `notebook_source` from cell 27. [Step 70 now names the notebook directly.]
   - `PINATUBO_RESULTS_ROOT` redirects `OUTPUT_ROOT`. [By design: it selects the isolated test sandbox; see `RESULTS_LAYOUT.md`.]

## 3. Picks and association (20–35)

**Status (17 Nov, later):**
- **Fixed by moving all phase-card decoding into FloVoPy** (`flovopy.core.vdap`): items 1, 3 and 5 below, plus minute-60 cards and P seconds ≥ 60.
  - The new pieces are `parse_vdap_phase_line`, `read_vdap_pha_blocks`, `dedupe_picks` and `filter_picks_near_median`. `lib/pha_parser.py` is now a thin wrapper around them.
  - Step 21 event IDs now use the separator-block position in the source file. The old→new map is in `pipeline_tmp/21_event_id_changes_20261117.csv`.
  - Steps 20–53 have since been rerun on the UTC data.
- **Fixed since:** item 6 (114 minute-60 lines now rolled over); item 8's blank-component channel bug (FloVoPy maps components explicitly); `pha_parser` now has tests.
- **Fixed 18 Nov:** the stale note in `34_association_review.md` (see §6). Item 2 is fixed: blocks holding two events are split. Item 4 is fixed: an S earlier than P is skipped, not moved a minute later. Item 7 is fixed: whole blocks are attached, and duplicates are recorded instead of resurfacing.

1. **S-only lines get the wrong minute** (`pha_parser.py` ~208): `.strip()` runs before blank→0, so `'9108082304'` parses as 23:00:04 instead of 23:04:00 (verified with `strptime`). This affects 62 S picks and 8 fake P picks. Most are removed later as outliers; FNG in `pinaug91_block0862` survives at 21:00:36.42 (should be 21:07:36.42).
2. **Events in blocks missing a "10" separator are discarded.** The median-time filter drops the smaller event (13 clusters with ≥3 stations, 72 picks). `_split_into_time_clusters` exists but is never called.
3. **Step 21 under-reports trimming.** `parse_pha_file` already applies the ±60 s filter, so 181 picks are removed without being logged; only 21 are reported as trimmed.
4. **S-minute rollover goes the wrong way** when the S seconds are just below the P seconds (`pha_parser` 256–262 and FloVoPy `vdap.py` ~976). Event 91060310 gets S−P = 59.99 s, so the whole event fails waveform association. About 25 monthly S picks are lost the same way.
5. **Monthly S picks lose onset, polarity and weight** (13,010 picks), while the step-20 reader keeps them.
6. 114 lines with minute = 60 or seconds ≥ 60 are rejected rather than rolled over. This is already noted in 24.
7. **Step 32 creates artificial pick-only events:**
   - Duplicate picks removed inside a waveform aren't marked as assigned (50 picks).
   - Monthly events are attached pick by pick, so 98 events (336 picks) are split between a waveform event and a pick-only event.
8. Latent issues:
   - `'' in 'ZNE'` is True (`pha_parser` line 222), so a blank column 4 would give channel `EH`.
   - `pha_parser` has no tests.
   - `34_association_review.md` says step 32 is stale, but it has since been rerun. [Fixed 18 Nov: refreshed from the UTC run.]

## 4. Hypocenters and catalog export (40–54)

**Status:** items 1–6 fixed: UTC rerun; step 50 links through picks, then by record window, and uses step 43's preferred origin; Nordic seconds, magnitude type and RMS repaired in FloVoPy. Item 9 fixed: step 51 derives its windows from the data. Item 7 fixed: steps 40 and 42 treat magnitude 0.00 as missing. Item 8 fixed: step 54 writes the membership JSON once. Item 10 fixed (18 Nov): steps 43–53 were rerun.

1. Clock offset and ±10 s tolerance: see §1.
2. **Preferred origin**: nb 50 cell 8 uses `ev.origins[0]` and ignores step 43's `preferred_source`. In 607 of the 3,500 two-source groups this picks the PINAALL origin; 203 of those have different times.
3. **Nordic origin seconds are wrong**: ObsPy 1.5 `_write_header_line` writes `str(microsecond)[0:1]` (verified), so 58.08 s becomes "58.8". About 9% of origins are off by 0.09–0.81 s; the rest are truncated rather than rounded.
4. **Nordic magnitude type becomes "W"** when `magnitude_type` is None (`_evmagtonor`, verified).
5. **Nordic RMS is written as 0.0** for every event; the parsed HYPO71 RMS is never passed through.
6. **Products are out of date**: the step 50, 51 and 52 outputs predate the 11 Nov step-43 rerun. The QuakeML has 0 magnitudes, and the 51 CSV has no P_H column.
7. [Fixed] Magnitude 0.00 is a missing-value code (39% of summary rows and 27% of PINAALL rows). Step 43 handles it; the step-40/41 indexes and step 42 don't.
8. [Fixed] **Step 54 rewrites the membership JSON inside the per-event loop.** It is now written once after the loop; a failed export leaves `<prefix>_membership_partial.json` instead. The assoc rows of an event share one remark (was one identical remark per row). With a mock Datascope, the full UTC catalogue exports in about 1 s: 21,753 events, 8,233 origins, 56,655 arrivals, 37,943 assoc and 3,414 netmag rows; 17,063 unlocated events have `prefor = -1`. Native Antelope readback is still untested.
9. [Fixed] **Step 51 coverage windows are hard-coded** (8 May–9 Jun, 30 Jun–18 Aug) and exclude days that do have waveform data (10–14 Jun, 27–29 Jun, 19 Aug).
10. [Fixed] Minor issues:
    - **Step 53 class labels** kept the Nordic padding and the trailing line type "3" (columns such as `W_P   …   3` in `53_daily_counts.csv`). The label is now read from columns 1–79 only, and the W/P/H flags come from the class table rather than from letters in the label.
    - **`PICKS_PRESENT` between steps 50 and 52.** Step 50 counts a step-32 pick container as picks even when no pick could be rebuilt; step 52 used `bool(event.picks)`. Step 52 now copies step 50's three presence flags, so they always agree with EVENT_CLASS, and writes the picks actually exported as `N_PICKS`. On the UTC catalogue the two had agreed for every event.
    - **`nearest` reused origin IDs.** When one hypocentre links to several events, each event now gets its own copy of the origins, with IDs `origin/<id>/copy-for/<event>` and a `hypocenter_shared:` comment. The default policy is one-to-one and was not affected.
    - **Unstable sort in step 43.** The default quicksort could reorder origins with equal times (1,608 of the 8,233 UTC origins share a time with another). Step 43 now uses a stable sort: equal times keep input order (summary file first, then line order). The groups and event IDs are unchanged, but the preferred origin changes in 3 events where the summary file repeats a time with a different solution (events 1547, 3305 and 3306; in 1547 the depth goes from 2.84 to 4.49 km). The order of origins within 822 groups also changes.
    - Rerun 18 Nov (steps 43, 44, 50–53): class counts are unchanged (W_P_H 4,353, W_P 3,362, W_H 154, W_ONLY 13,590, P_H 121, P_ONLY 111, H_ONLY 62), steps 50 and 53 agree on every class, and no origin ID is repeated. Step 52 rewrote all 21,753 S-files (each gained `N_PICKS`) with `overwrite=True`; it took 33 min, because FloVoPy `export_catalog_to_rea` rewrote its whole manifest (8 MB) after every S-file. **Fixed in FloVoPy:** the manifest is now saved once at the end (also after an exception or interrupt) and checkpointed at most every 60 s, so a hard kill loses at most a minute of tracking and those S-files are left untracked, never overwritten. A full rewrite now takes 38 s; it gives the same 21,753 S-files (identical apart from the write-time stamp on the type-I line) and the same manifest entries. ObsPy's "Evaluation mode None is not mappable" warning is now issued once with a count (7,947 events have a pick with no evaluation mode, written blank).
11. Verified correct: HYPO71 column positions, N/E signs, km depth, minute/second rollover, CSS3 units and ID wiring.

## 5. BOB (60–69)

**Status:** items 1–7 fixed: all-zero SSAM rows are dropouts; RSAM/EVNT/TILT carry window-end labels; rain carried-forward values, tilt saturation, mudflow skipped files and auxiliary sources are handled; the BOB tests pass. Item 2's time bases are fixed for every product: RSAM/EVNT/SSAM first (SSAM had been put on the BOB clock by mistake and is now on the MDETECT PC clock), then tilt, rain, mudflow and counts (§1 status). Item 8 no longer applies: `legacy_bob` was retired on 18 Nov.

1. [Fixed] **All-zero SSAM rows count as data.** The SSAM files use 0 in all 16 bands for dropouts, but only −998/−999 are masked. That is about 34k SSAM rows plus 1.2k DATA rows (e.g. BUR1 1,400 of 1,667 rows, and DON1 has 8 entirely zero days counted as valid). Coverage, the 69 overview and the spectrograms are all inflated.
2. [Fixed] **Time bases**: see §1. nb 62 sets `TIME_ANCHOR='start'`, but RSAM is end-of-window.
3. [Fixed] **Rain values are carried forward** (`blank(chid)=data` in CONVERT.FOR), so "value present" overstates fresh readings. BOB_QPLO.TXT names the rain channel columns, but the notebook doesn't use them.
4. [Fixed] **Tilt rail value 8192 (2^13) is treated as data**: 46 UBOR and 18 UBOT samples from 12 Jun 23:00. Mudflow maxima of 8016 and 8010 suggest the same limit.
5. [Fixed] **MUDFLOW `.DA2`–`.DA5` files** (10–11 Aug) are skipped without notice because only `*.DAT` is read.
6. [Fixed] **TEST, BUG1 and TEMP auxiliary sources feed coverage**; today they add no new network days.
7. [Fixed] **The two failing tests are bugs in the test file**: notebook cell `source` is a list of lines, so `tests/test_bob_notebooks.py` lines 51 and 78 need `''.join(c['source'])` (verified).
8. [Moot: retired 18 Nov] Older `legacy_bob` modules differ slightly from the notebooks (−998 handling in counts, tilt labelled local, radio ditto column).
9. Verified correct:
   - annual float32 layout (52,560 and 52,704 samples, 576-byte header, little-endian)
   - EVNT values are per-interval, not cumulative
   - COSPEC fixed-width parsing
   - SSAM band edges

## 6. Documentation

**Status:** [Fixed 18 Nov].

- `NOTEBOOKS_README.md` described `12_*` and `13_inspect_partial_dmx_recovery.ipynb` and `network_history/finalize_inventory.py`, none of which are in the repo. The `10_conversion/network_history/` package was deleted in commit 4f58990.

**Fixed 18 Nov:**
- `NOTEBOOKS_README.md` is rewritten from the current repo: every step 10–69 with its purpose and inputs, the UTC clock rules, the settings that matter (step 10 recovery, step 40 parser, step 50 policies, step 52 collisions and manifest, BOB QC and clocks), how to run the tests, and what the leftover files are. Older text described a script-to-module refactor that was later undone, `one_to_one` as the step-50 default (it is `picks_then_window`), step-52 collision suffixes (replaced by filename shifts), and superseded folders that no longer exist.
- `34_association_review.md` is refreshed from the UTC outputs. The "stale outputs" finding is resolved. All 119 step-30 groups are enclosed, including `91060310`. Step 32 now has 7,715 WAV+PICKS, 13,744 WAV_ONLY and 232 PICKS_ONLY containers, and no source group is split. All 58,068 step-22 picks are accounted for (56,655 mapped, 1,413 suppressed duplicates). The channel screen and multi-group counts are recomputed, and the example times are now UTC.
- The top-level `README.md` described `pipeline/01–07` scripts, `metadata/` and `docs/` folders, a Dropbox data tree and "no FLOVOPY dependency", none of which is true now. Its structure, installation, pipeline, data-directory and testing sections are rewritten.
- `CONFIGURATION.md` gave the old output roots, "Notebook 05" (now step 15) and a 100 Hz rate, and said tilt, rain, mudflow and counts have no clock rule. All are corrected. Step 10's introduction no longer mentions a command-line conversion or a 100 Hz rate. Step 32's markdown example times are UTC.
- `tests/test_pipeline_configuration.py` expected the network history directly under the output root; it is under `FAIR/metadata/` (since commit 28f382f). The test is corrected, and all 9 configuration tests pass.
- Notebooks 20–23, 30–33, 35, 40 and 42 had saved outputs from the old `converted` root. They were rerun on the UTC root. Every CSV and log they write is byte-identical to before, so nothing downstream needed rerunning. Step 35 now selects 221 files; the saved notebook keeps 4 of its 221 inline figures, and all are in the review folder.

