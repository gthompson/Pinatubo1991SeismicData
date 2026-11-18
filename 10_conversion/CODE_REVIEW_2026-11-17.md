# 10_conversion code review — 17 Nov 2026

Read-only review of notebooks 10–69, `pinatubo_steps/`, `legacy_bob/` and the relevant FloVoPy readers, checked against the legacy archive and the outputs in `~/work/Pinatubo1991Processing/converted`. Nothing in the repo or data was changed. "Verified" means reproduced independently during the review, not only reported by one reviewer.

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
- **B**: the BOB labels.

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

**BOB labels:**
- PHT from 10 May to about 13 September. This matches the RSAM eruption-onset check.
- Equal to H − 7 h (= UTC) from about 14 September, and H − 8 h (= UTC) after 28 October, so BOB switched to GMT around 14 September.
- 2–10 May: B = H, not yet explained. The seismic clocks are consistent there (hypocenters = H − 7 h), so this is on the BOB side.
- Tilt PPO `TIME.TXT`: "phased over 48 … from GMT to local, 18 Nov 91".

**Other findings:**
- Some files are out of sequence (`91051014`–`17`, `9105161 0`–`14`). Their H is hours earlier than their neighbours, with no lasting clock change.
- The annotated "LOCAL_PLUS_1MIN" records (21–23 May) show no 60 s step relative to the waveforms. If the PC clock was 1 min off, it affected all seismic streams equally.

Consequences:
- Step 50 matches waveform events to hypocenters with ±10 s and no clock offset, so almost no waveform–hypocenter pairs are found before ~21 May. After a 7 h correction, the ~9 s pretrigger still leaves only ~2,900 of ~4,500 pairs within ±10 s.
- The annotated PINAALL parser's local→UTC switch (`LOCAL_UTC_OFFSET=8`) would move pre-30-May origins to 15 h from the waveforms.
- The committed `pinatubo_hypocenters.csv` contains −8 h times, but `pinatubo_parse_summary.txt` says offset None, so the two files come from different runs.
- Notebook 69 overlays series that are on different time bases.

### UTC run (17 Nov)

**Implementation.**
- Rules: `clock_regimes.csv`, applied through `pinatubo_steps/clock.py` and FloVoPy `flovopy.core.clock` (`ClockRegimes`, `dos_mktime_wall_clock`).
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
- Tilt, rain, mudflow, COSPEC, counts and radio labels are kept as recorded and flagged `unverified`. No clock has been established for these products; the tilt note says it switched from GMT to local on 18 Nov 91.
- BOB 2–9 May (`B_ahead`, −15 h) is based only on the cross-correlation and has low confidence.
- The exact BOB switches are uncertain by about ±3 h: 9 May ~06:00 and 13 Sep ~09:00 (label time).

**Step 54** (CSS3) needs an Antelope Python environment and was not run.

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
  - 5 s after the reset: MDETECT keeps time by counting samples, about 2 s/day, and the PC was rebooted daily.
  - 60 s before the reset.
  - 300 s for BOB labels; 3 h for the 2–9 May BOB rule.
- The analysis config (`20_analysis/analysis_workflow.json`) now reads the UTC MiniSEED and network history.
- **SSAM correction fixed.** SSAM and SSAM DATA ran on the MDETECT PC, not the BOB PC. Their labels are 8 h behind RSAM until 13 Sep and match it afterwards (station-matched r = 0.72–0.99), so they are GMT throughout. The first UTC run had applied the BOB rule and put SSAM 8 h early for 11 Jun–13 Sep. SSAM now has its own `ssam` rule (0 h); in the UTC products SSAM matches RSAM at zero lag.
- Summary for the paper: `TIMING_AND_CLOCKS.md`.
- Clock drift within a day is not corrected. The IRIG traces are still removed in step 10; decoding them first would give a measured offset per file.

## 2. Waveforms (10, 11, 15, 16)

1. **The header sample rate is overwritten.** Raw DMX = 100.80645 Hz (12500/124), verified on `9105010W.DMX`. `waveform.sampling_rate: 100.0` → FloVoPy `fix_sampling_rate` sets 100 Hz, which stretches each record by 0.8% (~0.3 s over a 4,096-sample record). The original rate isn't kept in the index, provenance or MiniSEED. Settle which rate is true and record the raw one.
2. **Flat (constant) channels are dropped before provenance is built** (`trace_utils.remove_empty_traces`), so they appear nowhere. 98 of 143 sampled files lost at least one channel. This contradicts the "counted, not silently removed" text in nb 10 cell 13.
3. **5 of 21,475 DMX files are unaccounted for**: 2 contain only flat or IRIG traces (`status=empty`, not saved), and 3 truncated files (9106133O, 9107077R, 9108071V) are missing from the failures CSV.
4. **StationXML epochs end before the data** for BUR, PIE, PPO, UBO and GRN (12–25 June; 336 station-channel-days). Separately, nb 15 cell 6 starts CAB EHN on 06-16, but its first data is 07-03.
5. **Outputs are older than the config.** The index and failures CSVs were written 10 Nov, but `recover_partial: true` was added 11 Nov, so there are no `partial_recovery` columns.
6. Minor:
   - `pinatubo_steps/dmx_recovery.recover_file` is more permissive than FloVoPy's `recover_dmx_prefix`.
   - nb 15 cell 30 depends on `notebook_source` from cell 27.
   - `PINATUBO_RESULTS_ROOT` redirects `OUTPUT_ROOT`.

## 3. Picks and association (20–35)

**Status (17 Nov, later):**
- **Fixed by moving all phase-card decoding into FloVoPy** (`flovopy.core.vdap`): items 1, 3 and 5 below, plus minute-60 cards and P seconds ≥ 60.
  - The new pieces are `parse_vdap_phase_line`, `read_vdap_pha_blocks`, `dedupe_picks` and `filter_picks_near_median`. `pinatubo_steps/pha_parser.py` is now a thin wrapper around them.
  - Step 21 event IDs now use the separator-block position in the source file. The old→new map is in `pipeline_tmp/21_event_id_changes_20261117.csv`.
  - Steps 21 and 22 were rerun; steps 23 onward are stale.
- **Still open**: items 2, 4, 6–9.

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
   - `34_association_review.md` says step 32 is stale, but it has since been rerun.

## 4. Hypocenters and catalog export (40–54)

1. Clock offset and ±10 s tolerance: see §1.
2. **Preferred origin**: nb 50 cell 8 uses `ev.origins[0]` and ignores step 43's `preferred_source`. In 607 of the 3,500 two-source groups this picks the PINAALL origin; 203 of those have different times.
3. **Nordic origin seconds are wrong**: ObsPy 1.5 `_write_header_line` writes `str(microsecond)[0:1]` (verified), so 58.08 s becomes "58.8". About 9% of origins are off by 0.09–0.81 s; the rest are truncated rather than rounded.
4. **Nordic magnitude type becomes "W"** when `magnitude_type` is None (`_evmagtonor`, verified).
5. **Nordic RMS is written as 0.0** for every event; the parsed HYPO71 RMS is never passed through.
6. **Products are out of date**: the step 50, 51 and 52 outputs predate the 11 Nov step-43 rerun. The QuakeML has 0 magnitudes, and the 51 CSV has no P_H column.
7. Magnitude 0.00 is a missing-value code (39% of summary rows and 27% of PINAALL rows). Step 43 handles it; the step-40/41 indexes and step 42 don't.
8. **Step 54 rewrites the membership JSON inside the per-event loop.**
9. **Step 51 coverage windows are hard-coded** (8 May–9 Jun, 30 Jun–18 Aug) and exclude days that do have waveform data (10–14 Jun, 27–29 Jun, 19 Aug).
10. Minor issues:
    - The step 53 class labels keep the Nordic padding and trailing "3".
    - `PICKS_PRESENT` can disagree between steps 50 and 52.
    - The `nearest` policy can reuse origin IDs.
    - The unstable sort in step 43 can change event IDs between runs.
11. Verified correct: HYPO71 column positions, N/E signs, km depth, minute/second rollover, CSS3 units and ID wiring.

## 5. BOB (60–69)

1. **All-zero SSAM rows count as data.** The SSAM files use 0 in all 16 bands for dropouts, but only −998/−999 are masked. That is about 34k SSAM rows plus 1.2k DATA rows (e.g. BUR1 1,400 of 1,667 rows, and DON1 has 8 entirely zero days counted as valid). Coverage, the 69 overview and the spectrograms are all inflated.
2. **Time bases**: see §1. nb 62 sets `TIME_ANCHOR='start'`, but RSAM is end-of-window.
3. **Rain values are carried forward** (`blank(chid)=data` in CONVERT.FOR), so "value present" overstates fresh readings. BOB_QPLO.TXT names the rain channel columns, but the notebook doesn't use them.
4. **Tilt rail value 8192 (2^13) is treated as data**: 46 UBOR and 18 UBOT samples from 12 Jun 23:00. Mudflow maxima of 8016 and 8010 suggest the same limit.
5. **MUDFLOW `.DA2`–`.DA5` files** (10–11 Aug) are skipped without notice because only `*.DAT` is read.
6. **TEST, BUG1 and TEMP auxiliary sources feed coverage**; today they add no new network days.
7. **The two failing tests are bugs in the test file**: notebook cell `source` is a list of lines, so `tests/test_bob_notebooks.py` lines 51 and 78 need `''.join(c['source'])` (verified).
8. Older `legacy_bob` modules differ slightly from the notebooks (−998 handling in counts, tilt labelled local, radio ditto column).
9. Verified correct:
   - annual float32 layout (52,560 and 52,704 samples, 576-byte header, little-endian)
   - EVNT values are per-interval, not cumulative
   - COSPEC fixed-width parsing
   - SSAM band edges

## 6. Documentation

- `NOTEBOOKS_README.md` describes `12_*` and `13_inspect_partial_dmx_recovery.ipynb` and `network_history/finalize_inventory.py`, none of which are in the repo. The `10_conversion/network_history/` package was deleted in commit 4f58990.
