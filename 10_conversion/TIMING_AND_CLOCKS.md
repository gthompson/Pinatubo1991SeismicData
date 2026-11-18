# Timing of the 1991 Pinatubo seismic data: problems, corrections and their effect

*17 November 2026. Details and scripts: `CODE_REVIEW_2026-11-17.md` ("Clock model", "UTC run"); rules: `clock_regimes.csv`; code: `pinatubo_steps/clock.py`, `flovopy.core.clock`.*

## 1. Summary

None of the 1991 recording systems had an external time reference (no GOES or other satellite clock). Each computer kept its own clock and drifted, and the clocks were set to different time zones at different times. As a result, the recovered datasets disagreed with one another by up to 15 h:
- waveforms;
- phase picks;
- hypocentres;
- RSAM, EVNT and SSAM, and the other products in the BOB archive.

The hour-scale errors are systematic and documented. We corrected them, and every time in the products is now UTC. Drift of a few seconds per day cannot be corrected.

After correction, the datasets agree with one another:
- **Hypocentres:** 95–99.6% of origins fall inside a waveform record, compared with 6–14% before.
- **Picks:** the number of picks with no waveform fell from 38,997 to 3,141.
- **Catalogue:** the number of catalogue events with both a waveform and a hypocentre rose from 110 to 2,818.
- **Hypocentre linking:** with a new step-50 rule that links hypocentres through their picks first, 4,628 of 4,690 hypocentres are linked to a waveform or pick event. 4,389 of those events have a waveform.

## 2. The clocks

| Clock | System | Products timed by it |
|---|---|---|
| **H**: DMX header time | MDETECT on WILLIE (IBM-AT), the seismic PC | SUDS/DMX waveforms; picks read from them (SUDSPICK) |
| **W**: seismic PC wall clock | WILLIE | HYPO71 hypocentres; later monthly picks; SSAM |
| **R**: RSAM labels | Tandy Model 100, the RSAM acquisition PC | RSAM, RSAM event counts (EVNT) |
| **L**: lahar labels | RICKY (IBM-XT), lahar acquisition | mudflow; rain (assumed, see section 3) |
| **T**: tilt labels | SLEEPY (IBM-XT), tilt acquisition; files rewritten to local time on 18 Nov 1991 | tilt (1–13 June) |
| **C**: hand-entered times | probably typed in on the BOB PC | hourly CAB counts |
| dates only | entered by hand | COSPEC, daily PIE counts, radio maps |
| other | helicorders | not verified (section 6) |

BOB is the archive format and the display software. It ran on the aggregation PC (PORKY, or DAVE/ANDY), which received the RSAM, tilt and lahar products from their own acquisition PCs through a buffered serial printsharer, and SSAM from WILLIE. No data were time-stamped with the BOB PC's clock (PVO system diagram, after Murray). The 17 Nov code review called the RSAM clock "B" (BOB labels); it is the Tandy Model 100's clock.

The MDETECT → SUDSPICK → HYPO71 chain all traces back to one clock, the seismic PC. Within a waveform file the relative timing is good to milliseconds. Picks and hypocentres inherit that PC's drift, so drift does not affect how they associate with each other. It affects only their absolute time.

## 3. Time-zone errors and their evidence

**MDETECT assumed the PC was in California.** It converted the PC clock to "GMT" by adding the US Pacific offset: the DOS default time zone `TZ=PST8PDT`, applied through `mktime()`. So:
- H = W + 7 h during daylight time;
- H = W + 8 h after daylight time ended at 02:00 PDT on 27 Oct 1991.

Evidence:
- **Filenames:** each DMX filename date equals the date of (H − 7 h), and of (H − 8 h) from 28 October.
- **Hypocentres:** they equal H − 7 h in every week from May to August.
- **Event counts:** the EVNT cross-correlation steps from −7 h to −8 h on the same date.
- **John Power:** "MDETECT … always assumed that the computer was in California and corrected the time to GMT" (email to R. White, 29 Oct 2026).

**The seismic PC was on Philippine time (UTC+8) until 26 May, then on GMT.** Harlow installed a new MDETECT version and set the clock to GMT "on or about May 25" (Power, email; see also *Fire and Mud*, p. 227, "local versus GMT"). The data place the reset at about 00:41–01:40 UTC on 26 May:
- **Waveform files:** `91052617` (H 15:41) is followed by `9105261G` (H 08:40), so the clock went back 8 h.
- **Hypocentre catalogue:** `PINAALL.DAT` jumps back at the same point, from line 217 (08:31) to line 218 (01:40). Line 218 plus 7 h is file `9105261G`.
- **DMX headers:** the SUDS trace-descriptor field `localtime` is 0 in every file up to `91052617`. From `9105261G` onward, through December, it is −25200 (−7 h, stored in seconds). This is the new MDETECT version.
- **RSAM comparison:** relative to the RSAM labels (on Philippine time), the waveforms step by exactly 8 h at the same time.

**Picks switched clocks in June.**
- The single-event PHA files (3–16 June) and the monthly picks up to 10 June are on H. All of them fall inside waveform records at zero offset.
- From 27 June, the monthly picks are on W. 88–100% fall inside waveform records once 7 h is added.
- The switch falls in a gap with no picks, between 10 and 27 June. That gap contains a new set of HYPO71 control cards.

**RSAM and EVNT (Tandy Model 100), from cross-correlating event counts with the waveform trigger rate:**
- 2–9 May: the labels equal H. This is unexplained.
- 9 May to about 13 September: Philippine time. This also matches the eruption onsets.
- From about 09:00 on 13 September (±3 h): GMT.
- 10 November: Philippine time again for one block of labels (01:20–22:20). See the RSAM–SSAM check below.

**SSAM was on the seismic PC's clock (WILLIE), not the RSAM PC's.** Comparing SSAM and SSAM DATA with RSAM, station by station (r = 0.72–0.99):
- the SSAM labels are 8 h behind RSAM until 13 September;
- they match RSAM afterwards.

So SSAM was on GMT from the start of its data (1 June), like the seismic PC after the reset.

**RSAM against summed SSAM, day by day (step 69).** Summing SSAM bands BN01–BN10 gives a 1-minute proxy for RSAM. Each day's RSAM is cross-correlated with its 10-minute mean, station by station, on the UTC times. This compares the RSAM PC (Tandy) with the MDETECT PC directly.
- Of 465 station-days (1 June – 22 November), 412 have r ≥ 0.5 (most r ≈ 0.9). All but six are within 4 min of zero.
- The lag moves in a sawtooth: it grows by about 1 min per day, then jumps back (e.g. 0 → 3 min over 1–7 July, 0 → 3 min over 7–10 October). That is drift between the two PCs, reset when one of them was restarted. It is not corrected.
- **10 November:** all five station pairs are exactly 8 h apart (r = 0.92–0.94), and the EVNT counts match the waveform triggers 8 h earlier (r = 0.59). So the RSAM PC ran on Philippine time that day. RSAM and SSAM both resume at about 17:10 UTC on 9 November after a gap, which suggests the RSAM PC was restarted on local time. A separate rule (`R_PHT_10Nov`) now covers it; with it, all five pairs are at 0 min on 10 November and 411 of the 412 reliable station-days are within 4 min.
- One station-day (FNG1, 19 October, r = 0.52) has a −58 min lag; no other station shows it.

**Rain, mudflow and the hourly CAB counts: clocks found from the data.** Mudflow was acquired on RICKY. Rain is not in the system diagram; it is assumed to share RICKY's clock because it matches the mudflow records at 0 h throughout and switches with them. The CAB counts were entered by hand. None of their headers can be trusted, so each was cross-correlated with RSAM or EVNT, which are already verified in UTC. RICKY, the Tandy and the CAB counts all switch from Philippine time to GMT on about 13 September. These are separate PCs, so this looks like an operational decision to change the time base, not one shared clock:
- **Mudflow:** amplitudes match RSAM at −8 h until 12 September (r = 0.4–0.7 for USAC, GUMA and MARL) and at 0 h from 15 September. Every file says "converted from GMT to local", even after the switch.
- **Rain:** increments match RSAM at −7/−8 h before 13 September and at 0 h after; they match the mudflow records at 0 h throughout.
- **Hourly CAB counts:** the header says "TIME IN GMT", but against EVNT they match at −8 h until 13 September and at 0 h afterwards. Each hh:30 label is the centre of its hour.
- Hours 00–07 in the rain and mudflow files carry the correct date. `CONVERT.FOR` subtracts 8 h without rolling the date back, but its output survives only in `MUDFLOW/PROGS/*.DA2`–`.DA5`, which are not read.
- From 15 November the rain and mudflow files have a new format and no header. There is no lahar signal to test them against, so their labels are kept.

**Tilt is on Philippine time.** Tilt was logged on SLEEPY, its own acquisition PC. The files were shifted by 48 samples (8 h) "from GMT to local" on 18 November 1991 (`TIME.TXT`). The PBAT battery voltage confirms it: solar charging starts at the 07:00 label and peaks between 11:00 and 14:00 every day.

**Dates only.** COSPEC values (header "TIME IN GMT", all at 00:00), the daily PIE counts and the radio maps were entered by hand and carry dates, not times, so the dates are kept. Each uses whatever day the person entering it chose.
- **COSPEC:** the measurements were made in daylight, so each date is taken as a Philippine day (`window_start` 16:00 UTC the day before, `window_end` 16:00 UTC). A GMT date would be the same for any measurement after 08:00 PHT.
- **Daily PIE counts:** whether these are Philippine or UTC days cannot be told: they correlate equally well with EVNT summed over either (r = 0.64 and 0.62, 30 April – 4 June).

## 4. Corrections applied (raw time + correction = UTC)

| Data | Clock | Before reset (26 May) | 26 May – 27 Oct | After 27 Oct | Nominal uncertainty |
|---|---|---|---|---|---|
| Waveforms (DMX header) | H | −15 h | −7 h | −8 h | 60 s before reset, 5 s after |
| Single-event PHA picks; monthly picks to 10 June | H | −15 h | −7 h | — | same |
| Monthly picks from 27 June | W | — | 0 | 0 | 5 s |
| Hypocentres (`PINAALL.DAT`, `Pinatubo_all.sum`) | W | −8 h | 0 | 0 | 60 s / 5 s |
| SSAM, SSAM DATA | W | (no data) | 0 | 0 | 60 s (1-min labels) |
| RSAM, EVNT | R | −15 h (2–9 May, low confidence); −8 h from 9 May | −8 h to 13 Sep, then 0 | 0, except −8 h for 10 Nov | 300 s (10-min labels) |
| Hourly CAB counts | C | (no data) | −8 h to 13 Sep, then 0 | 0 (to 6 Nov) | 1800 s |
| Rain, mudflow | L | (no data) | −8 h to 13 Sep, then 0 | 0 to 15 Nov; unverified after | 600 s |
| Tilt | T | (no data) | −8 h | (no data) | 1 h |

**Resolving the repeated wall-clock hours on 26 May.** Setting the clock back 8 h made the wall-clock times from about 00:41 to 08:41 on 26 May occur twice. Records in that window are resolved as follows:
- **Waveforms:** the `localtime` field and the file order must both agree. They do for all 21,459 files.
- **Picks and `PINAALL.DAT`:** by line order.
- **`Pinatubo_all.sum`:** this file is sorted by time, so its 18 records in the window were resolved by matching:
  - 13 to `PINAALL.DAT` records within 1 s and 0.05°;
  - 5 to the waveform file covering them.

Every product keeps the raw time in a `raw_*` column, with the rule applied in `clock_regime` and the uncertainty in `clock_uncertainty_s`. QuakeML origins carry the uncertainty as `time_errors`, plus a comment giving the rule and the raw time.

## 5. What cannot be corrected

- **Clock drift.** MDETECT reads the PC clock at start-up and then keeps time by counting A/D samples: "good to about 2 seconds per day" (Lee, Tottingham & Ellis 1988, OFR 88-751). The PCs were supposed to be reset every day, but this was done only sporadically. So absolute times carry errors of seconds that vary from day to day. Each PC drifted differently.
- **The IRIG trace.** The IRIG trace in the DMX files would normally be used to correct the headers, with `IRIG.EXE` from the PC-SUDS utilities. Without a satellite clock, though, the IRIG generator was free-running, so it cannot provide absolute UTC. The `timecorrect` field is 0 in every file, so no IRIG correction was ever applied. The generator's oscillator was probably more stable than the PC clock and was not rebooted daily. Decoding it could therefore still show PC clock resets and drift relative to one continuous timescale. Whether that is worth doing depends on whether the code is decodable and the generator was left running.
- **Absolute anchors.** A regional or teleseismic earthquake recorded in a triggered file, with an origin time in ISC or USGS catalogues, would give the absolute clock error on that day to about 1 s. A few such checks would bound the drift.

## 6. Not corrected or uncertain

- **Rain and mudflow from 15 November 1991:** labels kept and flagged `unverified` (new file format, nothing to test against). The GMT rule for 1–14 November is extrapolated from September–October.
- **Tilt:** the PHT rule rests on the solar day, so it is good to about an hour.
- **Helicorders:** a separate clock again.
- **RSAM 2–9 May:** the −15 h rule rests only on the cross-correlation and has low confidence.
- **RSAM switch times:** the switch on 9 May (~06:00) and to GMT on 13 September (~09:00) are each uncertain by about ±3 h.
- **Before the reset:** the clock was set by hand to local time, and some records (21–23 May) are annotated "+1 min". The uncertainty before the reset is therefore 60 s.
- **Out-of-sequence files:** a few waveform files (e.g. `91051014`–`17`) have header times hours earlier than their neighbours. They are flagged, not changed.

## 7. Effect on associations

All products were regenerated in UTC (`~/work/Pinatubo1991Processing/utc`). The earlier run that kept the recorded times is in `~/work/Pinatubo1991Processing/converted`.

**Fraction of records falling inside a waveform record** (origins: up to 10 s before the record start, through its end):

| Month (1991) | Picks, recorded | Picks, UTC | `PINAALL.DAT` origins, recorded | `PINAALL.DAT` origins, UTC | `Pinatubo_all.sum` origins, recorded | `Pinatubo_all.sum` origins, UTC |
|---|---|---|---|---|---|---|
| May | 100% | 100% | 6.0% | 99.4% | 5.7% | 95.7% |
| June | 76.8% | 99.4% | 9.1% | 99.6% | 9.3% | 99.6% |
| July | 13.8% | 94.7% | 13.7% | 95.8% | 14.3% | 95.4% |
| August | 10.4% | 97.8% | 11.2% | 97.6% | 11.4% | 97.9% |

**Step 32 (waveform files and picks):**

| | Recorded | UTC |
|---|---|---|
| Events with waveform and picks | 2,928 | 7,712 |
| Waveform-only events | 18,521 | 13,747 |
| Pick-only events | 5,339 | 482 |
| Picks with no waveform | 38,997 | 3,141 |

**Step 50 catalogue (W = waveform, P = picks, H = hypocentre).** The recorded-time catalogue was built before the 17 Nov phase-card parser fix, which added about 225 picks; the effect on these counts is small.

The two UTC columns differ only in how step 50 links hypocentres to events:
- **"±10 s"** is the original rule: the origin must be within 10 s of the record start.
- **"picks + window"** is the new default:
  1. A hypocentre links to the event whose first P pick comes 0–10 s after the origin.
  2. Otherwise it links to the record whose window contains the origin, from 10 s before the record start to the record end.

| Event class | Recorded | UTC, ±10 s | UTC, picks + window |
|---|---|---|---|
| W + P + H | 94 | 2,735 | 4,352 |
| W + H | 16 | 83 | 155 |
| P + H | 0 | 258 | 121 |
| W + P | 2,831 | 4,977 | 3,355 |
| W only | 16,844 | 13,664 | 13,597 |
| P only | 5,325 | 224 | 111 |
| H only | 1,500 | 1,614 | 62 |
| **Total events** | 26,610 | 23,555 | 21,753 |

The last column also includes two later fixes. Step 32 attaches whole monthly blocks and suppresses 1,403 duplicate picks: repeated copies of the same event in the phase files, which previously reappeared as pick-only events. Pick-only events fell from 481 to 232, and picks with no waveform from 3,130 to 1,733. Separately, an S reading earlier than its P is now skipped.

**Why the original rule fell short:** records start about 11 s before the first P arrival, so origins fall a median of 9.5 s *after* the start, and about 40% lay outside ±10 s.

**Under the new rule:**
- **Through picks (4,446 links):** the first P arrives a median 2.3 s after the origin (99th percentile 4.8 s).
- **By record window (182 links):** the origin falls a median 10.3 s after the record start.
- **Against the old rule:** 3,046 of the old rule's 3,076 links are unchanged. Most of the 27 that differ now attach to the record that holds the hypocentre's picks.
- **Still unlinked (62 hypocentres):** 46 of them fall inside a record already linked to another hypocentre, typically two earthquakes in one file. These keep a comment naming that record. The rest have no record or picks nearby.

**RSAM, EVNT and SSAM versus waveforms:**
- In the UTC products, EVNT matches the waveform trigger rate at zero lag in every window from May to December.
- SSAM matches RSAM at zero lag (r = 0.71–0.92).
- Before correction, the offsets ranged from −7 h to +1 h, and SSAM–RSAM was off by 8 h.

## Sources

- J. Power, email to R. White, 29 Oct 2026, forwarded to G. Thompson, 2 Nov 2026.
- Newhall, C.G. & Punongbayan, R.S. (eds.), 1996, *Fire and Mud*, p. 227.
- Lee, W.H.K., Tottingham, D.M. & Ellis, J.O., 1988, USGS Open-File Report 88-751 (MDETECT / PC-Quake).
- Tottingham, D.M. & Lee, W.H.K., 1989, USGS OFR 89-205 (XDETECT).
- Ward, P.L., 1989, USGS OFR 89-188 (SUDS).
