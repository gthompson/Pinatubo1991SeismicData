# What lives in FloVoPy and what stays here

*Updated 19 November 2026. Replaces the note of 3 November.*

Code that would serve any VDAP-era archive lives in FloVoPy. Anything that encodes a Pinatubo decision (paths, clock rules, station mappings, association and export policies) stays in the notebooks and in `lib/`.

## In FloVoPy

| Module | Used by | What it does |
|---|---|---|
| `flovopy.core.vdap` | 10, 20, 21, 40 | DMX reading and conversion to a SEISAN WAV database (`read_dmx_file`, `convert_dmx_to_seisan`, with a `time_correction=` hook); phase-card decoding (`parse_vdap_phase_line`, `read_vdap_pha_file`, `read_vdap_pha_blocks`); pick clean-up (`dedupe_picks`, `filter_picks_near_median`, `split_picks_by_time_gap`); HYPO71 summary lines (`parse_hypo71_line`) |
| `flovopy.core.dmx_recovery` | 10 (through `vdap`) | Keeps the complete traces before the first defect of a damaged DMX file |
| `flovopy.core.clock` | `lib/clock.py` | The clock-rule engine: `ClockRegimes` reads a rule table and applies it with sequence and marker checks; `dos_mktime_wall_clock` |
| `flovopy.core.event_utils` | 50–52, `lib/shared.py` | Time association (`associate_events_by_time`), SEED-ID splitting, ObsPy comment lookup |
| `flovopy.seisanio.rea_export` | 52 | Catalogue to SEISAN REA, with a manifest, collision policy and fixes to ObsPy's Nordic header writer |
| `flovopy.legacy.bob` | 61–69 | BOB archive readers (RSAM, EVNT, SSAM, tilt, tabular products) and SSAM spectrograms |
| `flovopy.processing.spectrograms`, `flovopy.enhanced` | `20_analysis` | DPSS and Hann spectra, event-metric sidecars, CSV and Parquet exports |

## Kept here (`lib/` and the notebooks)

- `lib/config.py`: paths from `conversion_config.json`.
- `lib/clock.py` and `clock_regimes.csv`: which stream used which PC clock, and when it changed. The rules are Pinatubo history; the engine is FloVoPy's.
- `lib/pha_parser.py`: the Pinatubo policy for monthly PHA files (network XB, ±60 s median trimming, splitting blocks that lack a separator, logging every trimmed pick) on top of FloVoPy's readers.
- `lib/pha_comparison.py`: step 23's comparison of the single-event and monthly pick sources.
- `lib/shared.py`: table and plot helpers, the XB fallback ID, the HYPO71 rule that magnitude 0.00 means none (`mark_missing_magnitudes`), and step 50's hypocentre linking (`associate_hypocenters`: through picks, then by record window).
- In the notebooks: original and annotated PINAALL parsing, source priority, event classes, reporting windows, and the network-history reconstruction (step 70).

## Candidates for FloVoPy

These are general enough to move if another archive needs them:
- `parse_pha_file` in `lib/pha_parser.py`, as a monthly VDAP PHA reader;
- `associate_hypocenters` in `lib/shared.py`, for any triggered network;
- `mark_missing_magnitudes`, the HYPO71 convention;
- `compare_pick_sources` in `lib/pha_comparison.py`, as a generic two-catalogue pick comparison.
