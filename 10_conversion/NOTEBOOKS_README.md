# Pinatubo conversion notebooks

The numbered notebooks in this folder turn the surviving 1991 PVO archive (DMX waveforms, PHA phase files, HYPO71/PINAALL hypocentres and BOB monitoring products) into a FAIR archive: MiniSEED in a SEISAN WAV database, StationXML, an ObsPy/QuakeML catalogue, a SEISAN REA database, an Antelope CSS3.0 export and tabular BOB products. All times are converted to UTC where they are read.

## Running

- Use a kernel with the current FloVoPy checkout installed in editable mode, plus ObsPy, pandas, NumPy, Matplotlib and IPython (the `flovopy_asl` conda environment). Restart the kernel after changing FloVoPy.
- Open Jupyter from this folder or the project root. Every notebook reads `conversion_config.json` through `lib.config.pipeline_paths`: inputs under `data_top`, outputs under `output_root` (currently `~/work/Pinatubo1991Processing/utc`). See [../CONFIGURATION.md](../CONFIGURATION.md).
- Each notebook has a configuration cell (`SETTINGS`), a definitions cell tagged `pipeline_definitions` (functions only; tests load this cell), and a run cell. Run them top to bottom, in step order. Saved outputs in the committed notebooks come from the UTC run.
- Code generic to VDAP data lives in FloVoPy; Pinatubo choices stay in the notebooks and in `lib/` (see [FloVoPy and `lib/`](#flovopy-and-lib)).

## Clocks and UTC

Each data stream was time-stamped by its own acquisition PC, and those clocks changed between Philippine time and GMT during 1991. `clock_regimes.csv` gives the rule for every stream and period; `lib/clock.py` applies it. Raw times are kept in `raw_*` columns and the rule and nominal uncertainty in `clock_*` columns. Records that cannot be placed go to `*_clock_unresolved.csv` (or, for waveforms, the failures table). The evidence is in [TIMING_AND_CLOCKS.md](TIMING_AND_CLOCKS.md).

## Steps

| Step | Notebook | Purpose | Needs |
|---|---|---|---|
| 10 | `10_dmx_to_seisanWAV` | DMX → MiniSEED in the SEISAN WAV database; UTC start times; waveform index, per-file log and failures table; optional partial-file recovery | raw DMX |
| 11 | `11_waveform_timeseries_diagnostics` | Daily file counts and station availability | 10 |
| 20 | `20_parse_individual_phase_files` | Individual-event PHA files → pick index (primary picks) | raw PHA |
| 21 | `21_parse_monthly_phase_files` | Monthly PHA files → pick index (secondary picks, one block per event) | raw PHA |
| 22 | `22_merge_picks` | Merge 20 and 21; suppress duplicate picks | 20, 21 |
| 23 | `23_plot_pick_event_diagnostics` | Pick and source-group diagnostics; step-20/21 cross-source comparison | 20–22 |
| 30 | `30_associate_individual_picks_with_waveforms` | Match each individual-file group to the one waveform that encloses it | 10, 22 |
| 32 | `32_build_waveform_pick_events` | Waveform and pick-only containers (WAV+PICKS, WAV_ONLY, PICKS_ONLY) | 10, 22, 30 |
| 33 | `33_plot_event_association_diagnostics` | Daily association bar charts | 32 |
| 34 | `34_association_review.md` | Written review of steps 30–33 | — |
| 35 | `35_review_multiple_pick_groups` | Waveform plots of files holding several pick groups | 32 |
| 40 | `40_build_hypocenter_indexes` | Hypocentre indexes from `Pinatubo_all.sum` (`40_*`), PINAALL (`41_*`, original or annotated parser) and the later solutions GOLD, BRUN and PEPI (`40_gold_*`, `40_brun_*`, `40_pepi_*`) | raw hypocentre files |
| 42 | `42_compare_hypocenter_indexes` | Exact comparison of the two indexes | 40 |
| 43 | `43_associate_hypocenters` | Group origins from the five indexes; choose a preferred origin (PEPI > BRUN > GOLD > `Pinatubo_all.sum` > PINAALL) and a preferred HYPO71 magnitude | 40 |
| 44 | `44_plot_hypocenters_per_day` | Daily counts of the two hypocentre sources | 40 |
| 47 | `47_hypocenter_source_comparison` | Inventory of the five sources, clock-rule check against HYPO71, and differences between sources in the same event | 40, 43 |
| 50 | `50_build_obspy_catalog` | Canonical ObsPy catalogue (pickle and QuakeML) with seven W/P/H composition classes | 22, 32, 43 |
| 51 | `51_event_numbers_in_complete_windows` | Daily composition counts in reporting windows | 50 |
| 60 | `60_prepare_bob_archives` | Unpack BOB ZIP archives (preview by default; the only notebook that changes LEGACY) | raw BOB |
| 61–68 | `61_rsam_and_events` … `68_radio_maps` | One BOB product each: RSAM/EVNT, SSAM, tilt, rain, mudflow, COSPEC, counts, radio maps | 60 |
| 69 | `69_bob_coverage_and_review` | Network-wide BOB availability overview | 61–68 |
| 70 | `70_network_history_and_stationxml` | Network history, channel epochs on UTC, StationXML and audits; run last, once every dataset is on UTC | 10 (and 60–69 done) |
| 71 | `71_validate_network_history` | Checks on the step-70 products (every trace inside an epoch) | 70 |
| 72 | `72_external_clock_anchors` | Absolute clock check: ISC/USGS events against HYPO71, long S−P pick groups, SSAM and triggered records; analyst review of candidate records | 10, 22, 43, 62; network for the first download |
| 80 | `80_build_seisan_rea_catalog` | SEISAN REA S-files (exports run last) | 50 |
| 81 | `81_seisan_rea_diagnostics` | Audit of the written REA database | 80 |
| 82 | `82_export_antelope_css3` | Antelope CSS3.0 catalogue and wfdisc, with site, sitechan and calib from the step-70 StationXML | 50, 70, 80 |

Notes on specific steps:

- **10.** `waveform.recover_partial` is `true` in the configuration: a damaged DMX file keeps the complete traces before its first defect, and the waveform index records `partial_recovery` and `recovery_details`. Keep those columns in any downstream provenance. An optional section plots the raw recovered traces with FloVoPy `read_dmx_file`. See FloVoPy `docs/dmx_recovery.md`.
- **40.** Set `PARSER="original"` (default) or `"annotated"` in the PINAALL section. Annotated outputs have their own names and must be selected explicitly downstream.
- **43.** `origin_priority` and `preferred_magnitude_sources` set the preferred origin and magnitude. Repeats of an earthquake within one file (about 200 in `b-run.sum`) stay in one group (`one_origin_per_source=False`). GOLD, BRUN and PEPI magnitudes are on other scales and are never preferred.
- **50.** `association_policy` is `picks_then_window` (default), `one_to_one` or `nearest`. A hypocentre linked to several events under `nearest` gets a per-event copy of its origin with a unique ID.
- **80.** `collision="shift"` moves only the S-file *name* by up to 30 s when two events share a second; event and pick times are unchanged. A manifest (`.flovopy_rea_manifest.json`) makes reruns reuse unchanged files; `overwrite=True` is needed after the catalogue changes. Untracked files are never overwritten.
- **60–69.** Each product writes QC-schema-2 outputs (`coverage.csv`, `daily_coverage.csv`, `date_review.csv`, `review.json`, `export_status.json` and others) under `FAIR/BOB/<product>`. `OVERWRITE=False` protects existing products. Step 69 refuses incomplete or mixed products unless `ALLOW_PARTIAL=True`. RSAM/EVNT, SSAM, tilt, rain, mudflow and hourly CAB counts are converted to UTC with their acquisition PC's rule in `clock_regimes.csv`. COSPEC dates are taken as Philippine days; daily PIE counts and radio-map dates are kept as written. Rain and mudflow from 15 November, and CAB labels outside the rules, are flagged `unverified`.

## FloVoPy and `lib/`

Code that would serve any VDAP-era archive lives in FloVoPy. Anything that encodes a Pinatubo decision (paths, clock rules, station mappings, association and export policies) stays in the notebooks and in `lib/`.

### In FloVoPy

| Module | Used by | What it does |
|---|---|---|
| `flovopy.core.vdap` | 10, 20, 21, 40 | DMX reading and conversion to a SEISAN WAV database (`read_dmx_file`, `convert_dmx_to_seisan`, with a `time_correction=` hook); phase-card decoding (`parse_vdap_phase_line`, `read_vdap_pha_file`, `read_vdap_pha_blocks`); pick clean-up (`dedupe_picks`, `filter_picks_near_median`, `split_picks_by_time_gap`); HYPO71 summary lines (`parse_hypo71_line`) |
| `flovopy.core.dmx_recovery` | 10 (through `vdap`) | Keeps the complete traces before the first defect of a damaged DMX file |
| `flovopy.core.clock` | `lib/clock.py` | The clock-rule engine: `ClockRegimes` reads a rule table and applies it with sequence and marker checks; `dos_mktime_wall_clock` |
| `flovopy.core.event_utils` | 50, 51, 80, `lib/shared.py` | Time association (`associate_events_by_time`), SEED-ID splitting, ObsPy comment lookup |
| `flovopy.seisanio.rea_export` | 80 | Catalogue to SEISAN REA, with a manifest, collision policy and fixes to ObsPy's Nordic header writer |
| `flovopy.legacy.bob` | 61–69 | BOB archive readers (RSAM, EVNT, SSAM, tilt, tabular products) and SSAM spectrograms |
| `flovopy.processing.spectrograms`, `flovopy.enhanced` | `20_analysis` | DPSS and Hann spectra, event-metric sidecars, CSV and Parquet exports |

### Kept here (`lib/` and the notebooks)

- `lib/config.py`: paths from `conversion_config.json`.
- `lib/clock.py` and `clock_regimes.csv`: which stream used which PC clock, and when it changed. The rules are Pinatubo history; the engine is FloVoPy's.
- `lib/pha_parser.py`: the Pinatubo policy for monthly PHA files (network XB, ±60 s median trimming, splitting blocks that lack a separator, logging every trimmed pick) on top of FloVoPy's readers.
- `lib/pha_comparison.py`: step 23's comparison of the single-event and monthly pick sources.
- `lib/shared.py`: table and plot helpers, the XB fallback ID, the HYPO71 rule that magnitude 0.00 means none (`mark_missing_magnitudes`), and step 50's hypocentre linking (`associate_hypocenters`: through picks, then by record window).
- In the notebooks: original and annotated PINAALL parsing, source priority, event classes, reporting windows, and the network-history reconstruction (step 70).

### Candidates for FloVoPy

These are general enough to move if another archive needs them:
- `parse_pha_file` in `lib/pha_parser.py`, as a monthly VDAP PHA reader;
- `associate_hypocenters` in `lib/shared.py`, for any triggered network;
- `mark_missing_magnitudes`, the HYPO71 convention;
- `compare_pick_sources` in `lib/pha_comparison.py`, as a generic two-catalogue pick comparison.

## Tests

From this folder:

```bash
python -m pytest -q lib/tests tests
```

`lib/tests` covers the clock rules, PHA parsing and comparison, shared helpers and steps 32, 43, 50, 51 and 80–82 (the notebook functions are loaded from their `pipeline_definitions` cells). `tests/` covers the BOB notebooks and products, and runs step 71's controlled network-reconstruction checks. FloVoPy has its own suite (`python -m pytest -q flovopy/tests` in the FloVoPy checkout). The configuration checks are run from the project root (see [../CONFIGURATION.md](../CONFIGURATION.md)).

## Reviews and reference documents

- [CODE_REVIEW_2026-11-17.md](CODE_REVIEW_2026-11-17.md): full review of steps 10–69, with status and remaining items.
- [TIMING_AND_CLOCKS.md](TIMING_AND_CLOCKS.md) and [clock_regimes.csv](clock_regimes.csv): clock model and UTC rules.
- `34_association_review.md`: written review of steps 30–33 on the UTC run. The earlier reviews of steps 20–23 and 40–44 described the pre-UTC run and are in `superseded/reviews/`; their findings are in the code review (§3, §4).

## Other files

- `lib/`: Pinatubo helpers used by the notebooks; see [FloVoPy and `lib/`](#flovopy-and-lib).
- `hypo71_settings_199106.txt`: the HYPO71 control cards used in June 1991 (converted from an RTF copy): test parameters, coordinates of the 16 station-components, a five-layer P-velocity model (3.3–8.0 km/s, interfaces at 1, 4, 15 and 25 km) and Vp/Vs 1.78. Reference only; no step reads it.
- `superseded/`: retired code kept for the record (see its README): the pre-FloVoPy BOB notebooks, the `legacy_bob` converter, early SSAM and mudflow audits, a command-line wrapper for step 60 and an old refactor manifest.
