# Pinatubo conversion notebooks

The numbered notebooks in this folder turn the surviving 1991 PVO archive (DMX waveforms, PHA phase files, HYPO71/PINAALL hypocentres and BOB monitoring products) into a FAIR archive: MiniSEED in a SEISAN WAV database, StationXML, an ObsPy/QuakeML catalogue, a SEISAN REA database, an Antelope CSS3.0 export and tabular BOB products. All times are converted to UTC where they are read.

## Running

- Use a kernel with the current FloVoPy checkout installed in editable mode, plus ObsPy, pandas, NumPy, Matplotlib and IPython (the `flovopy_asl` conda environment). Restart the kernel after changing FloVoPy.
- Open Jupyter from this folder or the project root. Every notebook reads `conversion_config.json` through `pinatubo_steps.config.pipeline_paths`: inputs under `data_top`, outputs under `output_root` (currently `~/work/Pinatubo1991Processing/utc`). See [../CONFIGURATION.md](../CONFIGURATION.md).
- Each notebook has a configuration cell (`SETTINGS`), a definitions cell tagged `pipeline_definitions` (functions only; tests load this cell), and a run cell. Run them top to bottom, in step order. Saved outputs in the committed notebooks come from the UTC run.
- Code generic to VDAP data lives in FloVoPy (`flovopy.core.vdap`, `flovopy.core.dmx_recovery`, `flovopy.seisanio.rea_export`, `flovopy.legacy.bob`). Pinatubo choices (paths, clock rules, station mappings, association and export policies) stay in the notebooks and in `pinatubo_steps/`.

## Clocks and UTC

Each data stream was time-stamped by its own acquisition PC, and those clocks changed between Philippine time and GMT during 1991. `clock_regimes.csv` gives the rule for every stream and period; `pinatubo_steps/clock.py` applies it. Raw times are kept in `raw_*` columns and the rule and nominal uncertainty in `clock_*` columns. Records that cannot be placed go to `*_clock_unresolved.csv` (or, for waveforms, the failures table). The evidence is in [TIMING_AND_CLOCKS.md](TIMING_AND_CLOCKS.md).

## Steps

| Step | Notebook | Purpose | Needs |
|---|---|---|---|
| 10 | `10_dmx_to_seisanWAV` | DMX → MiniSEED in the SEISAN WAV database; UTC start times; waveform index, per-file log and failures table; optional partial-file recovery | raw DMX |
| 11 | `11_waveform_timeseries_diagnostics` | Daily file counts and station availability | 10 |
| 15 | `15_network_history_and_stationxml` | Network history, channel epochs on UTC, StationXML and audits | 10 |
| 16 | `16_validate_network_history` | Checks on the step-15 products (every trace inside an epoch) | 15 |
| 20 | `20_parse_individual_phase_files` | Individual-event PHA files → pick index (primary picks) | raw PHA |
| 21 | `21_parse_monthly_phase_files` | Monthly PHA files → pick index (secondary picks, one block per event) | raw PHA |
| 22 | `22_merge_picks` | Merge 20 and 21; suppress duplicate picks | 20, 21 |
| 23 | `23_plot_pick_event_diagnostics` | Pick and source-group diagnostics; step-20/21 cross-source comparison | 20–22 |
| 24 | `24_phase_pick_review.md` | Written review of the phase files | — |
| 30 | `30_associate_individual_picks_with_waveforms` | Match each individual-file group to the one waveform that encloses it | 10, 22 |
| 32 | `32_build_waveform_pick_events` | Waveform and pick-only containers (WAV+PICKS, WAV_ONLY, PICKS_ONLY) | 10, 22, 30 |
| 33 | `33_plot_event_association_diagnostics` | Daily association bar charts | 32 |
| 34 | `34_association_review.md` | Written review of steps 30–33 | — |
| 35 | `35_review_multiple_pick_groups` | Waveform plots of files holding several pick groups | 32 |
| 40 | `40_build_hypocenter_indexes` | Hypocentre indexes from `Pinatubo_all.sum` (`40_*`) and PINAALL (`41_*`, original or annotated parser) | raw hypocentre files |
| 42 | `42_compare_hypocenter_indexes` | Exact comparison of the two indexes | 40 |
| 43 | `43_associate_hypocenters` | Group origins from both indexes; choose a preferred origin | 40 |
| 44 | `44_plot_hypocenters_per_day` | Daily counts of the two hypocentre sources | 40 |
| 45 | `45_hypocenter_review.md` | Written review of the hypocentre sources | — |
| 50 | `50_build_obspy_catalog` | Canonical ObsPy catalogue (pickle and QuakeML) with seven W/P/H composition classes | 22, 32, 43 |
| 51 | `51_event_numbers_in_complete_windows` | Daily composition counts in reporting windows | 50 |
| 52 | `52_build_seisan_rea_catalog` | SEISAN REA S-files | 50 |
| 53 | `53_seisan_rea_diagnostics` | Audit of the written REA database | 52 |
| 54 | `54_export_antelope_css3` | Antelope CSS3.0 catalogue and optional wfdisc | 50 |
| 60 | `60_prepare_bob_archives` | Unpack BOB ZIP archives (preview by default; the only notebook that changes LEGACY) | raw BOB |
| 61–68 | `61_rsam_and_events` … `68_radio_maps` | One BOB product each: RSAM/EVNT, SSAM, tilt, rain, mudflow, COSPEC, counts, radio maps | 60 |
| 69 | `69_bob_coverage_and_review` | Network-wide BOB availability overview | 61–68 |

Notes on specific steps:

- **10.** `waveform.recover_partial` is `true` in the configuration: a damaged DMX file keeps the complete traces before its first defect, and the waveform index records `partial_recovery` and `recovery_details`. Keep those columns in any downstream provenance. The optional diagnostic export of recovery candidates uses `pinatubo_steps/dmx_recovery.py`. See FloVoPy `docs/dmx_recovery.md`.
- **40.** Set `PARSER="original"` (default) or `"annotated"` in the PINAALL section. Annotated outputs have their own names and must be selected explicitly downstream.
- **50.** `association_policy` is `picks_then_window` (default), `one_to_one` or `nearest`. A hypocentre linked to several events under `nearest` gets a per-event copy of its origin with a unique ID.
- **52.** `collision="shift"` moves only the S-file *name* by up to 30 s when two events share a second; event and pick times are unchanged. A manifest (`.flovopy_rea_manifest.json`) makes reruns reuse unchanged files; `overwrite=True` is needed after the catalogue changes. Untracked files are never overwritten.
- **60–69.** Each product writes QC-schema-2 outputs (`coverage.csv`, `daily_coverage.csv`, `date_review.csv`, `review.json`, `export_status.json` and others) under `FAIR/BOB/<product>`. `OVERWRITE=False` protects existing products. Step 69 refuses incomplete or mixed products unless `ALLOW_PARTIAL=True`. RSAM/EVNT, SSAM, tilt, rain, mudflow and hourly CAB counts are converted to UTC with their acquisition PC's rule in `clock_regimes.csv`. COSPEC dates are taken as Philippine days; daily PIE counts and radio-map dates are kept as written. Rain and mudflow from 15 November, and CAB labels outside the rules, are flagged `unverified`.

## Tests

From this folder:

```bash
python -m pytest -q pinatubo_steps/tests tests
```

`pinatubo_steps/tests` covers the clock rules, PHA parsing and comparison, shared helpers and steps 32, 43, 50–54 (the notebook functions are loaded from their `pipeline_definitions` cells). `tests/` covers the BOB notebooks and products. FloVoPy has its own suite (`python -m pytest -q flovopy/tests` in the FloVoPy checkout). The configuration checks are run from the project root (see [../CONFIGURATION.md](../CONFIGURATION.md)).

## Reviews and reference documents

- [CODE_REVIEW_2026-11-17.md](CODE_REVIEW_2026-11-17.md): full review of steps 10–69, with status and remaining items.
- [TIMING_AND_CLOCKS.md](TIMING_AND_CLOCKS.md) and [clock_regimes.csv](clock_regimes.csv): clock model and UTC rules.
- [FLOVOPY_MIGRATION_REVIEW.md](FLOVOPY_MIGRATION_REVIEW.md): what moved into FloVoPy and what stays local.
- `24_phase_pick_review.md`, `34_association_review.md`, `45_hypocenter_review.md`: written reviews between steps.

## Other files

- `pinatubo_steps/`: `config.py` (paths), `clock.py` (UTC rules), `shared.py` (ID and time helpers), `pha_parser.py` (Pinatubo wrapper around FloVoPy's phase-card reader), `pha_comparison.py` (step 23), `dmx_recovery.py` (step 10 diagnostics).
- `superseded/bob_pre_flovopy/`: the BOB notebooks that came before 60–69.
- `legacy_bob/` and `unpack_bob_archives.py`: the older BOB command-line tools; `unpack_bob_archives.py` now runs step 60's definitions. Outputs of the old `legacy_bob` tools are not inputs to step 69.
- `audit_ssam_coverage.py`, `audit_data_ssam_coverage.py`, `audit_mudflow_coverage.py` and `README_ssam copy.md`: recovered early SSAM and mudflow audits, kept for reference.
- `refactor_manifest.json`: the historical record of converting the original scripts to notebooks. The modules it lists were later folded back into the notebooks.
