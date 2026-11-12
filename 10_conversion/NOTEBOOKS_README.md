# Pinatubo pipeline notebooks

The 19 numbered Python scripts in Archive.zip now have corresponding notebooks and callable local modules. Existing scripts, the launcher, and step 10 are untouched. This is a refactor of archived implementations, not a claim that every historical parsing or association assumption has been validated.

Open the notebooks in Jupyter using a kernel with current FLOVOpy, ObsPy, pandas (supporting format="mixed"), NumPy, Matplotlib and IPython. Jinja2 is needed for LaTeX exports where used. Launch Jupyter from the pipeline folder or project root. Each notebook reads the shared conversion_config.json for input and external output roots, with per-notebook SETTINGS, a separate run cell, and returned objects for inspection. See ../CONFIGURATION.md. Diagnostic notebooks preview saved PNGs inline. No archive data processing has been run during refactoring.

The notebooks import adjacent pinatubo_steps modules with explicit keyword-only run(...) functions. There is no subprocess or argparse dependency in their execution paths. pinatubo_steps/config.py centralizes paths matching the archived shell launcher; DATA_TOP is supplied by the notebook. Keep this package alongside the notebooks. Restart the kernel after editing modules to avoid stale imports.

## Order and inputs

| Step | Purpose | Main prerequisites |
|---|---|---|
| 11 | Waveform diagnostics | Step 10 waveform index |
| 15 | Post-conversion network history, StationXML and provenance audit | Step 10 converted WAV database; historical source metadata |
| 20 | Individual-event PHA parsing | Individual PHA directory |
| 21 | Pinatubo multi-event PHA parsing | Monthly/yearly-format PHA directory |
| 22 | Merge and suppress duplicate picks | 20 + 21 |
| 23 | Pick/event diagnostics | 20 + 21 + 22 |
| 30 | Authoritative pick/waveform association | 10 + 22 |
| 32 | Waveform/pick event spine | 10 + 22 + 30 |
| 33 | Association diagnostics | 10 + 30 + 32 |
| 40 (includes former 41) | Separate HYPO71 and PINAALL ingestion sections | Pinatubo_all.sum plus original or annotated PINAALL |
| 42 | Exact hypocenter comparison | 40 + chosen 41 |
| 43 | Reconcile hypocenters | 40 + chosen 41 |
| 44 | Daily hypocenter diagnostics | 40 + chosen 41 |
| 50 | Consolidated catalog builder: one_to_one (default) or nearest | 22 + 32 + 43 |
| 51 | Counts in trusted coverage windows | Chosen 50 catalog pickle |
| 52 | Nordic REA export | Chosen 50 catalog pickle |
| 53 | REA diagnostics | 52 |

The combined 40_build_hypocenter_indexes.ipynb has independent summary and PINAALL sections. The PINAALL section selects original or annotated parsing. All parser implementations remain separate. Annotated outputs have separate names; explicitly select them downstream. The former v2 notebook is archived under superseded/step41. Step 50 is now consolidated: one_to_one is the default, while nearest explicitly selects the old reuse-permitting policy. Both keep the canonical output prefix. The alternate notebook is archived under superseded/step50; its import module and both command-line filenames delegate to the canonical builder.

The annotated PINAALL parser preserves reported time and time-basis warnings. local_utc_offset=None leaves unresolved local times unresolved; review the catalog's transition-date annotations before using those rows in UTC association. This is historical Pinatubo policy, not generic VDAP behavior.

## Small correctness changes accompanying the refactor

- Preserve event, waveform, pick and origin identifiers as strings at the CSV boundaries in steps 22, 30, 32, 33 and 50. A synthetic test demonstrated that numeric inference in the original step 32 could separate primary picks into PICKS_ONLY while putting secondary picks into the waveform event. Keeping identifiers as strings repairs that association and preserves leading zeroes.
- Step 52 records per-event origin provenance. The original global stats expression labelled every origin DERIVED, including real hypocenters, and checking the event after ensure_origin() could falsely claim that a derived anchor was a hypocenter. Both comments now use the recorded per-event status.
- Step 41 v2 raises ValueError for a missing input instead of calling an argparse parser that does not exist in notebook execution.
- Notebook settings use actual script interfaces: step 52 takes catalog_pkl, not the launcher's obsolete --quakeml; step 53 has no --db-name and uses the parser's default wavefile pattern. Step 51 was not wired into the archived launcher and now has explicit input/output settings.

## Validation and limits

All 19 notebooks pass notebook-schema and Python-syntax checks. All 19 callable modules import and accept their notebook settings. Original helper function ASTs are unchanged. The optional-parser import is relative to the local package.

A synthetic chain was executed through steps 20, 21, 22, 30, 32, 43, 50 alt, 51 and 52. It verifies low-gain fixed-column PHA parsing, monthly delimiters, primary/secondary suppression, primary and secondary picks in one waveform event, hypocenter association, catalog construction and real Nordic S-file output. The generated S-file retains REAL origin status. This does not replace testing against the actual legacy archive, particularly alternate PINAALL time conventions and the original step-50 association policy.

Known inherited issues and migration recommendations are in FLOVOPY_MIGRATION_REVIEW.md. Outputs follow original overwrite behavior. Step 52's collision suffixes can create duplicate S-files on reruns and are not recognized by step 53's filename matcher; use a deliberately chosen output folder and resolve that issue before treating reruns as complete inventories.

## Step-50 consolidation

The single active Step-50 notebook uses one shared catalog builder. The default policy has changed from nearest to one_to_one. Older script filenames still work through thin compatibility entry points. Existing data products were not regenerated. Set association_policy="nearest" to reproduce the original matching policy, or use separate output prefixes to compare results. Tests cover competing events, tolerance boundaries, ties, empty/no-match inputs, unmatched hypocenters, pick-only events, leading-zero IDs, missing optional pick metadata and QuakeML readback.

## Step-41 notebook consolidation

Use PARSER="original" (default) or PARSER="annotated" in the PINAALL section of 40_build_hypocenter_indexes.ipynb. Both parser modules and command-line scripts remain unchanged. Settings select the corresponding source and outputs. LOCAL_UTC_OFFSET only affects annotated parsing; its None default does not resolve local-time uncertainty. Existing data products were not regenerated.

## Combined hypocenter ingestion notebook

40_build_hypocenter_indexes.ipynb replaces the separate active step-40 and step-41 notebooks. Common setup is shared; settings/run/review cells are independent for Pinatubo_all.sum and PINAALL. Canonical output filenames still start with 40_ and 41_, so steps 42–44 require no changes for original input. The former active notebooks are under superseded/hypocenter_ingestion; earlier superseded variants remain archived where previously placed. No parser or command-line implementation changed.

## Shared-helper extraction and FLOVOpy promotion

The pipeline now imports table/time cleanup, XB fallback ID construction, plotting defaults and compatibility adapters from pinatubo_steps/shared.py. Scalar and table ID fallback now both treat missing fields as blank, rather than generating literal "nan" station/channel strings. Existing nonblank IDs are retained.

Dataset-independent association, four-field SEED-ID splitting and ObsPy comment lookup live in flovopy.core.event_utils. Generic association has configurable field names and neutral output columns; no Pinatubo network, source priority or event classes are embedded. The existing Step-50 helper names remain import-compatible adapters. Install/update FLOVOpy from the edited checkout before running the pipeline in another environment.

Monthly/yearly PHA block parsing, source-priority deduplication, PINAALL annotations/time conventions, trusted coverage windows and event classifications remain local and unchanged. Schema-specific build_pick/build_origin functions remain in the single Step-50 builder. Older migration recommendations above are historical; matching and metadata primitives have now been promoted, while schema/policy-dependent functions remain candidates.

## Selected VDAP/REA migrations

Single-event PHA ingestion now uses flovopy.core.vdap.read_vdap_pha_file. Monthly/yearly PHA grouping remains local. Step 40 adapts the corrected VDAP HYPO71 parser to its existing CSV fields. Step 43 calls ObsPy locations2degrees/degrees2kilometers directly, retaining its existing spherical-distance convention.

Step 52 delegates to flovopy.seisanio.rea_export.export_catalog_to_rea. The notebook exposes collision policy and overwrite controls. Reruns reuse manifest-tracked unchanged files; untracked legacy S-files produce collision errors rather than being overwritten. Use a fresh output folder when migrating an older export. Explicit filename shifts are recorded and never change actual event times. Export reports written/reused/skipped/failed records. CLI entry points 20/40/43/52 delegate to the same modules as the notebooks.

The selected work does not promote representative-event-time selection, generic pick deduplication, table-to-ObsPy conversion or diagnostics. Historical Pinatubo metadata, grouping and anchor selection remain local. See FLOVOPY_MIGRATION_REVIEW.md for current scope.

## PHA cross-source validation in Step 23

The final notebook section compares existing step-20 and step-21 CSVs independently of step 22. pinatubo_steps.pha_comparison.compare_pick_sources accepts copied DataFrames; compare_pick_csvs reads existing CSVs and writes nine separate 23_pha_ reports under QC/step23_pick_event_diagnostics/pha_cross_source by default. Source products and raw PHA files are not modified or reparsed.

Reports include directional per-day/overall overlap at 0.05 and 1 second (configurable), mutual-unique matches, reused counterparts, all candidate pairs, signed timing summaries, unmatched rows, invalid records, date audits and nearest same-identity candidates even outside the largest tolerance. Comparison uses full normalized SEED ID and phase. Row IDs distinguish duplicates without relying on possibly duplicated pick IDs. Raw/source provenance is retained where supplied.

Zero valid rows give NaN rates and no_records/invalid_only status. Missing inputs raise rather than masquerading as zero coverage. Reporting dates do not truncate cross-midnight match candidates. Invalid records and date audits include all input dates with a comparison_in_scope flag; the other per-pick reports honor reporting dates. All-candidate timing statistics can be biased by multiplicity; mutual-unique statistics are provided separately.

Folder-date audits require real source-path metadata. Step 20 now adds pha_path to future outputs; existing CSVs with basenames only cannot establish folder dates. Full filename/folder mismatches are flagged, not corrected. Monthly block grouping, source-priority merging and deduplication are unaffected. The three older root-level converter/verification scripts remain historical; they are not called by this review section.

## Legacy BOB conversion — sequenced FLOVOpy notebooks

The active workflow is now 60–69 below. Generic format readers come from
`flovopy.legacy.bob`; paths, channel interpretations and historic clock questions
remain in the Pinatubo notebooks. Use a kernel with the current editable FLOVOpy
checkout, pandas, NumPy, Matplotlib and IPython. Code is embedded in each notebook;
no local BOB parser module is imported by this sequence.

| Step | Notebook | Product |
|---|---|---|
| 60 | `60_prepare_bob_archives.ipynb` | Explicit in-place extraction, archive relocation and inventory |
| 61 | `61_rsam_and_events.ipynb` | Annual RSAM and historical RSAM EVNT |
| 62 | `62_ssam.ipynb` | SSAM plus DATA, retained as separate source products |
| 63 | `63_tilt.ipynb` | Tilt, battery, temperature and unresolved supporting channels |
| 64 | `64_rain.ipynb` | One combined daily-file rain table |
| 65 | `65_mudflow.ipynb` | Three positional mudflow fields |
| 66 | `66_cospec.ipynb` | Sparse SO2 observations and attributed unit inference |
| 67 | `67_counts.ipynb` | Distinct historical earthquake-count versions |
| 68 | `68_radio_maps.ipynb` | Dated telemetry-map snapshots |
| 69 | `69_bob_coverage_and_review.ipynb` | Network-wide daily presence figure, range table and review |

All notebooks use `conversion_config.json` through `pipeline_paths`, including
external project-result overrides. Inputs are `LEGACY_TOP/BOB`. Outputs are
`FAIR_TOP/BOB/<product>` under the same external output root. No additional output
root is introduced. A product can override its source folder with
`legacy_bob.<product>.source_subdir`, relative to BOB; absolute paths and ZIP
inputs are deliberately rejected. Product names include `rsam`, `rsam_events`,
`ssam`, `ssam_data`, `tilt`, `rain`, `mudflow`, `cospec`, `counts`, `radio`.
Old `legacy_bob.products` and `rain.source` settings belong to the historical CLI
and are not used by the new notebooks. Run the selected notebook instead.

Step 60 previews by default (`APPLY=False`). After reviewing its report, set
`APPLY=True`: files are extracted beside their ZIP and fully handled archives
are hash-verified and moved to the parallel `LEGACY/bob_zipfiles_obsolete` tree.
Existing files are never overwritten. Identical existing members are accepted;
conflicts, damaged members, unsafe paths and relocation clashes retain the active
archive for review. Nested ZIPs are handled in bounded passes. Reports and the
source inventory go to `FAIR/BOB/archive_preparation`. Do not delete unresolved
archives to bypass downstream ZIP guards. Keep an independent source backup.

Steps 61–68 never unpack ZIPs or alter LEGACY. They export raw and usable values,
missing flags, source file/line provenance, duplicate-time flags, reader metadata,
parser errors, per-series coverage, daily coverage and a review summary. Source
hashes and available BOB/STATIONS.TXT text accompany exports. `OVERWRITE=False`
protects existing derived products; review before deliberately rerunning with it
set True. Raw legacy names remain the identifiers for unresolved auxiliary
channels; waveform SEED IDs/gains are not assigned automatically.

The overview unions observed calendar days across stations, including SSAM/DATA,
without summing duplicate measurements. Sparse COSPEC dates remain sparse.
The main figure ends at 1 December 1991; full-range tables retain the 1992 SSAM
exception. Coverage means finite nonsentinel records exist, not uptime. No
cross-system clock synchronization, inferred calibration, interpolation, automatic
source-version selection or drift correction is applied. SSAM band definitions are now ingested from the archived BN descriptions; step 62
uses FLOVOpy precomputed band results for daily/hourly multi-channel figures.
BN01–10 are frequency-axis heatmaps; overlapping BN11–16 remain separate traces.
Verify SSAM averaging duration, RAIN/MUDFLOW field meanings, annual indexing phase
and historic GMT/local changes
before quantitative analysis. Each notebook documents those questions.

The superseded 60/61 notebooks are preserved under `superseded/bob_pre_flovopy`.
Older `legacy_bob` modules and CLI remain for compatibility with previous exports
and scripts; their outputs under `output_root/legacy_bob` are historical and are
not inputs to step 69. Do not mix those products with the new FAIR/BOB outputs.
The archive CLI now delegates to the safe step-60 definitions.

### QC review of the first BOB executions (12 November 2026)

The revised notebooks preserve the original saved-run findings in Markdown and
clear stale output cells. Original executed versions are backed up before install.
Most products had saved successful reads; MUDFLOW and the overview had not been
executed. Zero active ZIPs in step 60 means nothing needed extraction at that time,
not independent verification of already moved archives.

Each product now writes QC-schema-2 outputs. Complete source records retain all
reported dates/values plus review flags; `date_review.csv` lists invalid and
out-of-expected-period labels. `coverage_all_recorded_dates.csv` retains the raw
span, while `coverage.csv` and `daily_coverage.csv` use only valid labels in the
expected archive period. Defaults are 1991, with 1992 allowed for SSAM/DATA.
This flags the syntactically valid 1919 date in both CAB count versions without
inventing a correction. Override `legacy_bob.<product>.expected_dates.start` and
`end_exclusive` only with evidence.

Other products include `field_quality.csv`, `duplicate_records.csv`,
`source_roles.csv`, `review.json`, and `export_status.json`. TEST and TEMP path
names are auxiliary-source candidates, retained and labelled for review, not
silently dropped or merged. A timestamp is counted once per source series in
the daily-count plots; source versions still remain separate. Availability
figures show actual observed days by original series. Value panels crop annual
padding to usable dates. All series are plotted unless a visible limit is set;
`plot_manifest.csv` records both generated and explicitly omitted panels. SSAM
uses availability diagnostics plus its dedicated multichannel plots.

Exports are staged before replacement and completion is marked last. Step 69
requires complete schema-2 markers with matching run IDs; older, missing or mixed
products block the full overview. `ALLOW_PARTIAL=True` is an explicit diagnostic
mode with incomplete-row/figure labels, not a publication-ready completeness
claim. Radio maps have separate snapshot-equivalence checks.

Set `OVERWRITE=True` deliberately to regenerate previous derived outputs, then
run the revised notebooks in sequence. Inputs remain untouched except the
explicit archive-preparation action in step 60. These improvements have been
checked with fixtures; they do not imply a new full archive execution.


## DMX failure inspection and partial recovery

`13_inspect_partial_dmx_recovery.ipynb` reads the step-10 failures with ObsPy/FLOVOpy and plots each retained trace separately (no plotting merge of duplicate IDs). Candidate inspection output goes under the configured output_root/recovery_diagnostics. The shared validated-prefix implementation is flovopy.core.dmx_recovery; local step 12 exports diagnostic candidates and delegates structural parsing to that module.

FLOVOpy read_dmx_file tries regular reading first, then recovery by default. A recovery warning and stats.dmx_recovery mark partial reads. Step 10 remains strict by default; set waveform.recover_partial=true in conversion_config.json only after inspecting candidates. Enabled partial exports add partial_recovery and recovery_details to the waveform index. Keep those columns in any downstream or publication provenance. No raw file or clock is repaired. See FLOVOpy docs/dmx_recovery.md.

## Step 15 — final post-conversion network reconstruction

Run `15_network_history_and_stationxml.ipynb` after step 10. Step 05 is retained for an early documentary reconstruction; step 15 scans the exact configured SEISAN_WAV_DB for all selected years and writes under NETWORK_HISTORY_OUTPUT. It never falls back to the old source-tree header inventory. The notebook explains every block and embeds source precedence, helicorder sensor conflicts, all known epoch-boundary issues, PI2 normal/low-gain evidence, independent clocks and BOB time shifts, response-chain/ADC gaps, and questions for VDAP colleagues.

Generated audit products include scan source/file inventory/errors, source metadata and code/config hashes, sensor conflict register, StationXML schema/overlap/undated-channel audits and response completeness. Documentary XML is retained; separate `_selection_safe.xml` copies can encode nonoverlapping shared endpoints, with a change log explaining the one-microsecond selection convention. No date precision or calibration is invented. Full count responses remain an explicit target, not something established by the current nominal voltage file.

The notebook performs a full header scan by default; disable it only to reuse a matching root/year/file fingerprint inventory. It does not modify waveform samples or legacy records. `network_history/finalize_inventory.py` contains the reusable scan, audit, boundary-copy and packaging helpers.
