# Pinatubo pipeline notebooks

The 19 numbered Python scripts in Archive.zip now have corresponding notebooks and callable local modules. Existing scripts, the launcher, and step 10 are untouched. This is a refactor of archived implementations, not a claim that every historical parsing or association assumption has been validated.

Open the notebooks in Jupyter using a kernel with current FLOVOpy, ObsPy, pandas (supporting format="mixed"), NumPy, Matplotlib and IPython. Jinja2 is needed for LaTeX exports where used. Launch Jupyter from the pipeline folder or project root. Each notebook reads the shared conversion_config.json for input and external output roots, with per-notebook SETTINGS, a separate run cell, and returned objects for inspection. See ../CONFIGURATION.md. Diagnostic notebooks preview saved PNGs inline. No archive data processing has been run during refactoring.

The notebooks import adjacent pinatubo_steps modules with explicit keyword-only run(...) functions. There is no subprocess or argparse dependency in their execution paths. pinatubo_steps/config.py centralizes paths matching the archived shell launcher; DATA_TOP is supplied by the notebook. Keep this package alongside the notebooks. Restart the kernel after editing modules to avoid stale imports.

## Order and inputs

| Step | Purpose | Main prerequisites |
|---|---|---|
| 11 | Waveform diagnostics | Step 10 waveform index |
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

## Legacy BOB conversion (step 60)

Run `60_convert_legacy_bob.ipynb`, or `python 10_conversion/60_convert_legacy_bob.py` from the repository root. Use `--config PATH` for a different shared configuration and `--products counts` (or cospec/radio) for a subset. No waveform processing is required.

Inputs come from `LEGACY_TOP/BOB/{COSPEC,COUNTS,RAD_FREQ}`, respecting `data_top` and optional `legacy_root`. All CSV, JSON and figure products go beneath the same conversion `output_root`, in `legacy_bob/{cospec,counts,radio}`; the existing external-output and input-overlap guards apply. No additional output-root setting is used. A manifest records input and code hashes and the effective configuration. Missing selected inputs fail preflight; no missing series is silently skipped.

COSPEC retains repeated timestamps; inferred units are labeled in configuration. COUNTS retains missing values, suspected −999 sentinels, duplicate times, unexpected years and malformed dates, without repairs or combining CAB versions. Radio tables retain literal tuning precision, ditto marks and both source copies; the convenience snapshot table is emitted only after checking agreement. MAP.TXT is hashed as context, not interpreted as geographic geometry. These outputs are standardized transcriptions and diagnostics, not calibrated or fully validated scientific products.

The older COSPEC entry point in `20_analysis` now delegates to step 60 and uses conversion_config.json. Its former analysis-output configuration is retained only as historical reference.

### RAIN compilation

Step 60 now includes `rain`. Run `python 10_conversion/60_convert_legacy_bob.py --products rain`. All daily `.DAT` files in LEGACY/BOB/RAIN compile to `output_root/legacy_bob/rain/rain_records.csv`, plus a coverage/provenance JSON and README. Both spaced and hyphenated date formats are supported. Values remain positional and uncalibrated; −998 flags, original time labels, duplicate times, raw lines and file/line provenance are retained. No timezone is assigned.

Optionally set `legacy_bob.rain.source` in conversion_config.json to a RAIN ZIP or another directory. Relative paths resolve against that configuration file. ZIPs are read directly, excluding __MACOSX entries, without extracting files. Default input remains the LEGACY folder.

### TILT and supporting channels

`61_tilt_read_and_plot.ipynb` provides the TILT class reader, channel/coverage table, source notes, CSV export and separate-panel plots for UBO and PPO. Step 60 also accepts `--products tilt`, writing beneath the shared `output_root/legacy_bob/tilt`. Requires NumPy, pandas and Matplotlib, without an ObsPy or FLOVOpy dependency. TILT deliberately uses composition rather than RSAM inheritance: signed tilt, temperature and battery observables must not inherit amplitude-processing or UTC assumptions. The binary storage layout is the same annual 576-byte-header/10-minute-float layout. Original source notes and hashes are exported.
