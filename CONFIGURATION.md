# Pinatubo notebook configuration

The active conversion notebooks share `10_conversion/conversion_config.json`.
The active analysis notebooks share `20_analysis/analysis_workflow.json`.
Keep these small configuration files with the code; generated data go elsewhere.
No archive conversion or analysis is started by editing or loading configuration.

## Default input and output locations

| Setting | Default |
|---|---|
| Conversion `data_top` (existing archive, read inputs) | `~/Library/CloudStorage/Box-Box/thompsong/DATA/pinatubo1991` |
| Conversion `output_root` | `~/work/Pinatubo1991Processing/utc` |
| Analysis waveforms (read) | conversion `<output_root>/FAIR/SEISAN/WAV/PNTBO`; optional `wav_root` override |
| Analysis `output_root` | `~/work/Pinatubo1991Processing/analysis` |
| Analysis station metadata (read) | conversion `<output_root>/FAIR/metadata/network_history` (step 15) |

The Box input directories exist on the inspected machine. Change these settings
for another machine, drive, selected archive version or deliberately separate run.
`~` expands to the current user's home. Relative paths are resolved against the
configuration directory, so a relative `products` inside the checkout is rejected.

The shared helper resolves paths and symlinks before rejecting output roots in
this or another Git checkout. Conversion outputs must not overlap LEGACY inputs.
This validates configured roots; direct calls to low-level helpers or overriding
notebook SETTINGS are expert escape hatches and still require deliberate paths.

## Conversion

Notebook 10 writes MiniSEED under `<conversion-output>/FAIR/SEISAN/WAV/PNTBO`.
`waveform` options in the JSON control the sampling rate, glob, file limit,
overwrite, verbosity and partial-file recovery. `sampling_rate: null` keeps the DMX
header rate (3125/31 Hz = 100.80645 Hz); a number would overwrite the header rate
without resampling. `recover_partial: true` keeps the complete traces before the
first defect of a damaged file and records it in the waveform index.

Notebook 15 writes network history, channel epochs, StationXML and audits to
`<conversion-output>/FAIR/metadata/network_history`, reading the station evidence
under `LEGACY/STATION_METADATA/NETWORK_HISTORY`. The step order and products of
every notebook are listed in `10_conversion/NOTEBOOKS_README.md`.

Subsequent conversion notebooks use `<conversion-output>/pipeline_tmp` for indexes,
logs and QC, and `<conversion-output>/FAIR` for metadata and REA. Existing old products
are not silently copied to the new output tree. Run prerequisites into that tree or
explicitly select verified inputs in notebook SETTINGS. Input and output roots are
separate: do not point `data_top` at an empty output tree.

Set `CONFIG_FILE` in the setup cell to use another configuration, or set
`PINATUBO_CONVERSION_CONFIG` in the kernel environment. The optional
`PINATUBO_CONVERSION_OUTPUT` environment variable overrides the output root.
Restart the kernel after updating code so stale modules are not retained.
Per-stage scientific parameters remain visible in notebook SETTINGS.

### Times are converted to UTC

`conversion_config.json` sets `"time_basis": "utc"` (the default), writes to
`~/work/Pinatubo1991Processing/utc`, and reads the clock periods from
`10_conversion/clock_regimes.csv`. Every time is converted where it is read:

| Step | Times converted |
|---|---|
| 10 | waveform start times (and SEISAN names); the rule must agree with the DMX header `localtime` field and the file order |
| 20, 21 | pick times |
| 15 | channel epochs (field-note dates and times are Philippine time) |
| 40 | origin times |
| 61–65, 67 | BOB RSAM, EVNT, SSAM, tilt, rain, mudflow and hourly CAB-count labels |

Raw values are kept in `raw_*` columns. The rule applied, and a nominal
absolute-time uncertainty, are recorded in `clock_*` columns; step 50 copies the
uncertainty into each QuakeML origin's `time_errors`. Records that cannot be
placed are written to `*_clock_unresolved.csv` (or, for waveforms, the failures
table) instead of the index. COSPEC dates are taken as Philippine days; daily PIE
counts and radio-map dates are kept as written. Rain and mudflow from 15 November
1991, and CAB labels outside the rules, are flagged `unverified`. Set `"time_basis": "recorded"` (and a different `output_root`)
only to reproduce the uncorrected products, such as the earlier run in
`~/work/Pinatubo1991Processing/converted`.

## Analysis

Run outputs live in `<analysis-output>/notebook_workflow/<run-id>`. Persistent event
sidecars, original-sample inventories, onset caches and review registry live in
`event_metrics_cache`, `file_inventory_cache`, `pretrigger_file_cache` and
`window_review_registry.csv` under that same external root. Generated backups of
configuration changes go to `config_backups` there; the deliberately edited active
JSON remains in `20_analysis`. Analysis notebook 10 copies the conversion step-15
products into the run; it never rebuilds or writes them.

`PINATUBO_ANALYSIS_OUTPUT` optionally overrides the analysis output root. Numerical
selection, frequency bands and window policy have not been changed by this update:
current mode is all files, fixed five-second assumed noise, event to record end.
For a controlled trial select a sample in notebook 20 or the shared JSON first.
Do not interpret that reference window as verified quiet background.

Analysis reads the UTC waveform database and station metadata written by conversion,
located through `conversion_config.json`. Set `wav_root` in `analysis_workflow.json`,
or `PINATUBO_WAV_ROOT`, only to analyse a different waveform tree. The waveform file
paths and the step-15 product hashes are recorded with each run.

Notebook 60 writes a draft publication selection for the current run when none
exists, and stops if the existing selection belongs to another run. The draft for
the pre-UTC run is kept as `publication_selection.draft_e75d49b2bf48.json`.
Choosing the final examples is a deliberate scientific step. Existing cache directories in the repository are left intact.
This update does not migrate them or promise cache compatibility after code/config
changes. Move/copy reviewed products only after checking signatures and provenance.

Retired analysis code (the pretrigger pilot, the standalone metrics command with
its `config*.json` files, the COSPEC plotter and the pre-UTC RSAM notebook) is kept in
`20_analysis/superseded/`; see its README. RSAM and COSPEC now come from conversion
steps 61 and 66.

## Configuration checks

From the repository root, run:

```bash
python -m unittest discover -s tests -p test_pipeline_configuration.py -v
```

These tests cover root separation, containment and symlink rejection, environment
overrides, setup-context/manifest routing and external configuration backups. They
exercise setup in isolation from unavailable FLOVOpy numerical dependencies; they
are not a numerical pipeline or full-archive validation. Run the existing scientific
regression tests separately in the matching FLOVOpy environment. The network-product
tests expect notebook 05's products in the configured external network directory.
