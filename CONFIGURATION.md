# Pinatubo notebook configuration

The active conversion notebooks share `10_conversion/conversion_config.json`.
The active analysis notebooks share `20_analysis/analysis_workflow.json`.
Keep these small configuration files with the code; generated data go elsewhere.
No archive conversion or analysis is started by editing or loading configuration.

## Default input and output locations

| Setting | Default |
|---|---|
| Conversion `data_top` (existing archive, read inputs) | `~/Library/CloudStorage/Box-Box/thompsong/DATA/pinatubo1991` |
| Conversion `output_root` | `~/DATA/Pinatubo1991Processing/conversion` |
| Analysis `wav_root` (existing recordings) | `~/Library/CloudStorage/Box-Box/thompsong/DATA/pinatubo1991/FAIR/SEISAN/WAV/PNTBO` |
| Analysis `output_root` | `~/DATA/Pinatubo1991Processing/analysis` |
| Analysis `network_history_products` (read metadata) | `~/DATA/Pinatubo1991Processing/conversion/network_history` |

The Box input directories exist on the inspected machine. Change these settings
for another machine, drive, selected archive version or deliberately separate run.
`~` expands to the current user's home. Relative paths are resolved against the
configuration directory, so a relative `products` inside the checkout is rejected.

The shared helper resolves paths and symlinks before rejecting output roots in
this or another Git checkout. Conversion outputs must not overlap LEGACY inputs.
This validates configured roots; direct calls to low-level helpers or overriding
notebook SETTINGS are expert escape hatches and still require deliberate paths.

## Conversion

Notebook 05 writes network maps, StationXML, chronology and timing/epoch tables to
`<conversion-output>/network_history`. Optional header rescans write under its
`header_scan` subdirectory, leaving the shipped evidence files unchanged.

Notebook 10 writes MiniSEED under `<conversion-output>/FAIR/SEISAN/WAV/PNTBO`.
`waveform` options in the JSON control the sampling-rate argument, glob, file limit,
overwrite and verbosity. The retained 100-Hz setting is an existing choice requiring
the scientific check already identified; this path change does not validate it.

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

## Analysis

Run outputs live in `<analysis-output>/notebook_workflow/<run-id>`. Persistent event
sidecars, original-sample inventories, onset caches and review registry live in
`event_metrics_cache`, `file_inventory_cache`, `pretrigger_file_cache` and
`window_review_registry.csv` under that same external root. Generated backups of
configuration changes go to `config_backups` there; the deliberately edited active
JSON remains in `20_analysis`. Network rebuilding writes to the current external
run; otherwise metadata are read from `network_history_products`.

`PINATUBO_ANALYSIS_OUTPUT` optionally overrides the analysis output root. Numerical
selection, frequency bands and window policy have not been changed by this update:
current mode is all files, fixed five-second assumed noise, event to record end.
For a controlled trial select a sample in notebook 20 or the shared JSON first.
Do not interpret that reference window as verified quiet background.

Analysis initially reads the existing Box waveform archive. If conversion creates
a different selected waveform release, explicitly update `wav_root` to its
`FAIR/SEISAN/WAV/PNTBO` directory before analysis. Changing conversion's output root
does not silently change the analysis input.

The publication selection remains pinned to its earlier pilot. A changed run/root
may require deliberate example reselection; it must not silently substitute new
scientific examples. Existing cache directories in the repository are left intact.
This update does not migrate them or promise cache compatibility after code/config
changes. Move/copy reviewed products only after checking signatures and provenance.

The older combined pretrigger notebook and standalone metrics command also route
outputs externally. The standalone command retains its own `config*.json` files;
it is not the current notebook-analysis workflow. The historical RSAM notebook
already targets an external Box plot folder and remains separate.

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
