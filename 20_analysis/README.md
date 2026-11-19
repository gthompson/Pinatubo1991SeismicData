# Pinatubo 1991 waveform analysis

Notebooks `00`–`60` measure noise and event windows, spectra and summary figures from the UTC waveform archive built by `../10_conversion`. Start with [ANALYSIS_NOTEBOOKS.md](ANALYSIS_NOTEBOOKS.md), which gives the sequence, the products and the caveats.

- Configuration: `analysis_workflow.json` (selection, processing, spectra, plots, window policy, `exports`, `output_root`). Waveform and station-metadata locations come from `../10_conversion/conversion_config.json`; see [../CONFIGURATION.md](../CONFIGURATION.md).
- Code: `analysis_workflow.py` (orchestration, provenance and plots), `pretrigger_qc.py` (batch adapter around FloVoPy onset detection), `hybrid_diagnostics.py` (experimental notebook 45). Numerical methods live in FloVoPy.
- Conventions: [METRIC_SCHEMA.md](METRIC_SCHEMA.md) and [NOISE_WINDOW_STRATEGY.md](NOISE_WINDOW_STRATEGY.md).
- Tests: `python -m pytest -q` from this folder.
- Owned by conversion, not here: station metadata and StationXML (step 70) and all BOB products, including RSAM (step 61) and COSPEC (step 66).
- `superseded/`: retired notebooks and the old command-line pipeline, kept for the record.
