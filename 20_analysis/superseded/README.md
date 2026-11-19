# Superseded analysis code

Kept for the record; nothing in the current notebook sequence imports it, and `pytest.ini` in the parent folder does not collect its tests. Paths in these files assume they sit in `20_analysis`, and some reference pre-UTC data, so do not rerun them as they are.

| Item | Retired | Replaced by |
|---|---|---|
| `00_archive_qc_and_pretrigger.ipynb`, `PRETRIGGER_PILOT_README.md` | 18 Nov 2026 | Notebook 20 (onset histogram, frequency and Hilbert plots). Its file sampler `pretrigger_qc.select_files` was removed with the `pilot` selection mode; see Git history before this move. |
| `cli_pipeline/`: `pipeline.py`, `summary_plots.py`, `temporal_features.py`, their configs, `README*.md`, `validation/pinatubo_smoke/`, `test_enhanced_stream.py`, a config backup, `tests/test_pipeline.py` | 18 Nov 2026 | The notebook sequence and `analysis_workflow.py`; features come from FloVoPy `core.features`. |
| `cospec/`: `cospec_plotter.py`, `cospec_config.json`, `COSPEC_README.md`, `70_cospec_legacy_series.ipynb`, `tests/test_cospec_plotter.py` | 18 Nov 2026 | Conversion step 66 (`10_conversion/66_cospec.ipynb`), which treats COSPEC dates as Philippine days; this plotter labelled them GMT midnight. |
| `legacy_rsam/legacy_rsam_analysis.ipynb` (and its HTML export) | 18 Nov 2026 | Conversion step 61 (RSAM and RSAM events on UTC). The notebook read raw RSAM and pre-UTC waveforms from a hard-coded Box path. |
| `earthscope_probe/test_earthscope_download.ipynb` | 19 Nov 2026 | Not replaced: a one-off probe of EarthScope (FDSN station, availability and dataselect) for network XB in 1991; it was never part of the sequence. |
