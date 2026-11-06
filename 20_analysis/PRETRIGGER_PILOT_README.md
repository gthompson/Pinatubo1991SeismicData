# Archive QC and noise-window pilot

Open `00_archive_qc_and_pretrigger.ipynb` using the FLOVOpy Python environment. Keep `pretrigger_qc.py` beside it. The notebook selects a small sample of archive files, saves diagnostic tables/figures, and leaves all window decisions unreviewed.

See `NOISE_WINDOW_STRATEGY.md` for the decision rules, assumptions, thresholds and production validation plan. The implementation now uses distinct-station event coincidence, trace-specific early-rise refinement and a quiet-prefix audit rather than extending noise to the first large amplitude crossing. Weak channels default to the assumed five-second minimum; invalid references or guard conflicts remain unresolved.

Full-record views accompany onset zooms. Tables retain original amplitude/STA-LTA candidates alongside selected onset support, noise status and flags. Independent spectral and signed Hilbert diagnostics include coverage information. Negative Hilbert estimates are retained, not silently discarded.

Outputs are under `products/pretrigger_qc/<fingerprint>/`. Review `manual_window_review.csv` before using noise/event metrics. Existing review files are preserved for matching runs. Source waveforms are unchanged; no timing correction or instrument response is applied. Arrival times do not identify the acquisition trigger or establish a ten-second buffer.

Run helper checks with the FLOVOpy environment: `python -m unittest discover -s tests -p 'test_*.py'`.

## FLOVOpy implementation

The authoritative engine is `flovopy.processing.detection.detect_pretrigger_window`, with `PretriggerSettings` in that module. `flovopy.core.vdap.detect_pretrigger_window` is a thin entry point to the same implementation, with identical settings/results. Generic code lives in `flovopy.processing.event_windows`; the local helper now contains only file selection, export/review orchestration and plots, plus compatibility imports for existing diagnostic tests.

Use the updated FLOVOpy checkout in the notebook kernel. Generic results use `_trace_s` and `_record_s` timing fields. The local export layer maps `_record_s` to the existing `_file_s` columns. New onset-support start/end columns describe the first measurement support, not statistical confidence or proof of a physical onset. Support is particularly useful for reviewing emergent signals; rockfall/PDC performance still needs labelled validation.

Other datasets may use a shorter initial reference and explicitly enable single-station mode with `network_min_stations=1`. Optional frequency/Hilbert diagnostics can be disabled in settings. Durations are rounded to whole samples, with actual fine/frequency window lengths recorded in QC. Invalid reference traces cannot seed a network event. Pinatubo keeps the five-second minimum, one-second guard and distinct-station defaults. New engine provenance creates a separate output directory; prior manual review files are preserved.
