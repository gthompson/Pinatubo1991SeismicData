# Event Metrics Pipeline v3

## Run

```bash
python pipeline.py --config config.json --max-files 5
python pipeline.py --config config.json
python summary_plots.py event_metrics_output
```

Replace `pipeline.py` and `summary_plots.py` together. Merge the additional `artifact_audit` configuration keys from `config.example.json` into your config.

## New audit

- The existing `peak_hz`, `peak_psd`, `psd_ratio` are retained for backward compatibility.
- `narrowband_detected` now requires a local peak, a minimum prominence, and maximum width. A 38–43 Hz maximum alone is not a detected artifact.
- `fixed_psd`, `fixed_background_psd`, `fixed_ratio` measure PSD at the bin nearest 40.45 Hz and surrounding spectral background, independently of whether a narrowband peak is detected.
- `fixed_frequency_attenuation_db` measures pre/post filtering attenuation at that fixed bin.
- `noise_before_*`, `noise_after_*`, `event_before_*`, `event_after_*` are the same audit applied to the first five seconds and event portion, both before and after filtering. Each includes `status`, `nperseg`, `fixed_psd`, `fixed_background_psd`, etc.
- The 5-second noise window has lower frequency resolution and may contain seismic signal; do not equate it automatically with true pre-event noise.
- PSD is in counts squared per Hz. Cross-station amplitude comparisons require calibration and gain history.

## Plots

- `artifact_overview.png`: detected peak frequency, prominence ratio, attenuation, scatter (no misleading connecting lines).
- `station_coverage.png` and `daily_coverage.csv`: daily unique station/channel and event-file counts. This is **coverage in recorded files**, not proof of operational uptime.
- `<NET.STA.LOC.CHA>_metrics.png`: spectral band ratio (bubble area encodes amplitude), mean frequency, event vs initial-window RMS, absolute artifact vs broadband PSD.
- `<NET.STA.LOC.CHA>_noise.png`: initial-window RMS, 40.45 Hz PSD, local background PSD through time.
- `<NET.STA.LOC.CHA>_artifact.png`: full-trace artifact/background PSD and noise-vs-event fixed-frequency PSD.

## Notes

- Existing CSVs can be replotted, but the new PSD fields are only populated by rerunning waveform processing with v3. Existing historical plots may therefore have empty new panels.
- `input.start` and `input.end` define the processing interval; the example processes **June 1991 only**. Compare the number of June files on disk with `files_processed.csv` before assuming a file-discovery problem.
- The pipeline uses ObsPy `Stream.filter('lowpass', ...)` on a copy; original files are unchanged.
- This version has passed Python syntax checks. Full ObsPy/FloVoPy execution needs validation in your `flovopy_plus` environment.

## v4: Daily station representation and lower event-amplitude envelope

`summary_plots.py` now produces `daily_station_counts.csv`,
`daily_station_amplitude.csv`, `plots/stations_per_day.png`,
`plots/daily_min_amplitude_heatmap.png`, and one
`plots/<trace_id>_daily_min_amplitude.png` for each channel.

Daily amplitude uses the **event-window `sample_rms` in counts**, after the
configured filtering, and is grouped by UTC day and complete trace ID. Each
station/channel/day has the minimum, 5th percentile, 10th percentile, median,
and number of distinct recorded WAV files. This avoids pooling differently
calibrated stations and displays sample size to help interpret minima.

Station counts represent stations *present in the recorded event WAV files*,
not verified network uptime. The minimum recorded event RMS is **not** a
calibrated detection threshold. Daily minima tend to increase mechanically
when fewer events are observed, so interpret alongside the 5th percentile,
median, and per-channel daily event count. A stronger test of detection
completeness requires station uptime, triggers and event association.

Regenerate plots and tables without waveform reprocessing:

    python summary_plots.py event_metrics_output
