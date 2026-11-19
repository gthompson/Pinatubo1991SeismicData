# Event Metrics Pipeline v6 — temporal-spectral features

This package retains v5 plots and adds `temporal_features.py`, a reusable extraction module intended for later migration into FloVoPy. The module calculates event-window features from a filtered trace, using a paired initial five-second window to estimate band-specific noise. The pipeline appends these features to **event** rows in `trace_window_metrics.csv`. It does not overwrite existing EnhancedStream metrics.

## New features

- Full-band and LF/HF: RMS, energy proxy, Hilbert-envelope peak time and magnitude, envelope mean/std/peak-to-mean, energy quantile times (5/10/50/90/95%), D10–90 and D5–95 durations, temporal energy centroid and spread, first/last half and quarter energy fractions, envelope slopes, peak count.
- Per-band noise-relative: RMS SNR, envelope peak SNR, first/last time exceeding noise-relative envelope threshold, above-threshold duration and fraction.
- HF/LF comparisons: peak-time delay, onset/offset delay, energy quantile delays, centroid delay, energy ratio and log2 ratio, early/late band-ratio medians and change, HF-dominant/LF-dominant fractions, first sustained HF-to-LF transition time, envelope cross-correlation lag and normalized peak, envelope correlation, relative peak envelope, early/late energy fractions, LF energy after HF peak.

## Run

```bash
python pipeline.py --config config.example.json --max-files 20
python summary_plots.py event_metrics_output
```

The new metrics require reprocessing the waveforms; v5's summary-only plots did not. Existing data are in **uncalibrated counts**, so absolute amplitudes across station gains are not comparable. The first five seconds are *assumed* to be noise; validate against actual trigger times. Event duration is an observed-window measure and can be censored by recording length. Band filtering occurs after the original lowpass and on copies. Zero-phase filters and Hilbert envelopes have boundary artifacts; short events, edge peaks, and low-SNR measurements should be QC-screened. Noise-relative onsets may be undefined (NaN) and LF/HF transitions are heuristic. The current algorithm extracts features, not VT/LP/hybrid labels.

Configuration is in `processing.temporal_features`; disable with `processing.compute_temporal_features=false`.

## FloVoPy migration

The available `EnhancedStream` source already implements sample statistics, an energy proxy, spectral features, SAM/SEM and band ratios. Move the new `extract_temporal_features` into a reusable `EnhancedTrace`/`EnhancedStream` method after testing, avoiding duplicate calculations. A future enhancement should include a proper station-independent reference time, window-censoring flags, event-level multi-station aggregation, and reproducible feature schema/versioning.
