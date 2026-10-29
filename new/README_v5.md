# v5: Daily initial-window noise and event-detection diagnostics

This version extends v4 `summary_plots.py`. The waveform processing and `config.example.json` remain unchanged. The existing event and artifact audit plots are retained.

## Usage

Regenerate all plots from existing CSV files (no waveform reprocessing):

```bash
python summary_plots.py event_metrics_output
```

To generate **only the new daily noise products**, which is much faster:

```bash
python summary_plots.py event_metrics_output --daily-noise-only --dpi 140
```

The output directory must contain `trace_window_metrics.csv`, with `noise` and `event` rows and columns `file`, `trace_id`, `starttime`, `window`, `sample_rms`. `daily_station_counts.csv` is used if available, otherwise station coverage is calculated from event rows.

## New CSV outputs

- `daily_station_noise.csv`: per UTC date and SEED ID: number of initial windows, minimum/median/mean and P10/P90 noise RMS, number of events, minimum/P05/median event RMS, and median/P10 paired event-to-initial-window RMS ratios.
- `event_initial_window_pairs.csv`: per file and SEED ID, event and initial-window RMS and their ratio.
- `daily_network_noise_summary.csv`: descriptive network daily medians of station-level noise and minimum recorded event amplitudes.

## New figures

- `plots/<SEED_ID>_daily_noise.png`: daily noise median and P10–P90 envelope, minimum/P05 event RMS, paired event/initial-window RMS ratios, stations represented per day, and number of initial windows.
- `plots/daily_noise_heatmap.png`: daily median initial-window RMS for each station/channel.
- `plots/daily_network_noise_and_coverage.png`: descriptive network-wide noise/event lower envelope and station representation.

## Interpretation

- The first five seconds of a waveform are **assumed**, not independently verified, to be pre-trigger noise. The figure and CSV labels deliberately use “initial window”.
- RMS values are in **uncalibrated counts**, following the existing 25 Hz filtered processing path. Compare a station/channel only within consistent instrument gain and sensor epochs.
- The daily minimum of observed events is **not** a formal detection threshold. It depends on sample size and catalog completeness. The P05 is a complementary descriptive statistic, not an unbiased completeness estimator.
- Stations represented in event WAV files do **not** establish station uptime.
- Ratios are computed for event and initial-window measurements from the same file and channel before daily aggregation; they are not ratios of independent daily medians.
