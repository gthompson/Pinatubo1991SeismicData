# Generic FloVoPy event metrics pipeline (prototype v0.1)

This is a **generic**, non-Pinatubo-specific processor. It reads event waveform
files from a SEISAN `WAV/<DB>/<YEAR>/<MONTH>` tree or file glob patterns.
It audits a configurable narrowband artifact on the **unfiltered** data, then
uses **ObsPy `Stream.filter('lowpass', ...)`** on a copy of each full trace.
Finally it computes metrics for an initial background window and a subsequent
event window using `EnhancedStream.ampengfft(allow_uncalibrated=True)`.

## Requirements

Python 3.10+, `obspy`, `numpy`, `scipy`, `pandas`, and an installed FloVoPy
version containing the revised counts-enabled `EnhancedStream` and spectral
module. No instrument response correction is performed. No input file is
modified. Install dependencies in your existing `flovopy_plus` environment.

## Quick start

1. Copy `config.example.json` to `config.json` and edit `input.seisan_root`,
   date range, and `output.directory`.
2. Run a pilot:

   ```bash
   python pipeline.py --config config.json --max-files 5 --verbose
   ```
3. Run the whole configured period:

   ```bash
   python pipeline.py --config config.json
   ```

For arbitrary files, change `input.mode` to `files` and set e.g.
`"paths": ["/data/events/**/*.mseed"]`. Glob patterns are recursive.

## Outputs

* `artifact_audit.csv`: **one row per trace**, measured before filtering.
  Contains estimated PSD peak frequency around `target_hz`, peak/background
  ratio, detection flag, PSD fraction in the search band, and testability status.
  The detection threshold is heuristic; `not_testable` is not `false`.
* `trace_window_metrics.csv`: one row per **trace and window**, with flattened
  FloVoPy metrics, including amplitude statistics, SAM/SEM, band ratios and
  power-weighted mean frequency (depending on installed FloVoPy version).
* `qc_log.csv`: per-trace/window success or error.
* `files_processed.csv`: file-level success/errors.
* `config_used.json`: exact configuration used for reproducibility.
* `filtered_mseed/`: optional output; disabled by default. Uses FLOAT64
  MiniSEED encoding to retain processed samples without quantization.

**Important:** the first 5 s is assumed to be background *for now*. It is not
validated as pre-event noise and can include a trigger, earthquake, or another
signal. `noise_guard_seconds` shortens the end of that interval, and
`event_start_seconds` defines the event window start. This pipeline does not
perform onset picking or read REA event origin times. Noise sampling is
**event-triggered**, not a continuous ambient-noise measurement.

## Filtering and audit

* The artifact audit uses SciPy Welch PSD **before filtering**, with a local
  background median and configurable frequency window. Results can vary with
  record length, sampling rate and spectral resolution. The observed 40.45 Hz
  peak is not presumed to exist in every dataset or trace.
* Default 25 Hz, six-corner, zero-phase Butterworth low-pass is performed with
  ObsPy `Stream.filter()`. For 100 Hz data this strongly suppresses ~40 Hz
  contamination. It is **not** mathematically zero, and the filter can also
  attenuate legitimate energy near 20–25 Hz. Test the filter response before
  treating 4–20 Hz metrics as quantitatively unbiased.
* Filtering is applied to the **entire trace** before window splitting to
  reduce window-edge effects. Filtering still has end-of-record transients.
* The original waveform is preserved. The output measures count-based
  amplitudes/energy proxies, **not physical ground velocity or energy**.
* Station-dependent sensor response, attenuation and telemetry gain are
  **not applied**. The separately reconstructed station metadata can be
  joined in a later calibration step.
* Default low/high analysis bands are 0.8–4 Hz and 4–20 Hz.
* At present, the full-waveform mean frequency is calculated **after** the
  low-pass; the high-frequency artifact is captured separately in the audit.

## Known limitations / planned improvements

* CSV results are accumulated in memory until completion. For very large
  archives, add incremental/atomic writing or Parquet partitioning.
* SEISAN mode screens by filename date and reads all matching files. It does
  not join REA metadata; a later stage can use `SeisanArchive` for that.
* Does not repair gaps, overlaps, clipped channels or time corrections. It
  logs invalid traces; add gap/clipping QC before production analysis.
* Filters require the low-pass frequency to be below each trace's Nyquist;
  incompatible traces are logged as errors rather than silently resampled.
* Low-frequency band filtering and spectral estimates can be unreliable for
  very short records. Increase `minimum_window_seconds` if needed.
* No formal regression tests have been executed in this delivery environment
  because ObsPy/FloVoPy are not installed here. Syntax compilation was checked.

## API

`run(config, max_files=...)`, `process_file(path, config)`, `audit_trace(trace,
config['artifact_audit'])`, and `preprocess(stream, config['processing'])` can
be imported by later FloVoPy code. The script itself is independent of Pinatubo.
