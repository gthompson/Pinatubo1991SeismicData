# Trace-specific noise and pretrigger window strategy

The target is a conservative, reviewable noise prefix for each trace. An arrival time is not the acquisition trigger: these measurements cannot establish MDETECT's configured pretrigger buffer, or fix the recorded clock.

## Decision sequence

1. Preserve the source waveform and gaps. Exclude IRIG/empty traces. Measure reference-centred counts, RSAM, RMS, causal STA/LTA and separate frequency diagnostics. Treat the first five seconds as an explicit working assumption, not verified quiet data.
2. Check that reference for adequate finite coverage, flat leading padding, zero/unusable level and large isolated transients. Invalid reference means no proposed noise window. Previously zero-filled gaps and low-level seismicity may escape these checks.
3. Find all sustained strong amplitude and amplitude-supported STA/LTA candidates. Establish the earliest cluster within three seconds supported by at least two distinct network/station identities. Multiple components at one station count once. This establishes a candidate event, not necessarily an earthquake: shared interference can also coincide.
4. Within two seconds before to eight seconds after that cluster anchor, find each trace's earliest supported rise. Include weaker sustained amplitude increases, STA/LTA and changed one-second RSAM bins. A one-second bin's start is a conservative support bound, not a precise arrival pick. One-second/weak evidence cannot establish the network cluster alone. Coarse one-second evidence can only backtrack a fine/STA candidate by at most two seconds; it cannot independently create an arrival on a weak channel.
5. Propose noise from trace start to that onset/support bound minus one second. Audit the extension from five seconds onward for earlier sustained amplitude changes, strong isolated transients, gaps or incomplete bins. Shorten to the earliest limiting activity minus the same guard. Isolated weak fluctuations alone do not shorten a window. Negative or too-short results remain unresolved; do not silently force five seconds across the guard.
6. If a trace has no candidate in the network interval, retain only the assumed initial five seconds. Do not extend toward a late secondary burst. Without a network cluster, retain the minimum with an explicit unresolved status. A single-station recording therefore requires manual review rather than automatic extension.
7. Keep frequency evidence independent. Changes in band-limited centroid/PSD or signed Hilbert frequency flag questionable intervals; their absence does not prove quiet. Do not require a frequency shift to detect an earthquake. All proposals remain UNREVIEWED until manually accepted.

## Default screening parameters

Strong amplitude uses the larger of three times reference RSAM and reference RSAM plus six robust scatter estimates. Fine bins are 0.25 s and sustained runs last at least 0.5 s. Refinement uses 1.5 times reference level or reference plus three robust scatter estimates. Coarse one-second refinement uses scatter among the initial one-second measurements. These are configurable heuristics, not statistical significance levels.

STA/LTA uses 0.5/3 s windows and threshold 3. Network STA/LTA seeds also require a local median amplitude ratio of at least 1.3. The coincidence window is 3 s with at least two stations; the local search spans anchor minus 2 to plus 8 s. These bounds should be checked against station geometry and event classes before processing the full archive.

Initial transient screening compares the largest absolute sample to ten times the median absolute level or median plus ten robust spreads, whichever is larger. Extension transient screening uses four times the initial 99.5th percentile absolute amplitude. Real nonstationary noise may shorten a window; a flagged short or unresolved proposal is preferable to silently contaminating noise metrics.

## Hilbert diagnostic

Linearly detrend each finite contiguous run, apply SciPy Hilbert, unwrap phase and difference at sample midpoints. Retain signed frequency, including negative estimates; removing negatives biases summaries. Mask low-envelope samples, run edges and nonfinite estimates. A 0.25-s rolling median requires at least half its support, masks unreliable central samples and never crosses a true gap. Save raw and screened values, one-second valid coverage and negative fraction. Broadband mixed seismic/noise signals do not necessarily have a single physically meaningful instantaneous frequency. This is a diagnostic, not an onset picker.

## Review and validation

Inspect full-record and onset-zoom figures together, including every channel. Confirm the first five seconds, the earliest visible activity and the one-second guard. Check quiet-looking weak channels and detector disagreements; do not assume the high-amplitude candidate is the first arrival. Review conservative early-bin bounds and any shortened windows. Use `manual_window_review.csv` to record accepted intervals and reasons; accepted fields are deliberately blank. Event/coda ends remain unset and require a separate strategy.

`network_event_context.csv`, `onset_evidence.csv`, trace QC, one-second metrics, frequency summaries and compressed Hilbert samples preserve the evidence. Input/settings/version fingerprints separate runs and preserve manual review for repeated runs. Compare results only after checking the run manifest.

Tests cover distinct-station voting, weak/late channels, emergent rises, transient pulses, bad initial reference, conflicting guards, no-event recordings and signed Hilbert coverage. The four-month pilot is an exploratory demonstration, not an accuracy validation. Before production, manually label a stratified set spanning months, station/gain types, emergent/impulsive events, weak traces, spikes, gaps and multiple events. Reserve a separate holdout set; compare contamination and unresolved rates under threshold perturbations. Prioritize avoiding event energy in accepted noise over maximizing noise duration. Use a second reviewed clean noise interval when the initial prefix fails; do not automatically invent one in the event coda.

The desired event window should be trace-specific too, but onset-to-record-end is not a valid automatic event/coda window. Until coda-end validation is implemented, leave its end blank.

## FLOVOpy implementation

The authoritative engine is `flovopy.processing.detection.detect_pretrigger_window`, with `PretriggerSettings` in that module. `flovopy.core.vdap.detect_pretrigger_window` is a thin entry point to the same implementation, with identical settings/results. Generic code lives in `flovopy.processing.event_windows`; the local helper now contains only file selection, export/review orchestration and plots, plus compatibility imports for existing diagnostic tests.

Use the updated FLOVOpy checkout in the notebook kernel. Generic results use `_trace_s` and `_record_s` timing fields. The local export layer maps `_record_s` to the existing `_file_s` columns. New onset-support start/end columns describe the first measurement support, not statistical confidence or proof of a physical onset. Support is particularly useful for reviewing emergent signals; rockfall/PDC performance still needs labelled validation.

Other datasets may use a shorter initial reference and explicitly enable single-station mode with `network_min_stations=1`. Optional frequency/Hilbert diagnostics can be disabled in settings. Durations are rounded to whole samples, with actual fine/frequency window lengths recorded in QC. Invalid reference traces cannot seed a network event. Pinatubo keeps the five-second minimum, one-second guard and distinct-station defaults. New engine provenance creates a separate output directory; prior manual review files are preserved.
