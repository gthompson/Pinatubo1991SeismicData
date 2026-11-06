# Pinatubo analysis notebook sequence

Run in `flovopy_env` with the updated editable FLOVOpy installation. Start notebooks from this directory or the project root. Edit **analysis_workflow.json** once. By default it selects four real files spread across May–August 1991; all products are explicitly scoped to that pilot. `PINATUBO_WAV_ROOT` or `wav_root` can override the known Box WAV/PNTBO location without probing the old external-drive configuration.

| Notebook | Main products |
|---|---|
| 00_archive_inventory_and_qc | Original-sample inventory, coverage, lengths, padding, gaps, extrema and artifact screens |
| 10_stationxml_and_network | Rebuilt historical maps/diagrams, chronology/timing ledger, draft XML and nominal-response audit |
| 20_pretrigger_and_window_selection | FLOVOpy proposals, full/zoom/frequency views, canonical manual review and validated intervals |
| 30_noise_and_event_metrics | Same applicable FLOVOpy amplitude, SAM/SEM, temporal/shape, spectral and cepstral features for both accepted window types |
| 40_spectra_and_spectrograms | Matched Welch/DPSS spectra, three Hann resolutions and normalized DPSS spectrograms, numerical arrays |
| 50_archive_summary_figures | Coverage/QC and accepted metrics by channel, window and gain epoch |
| 60_publication_figures | Fixed, explicitly draft selection, PNG/PDF/SVG, captions and asset hashes |

`legacy_rsam_analysis.ipynb` remains separate and is not overwritten. Acquisition cadence and independent clocks preclude automatic event/RSAM alignment. The older combined `00_archive_qc_and_pretrigger.ipynb`, command-line `pipeline.py` and legacy helpers are retained; the new sequence does not call the old pipeline or `temporal_features.py`.

## Shared code and provenance

`analysis_workflow.py` is a local orchestration/plotting layer. FLOVOpy owns onset proposals (`processing.detection`, also discoverable through VDAP), descriptive amplitude/SAM/SEM (`EnhancedStream`), shape/temporal/cepstral features (`core.features`), gap-aware Hann PSDs (`processing.spectrograms`) and PSD moments/band powers (`processing.spectral_metrics`). A local density-normalized DPSS estimator supports matched comparisons; the legacy multitaper plotting function is not treated as calibrated PSD.

Products go to `products/notebook_workflow/<run-id>`. Configuration, selected files' absolute paths/size/mtime, workflow code and relevant FLOVOpy source hashes define that ID. This separates changed inputs or processing assumptions; it does not overwrite previous manual reviews. Network source hashes are recorded separately. Size/mtime fingerprints detect ordinary source changes; they are not full waveform-content checksums. Absolute paths intentionally make runs machine-specific.

## Window review

Notebook 20 creates `window_review.csv` without accepting proposals. Inspect the full record and zoom, then edit accepted bounds or supply explicit `REVIEW_UPDATES`. File-relative intervals are half-open. Both source identity and bounds are checked, with a one-second default noise/event guard. Invalid overlaps, short/out-of-range windows, incomplete signal bounds and duplicate trace keys are rejected. Event ends must be inspected; there is no automatic record-end/coda pick. A complete noise-only interval may be accepted while leaving both signal bounds blank.

Accepted rows feed notebook 30. With no accepted windows, it writes an empty, fixed-schema table and useful review/QC products rather than inventing scientific decisions. Notebook 40 still shows labelled whole-record estimator comparisons. Threshold onset/duration on noise without an external reference stays missing. Changing the review CSV makes metric summaries stale until notebook 30 is rerun.

## Extending to the archive

Set `selection.mode` to `all` for all discovered files in WAV year/month folders. Remove `max_files`, or its presence will be labelled as limited scope. Discovery may include unreadable candidates; read failures remain in the file inventory. Computation is sequential per file. The full inventory is cached within a run, but the first full scan can take substantial time and is not performed by default. Full-run window detection/review can also be expensive. Validate thresholds and a labelled holdout before scaling acceptance; no labels are automatically inferred.

The pilot is selected for coverage across recording days, not as a representative statistical sample. Triggered records do not establish station uptime or unbiased earthquake rates. Trigger-preceding background is selected noise, not a continuous ambient survey. Recorded clocks are uncorrected, response electronics are unknown, epoch matching is provisional and physical cross-station amplitudes are not inferred.

## Spectra and publication

The comparison uses matched segment lengths and grids across each accepted pair. Welch averages Hann-segment density estimates; DPSS averages density-normalized taper periodograms over the same segments. Both are one-sided count²/Hz, not raw squared FFT magnitude. Approximate DPSS bandwidth is 2 NW/T. Spectrogram bins are complete windows only, with no padding/interpolation. Greater overlap smooths display rather than adding independent information. SAC MEM/MLM and reassignment remain follow-up methods, not implemented or validated in this first sequence.

`publication_selection.json` freezes explicit example keys, source fingerprints and plotting settings. Its initial status is DRAFT_REQUIRES_AUTHOR_SELECTION. It is not silently updated to another run; change it deliberately after scientific review. Final scientific selection, captions and journal-size visual review are still required. Historical maps remain the existing reconstruction; no ASL/FlowLoc refactor is introduced.

See **METRIC_SCHEMA.md** and the generated `metric_schema.csv` for units and conventions. Tests in `tests/test_analysis_workflow.py` cover interval validation, review integrity, source changes, PSD normalization and feature computation for both window types.

## Larger pretrigger batches and time plots

Notebook 20 now has visible selection controls. Set `FILES_PER_MONTH=50` for up to 200 evenly spread files across May–August; raise it to 100/250, or use `SAMPLE_MODE='all'` for the entire WAV archive (months are ignored in all mode). `MAX_FILES` applies a spread-out overall cap. The shared JSON selection is backed up and updated, so rerunning other notebooks uses the same selected files. Existing publication selections remain pinned to their original run. The old `pilot` mode remains available for one-file-per-selected-day sampling.

Per-file onset caches allow interrupted batches to resume. Only `MAX_EXAMPLE_FILES` spread-out records retain arrays for plots; combined QC/evidence and cache indexes are saved on disk. Large-batch frequency and Hilbert diagnostics are off by default; one detailed example is measured separately. These diagnostics do not control the onset algorithm. Read/analysis failures are retained as file-status rows, not zero-length windows.

`noise_windows_through_time.csv` contains per-trace lengths, statuses and recorded file times. `event_noise_window_summary.csv` contains per-file medians/10–90% spread after collapsing components to equal station votes, alongside earliest selected onset times. The time figure separates minimum-only assumptions and invalid outcomes from supported/shortened proposals. The guard and earlier transients can shorten a noise prefix; this plot does not recover the hardware/software pretrigger buffer. New proposals update the candidate columns while preserving explicit manual decisions in the canonical review file.

## Persistent event metrics (notebook 30)

`EnhancedEvent` JSON sidecars are now the authoritative full results, under `products/event_metrics_cache/<record-id>/<computation-signature>/event.json`, outside run-ID directories. Per-file signatures include the source fingerprint, accepted intervals, processing/PSD options, provisional epoch metadata, code/library identities and versions. Selection size and plotting options do not force unchanged events to be measured again. Original-sample inventory/QC is independently cached by source and audit/epoch signature. Explicit review decisions carry across changed selections by matching exact trace/source fingerprints; conflicting historical decisions are reported in `review_import_conflicts.csv`.

Notebook 30 keeps the existing `window_metrics.csv` as a fixed plotting view and adds `all_window_metrics.csv` with all fields dynamically exported from EnhancedCatalog. Arrays/lists are complete JSON-valued cells, not truncated representations. `events_metadata.csv` includes full event-wide metadata; `event_metrics_index.csv` selects the active sidecar version per recording. `events.qml` carries standard core event metadata; arbitrary waveform features remain in JSON. No earthquake origin or magnitude is inferred from file start time. Metadata-only records without accepted windows receive sidecars but no invented window features.

The full trees contain EnhancedStream descriptive metrics, SAM/SEM, amplitude-spectrum summaries/arrays, SSAM, band ratios, station summaries, the complete feature values/flags/provenance, PSD estimates/settings and pre/post artifact audits. New keys are automatically preserved/exported. Nonfinite values are JSON null. Full original computed short-band values remain available for audit alongside the masked plotting view and reliability flags. Rejected/undefined measurements are explicit, not replaced with zero.

Use `FORCE_RECOMPUTE=True` only to deliberately redo numerical work. The export-only option rebuilds combined outputs from cached sidecars without reading waveforms. `metric_provenance.json` reports computed/reused/metadata-only files. Review-note changes refresh metadata without numerical recomputation. If windows or source data change, only the affected record gets a new version; older versions are preserved.

Notebook 50 now includes an optional EventRate view using explicit recorded-file timestamps and no energy/magnitude inference. It counts selected waveform files, not earthquakes or continuous station uptime. The FLOVOpy guide is `flovopy/docs/enhanced_metric_sidecars.md`.

Per-file onset caches now live under `products/pretrigger_file_cache` and original-sample inventory caches under `products/file_inventory_cache`, also independent of selection/run IDs. Retained plotting examples may still be reread deliberately; cached batch records and unchanged metric events need no waveform recomputation.

## Automatic window policy: no per-event acceptance required

Notebook 30 now has `WINDOW_POLICY='fixed'` (default) or `'detected'`. It saves a `windows` section in the shared JSON and does not change your current archive selection. Fixed mode selects the first 5 seconds of each trace as noise and `[5 s, record end)` as event data, with no guard gap. It does not require notebook 20 or any manual decisions.

Detected mode uses each trace's selected onset and audited noise prefix, with a configurable default 1-second guard. `DETECTED_FALLBACK='fixed'` uses the fixed 5-second split when detection is unresolved, with explicit fallback flags; `'skip'` omits those traces. Existing compatible onset tables/caches are reused. Automatic policies never rewrite human review decisions; advanced `reviewed` mode retains that option.

`selected_windows.csv` and `window_selection_issues.csv` record the actual choices and skipped/short cases. Every metric row and sidecar records policy, method, fallback/quality flags and event-end rule. Event ends at record end are explicitly record-limited data windows, not automatically picked coda ends. Physical durations must be interpreted accordingly. Invalid/missing samples retain QC/rejection flags rather than being interpolated.

Restart the kernel after updating code. Run notebook 30, then 40 and 50. Notebook 20 is optional diagnostics in fixed mode; its selection control now defaults to `None` to preserve the shared full-archive selection instead of resetting it to the pilot. Publication notebook 60 still requires deliberate revision if its frozen selection belongs to an older run.

Full numerical results are much larger than metadata-only sidecars. Processing holds one recording's arrays at a time. Ensemble CSV export uses two passes over sidecars, preserving all fields/JSON-valued arrays without loading all spectra into memory. Event-level metadata CSV omits the duplicate trace payload list; all trace/window results remain in canonical JSON and the full trace export. Completed recordings are reusable after interruption; switching back to a prior policy can reuse its cached versions.
