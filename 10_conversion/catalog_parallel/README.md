# Parallel candidate-aware ObsPy catalogs

This workflow reads existing corrected pipeline products and writes separate trial products. It does not repick waveforms, relocate earthquakes, change clock rules, or replace production outputs.

## Run order

Use the existing `flovopy_asl` Python environment (ObsPy, pandas, NumPy, SciPy, matplotlib, Jupyter).

1. **49_identify_candidate_events.ipynb** groups existing picks and writes diagnostic proposals. Review its plots and tables first.
2. **50a_build_source_catalogs.ipynb** builds the five historical-source catalogs and an observations intermediate. `USE_CANDIDATE_ASSOCIATIONS=True` applies only supported, mutually unique proposals. Set False to use the earlier container-based association workflow.
3. **50b_merge_source_catalogs.ipynb** merges the saved catalogs and verifies conservation and QuakeML round trips.

All three notebooks must use the same `RUN_NAME`. The default is `<configured OUTPUT_ROOT>/catalog_parallel/candidate_trial_01/`. Choose a new name for each run; nonempty output directories are refused. A failed run can leave partial products, so use a new name after fixing a failure.

Launch from this directory, `10_conversion`, or the repository root. `PINATUBO_CODE_ROOT` can identify the repository explicitly. `PINATUBO_CATALOG_TRIAL_ROOT` overrides the trial root. Helpers are local to this directory. Production steps 40, 43, 50, and 80 remain unchanged.

## Scope and provisional thresholds

The scientific target is organization of surviving observations, not complete earthquake detection. Step 49 clusters all existing P picks within each existing observation window. It does not add picks or detect events without P picks. Original phase grouping is evidence rather than an event identity.

Initial settings are explicit and provisional:

- Adjacent-P gap: 2 seconds; total cluster span limited to 6 seconds to prevent chaining.
- Sensitivity gaps: 1.5, 2, and 3 seconds. Changed membership is flagged.
- Minimum independent stations: 2; channels at the same station do not supply independent station support.
- Repeated station P arrivals separated by more than 0.5 seconds, and historical groups split across clusters, require review. Duplicate near-simultaneous historical observations are retained.
- S picks attach only if their historical group identifies one candidate and a same-station P precedes them by 0–15 seconds. Other picks remain unresolved.
- Waveform support: 0.15-second RMS envelope; 90th-percentile envelope in −0.2 to +0.8 seconds around the existing P pick, divided by median envelope from −3 to −0.5 seconds. The initial ratio threshold is 3 at two or more stations. This is neither a calibrated SNR nor a probability.
- Missing channels, inadequate windows, flat/gapped traces, zero baselines, overlapping trace segments, and possible clipped plateaus are flagged. Coda and emergent arrivals can defeat this simple test. A weak score does not prove a pick is noise.
- Origin associations compare the existing step-43 preferred origin with candidate first-P times within −0.5 to +10 seconds. All temporal competitors are retained. Only mutually unique associations to supported candidates are proposed automatically. No travel-time model or origin regrouping is introduced.

The default reads a reproducible sample of **150** picked waveform windows and writes up to **30** review plots, mixing potentially difficult and ordinary cases. Set `max_waveforms=None` to check all picked waveform windows. Unchecked windows never enable automatic origin proposals. The clustering and its gap-sensitivity check run over all picks regardless of that limit.

An explicit read-once loop handles each selected waveform file. Envelopes are cached per channel; memory for plotting is bounded by the selected plot count. The waveform SHA-256 and read errors are recorded. All paths reference the existing SEISAN files; no waveform data are written.

## Diagnostic outputs

`candidates/` contains:

- `candidates.csv`: candidate ID, window, P-time range, independent stations, original pick groups, stability, flags and waveform-support status.
- `pick_membership.csv`: one row for **every** input pick, including previously unmapped picks and unresolved S picks. P membership does not imply a validated earthquake.
- `envelope_support.csv`: per-checked-P ratio, station, waveform channel, support flag and quality reason.
- `origin_associations.csv`: one decision per historical hypocenter group and all candidate alternatives.
- `origin_candidate_edges.csv`: every temporal origin/candidate possibility, including competitors rejected for automatic association.
- `sensitivity.csv`: candidate counts at each gap setting for each window; membership instability is also recorded on candidates.
- `waveform_audit.csv`: read successes, errors, paths and checksums.
- `review_queue.csv`: ambiguous or unsupported candidates with blank review fields. Unchecked windows are explicitly marked; they are not thousands of demonstrated failures.
- `plots/`: all channels with P/S markers, normalized envelopes, candidate bands and competing preferred-origin times. These are diagnostics, not evidence that every pick is correct.
- `manifest.json`: settings, input/product/code fingerprints, counts and decisions.

Copy review_queue.csv to a separate working file before editing it. The diagnostic bundle is immutable for downstream use: edited files or changed upstream indexes cause integration to stop. Review notes are not automatically applied as overrides in this implementation.

## Candidate-aware integration

Only supported, mutually unique proposals associate a historical hypocenter group with a pick candidate. There is no old shared-window fallback in this mode. A cluster needing review remains an unlocated candidate container; an unresolved historical group remains a separate historical event with candidate alternatives recorded. Consequently candidate-aware catalog container totals must **not** be interpreted as an earthquake count.

All original historical origins and magnitudes survive exactly once. Picks assigned to a cluster are kept on that candidate; other picks are retained in unresolved observation containers. The 1,413 picks absent from the previous step-32 map are now retained rather than dropped. Each pick includes membership provenance and its waveform-support diagnostic where measured.

One Event has zero or one portable waveform reference relative to `SEISAN/WAV` (e.g. `PNTBO/1991/06/filename`). Several events can reference the same file. Missing files are flagged, and historical solutions survive without waveforms. Actual Picks retain channel-specific WaveformStreamID; no artificial Arrivals or Picks are made.

## Catalog products and magnitude policy

`sources/` contains `hypo40.xml` (Pinatubo_all.sum), `pinaall.xml`, `gold.xml`, `brun.xml`, `pepi.xml`, `observations.xml`, association tables, and a manifest. Read each historical-source file directly with `obspy.read_events`; it contains only that source's origins and magnitudes. Actual picks enter through observations.xml during merging.

`merged/` contains catalog_combined.xml, event_manifest.csv, and verification.json. It preserves waveform-only and pick-only observation containers as well as candidate and located events.

Source records use stable source/file/line-derived IDs. Event IDs for historical groups derive from group membership; candidate IDs derive from the window and P-pick membership. Grouping changes therefore change their IDs. Every Origin/Magnitude preserves source file/line, raw and corrected time, reported magnitude, available quality fields, clock provenance and source hash in namespaced JSON. `catalog_workflow.get(obj)` decodes it after reading QuakeML.

Preferred origins follow step 43. Preferred magnitudes use BRUN, PINAALL, Pinatubo_all, GOLD, PEPI, independently of preferred origin. Values are untransformed and types remain unknown; this is not a homogeneous calibrated magnitude scale. Within-source ties are deterministic, not quality selections. The existing zero-as-missing policy is retained, with raw zeros preserved as provenance.

Exports cover the retained UTC indexes, not unresolved-clock or unlocated records excluded upstream. TESTALL and byte-identical aliases are not added as independent sources. Existing Nordic export (step 80) is not adapted or run; these trial outputs are not a drop-in replacement for its production pickle.

## Tests and practical next step

```sh
python -m unittest discover -s 10_conversion/catalog_parallel -p 'test_*.py' -v
```

Tests cover multiple events, station independence, span limits and gap sensitivity, ambiguous S picks, competing origins, noise/flat/clipped traces, unchecked-waveform restrictions, tampered input refusal, duplicate origins, missing magnitudes/waveforms, and QuakeML conservation. Export checks validate IDs, object references, scientific values, membership, and provenance across write/read/merge.

The next scientific step is a small visual review of the pilot plots and thresholds, followed by full waveform checking if the pilot is satisfactory. Passing tests establishes correct implementation and accounting, not correct earthquake identification or completeness.

## Executed pilot — 24 November 2026

The workflow functions were executed on the current complete indexes, with envelope diagnostics on a reproducible sample of 150 out of 7,715 picked waveform windows. The three notebook files were schema/syntax checked and their configuration cells executed; the real-data workflow ran through their underlying functions rather than a Jupyter kernel. All 11 regression tests passed.

Pilot results: 58,068 picks accounted for; 8,618 P-pick candidate clusters (not an earthquake count); 111 supported candidates; 89 mutually unique origin-association proposals. All 150 waveform reads succeeded, and 30 diagnostic plots were generated. The large unchecked-window category is expected in this sampled run.

Full source/merge export with those pilot proposals passed QuakeML schema, round-trip, reference and conservation checks: 15,692 Origins, 10,939 Magnitudes, 58,068 Picks, and 7,167 historical located groups. There are 30,029 total containers because unmatched historical groups, unlinked pick candidates, unresolved picks and waveform-only records are retained separately. Do not interpret that total as an earthquake count.

Products are in `/Users/thompsong/work/Pinatubo1991Processing/utc/catalog_parallel/candidate_verified_2026-11-24/`. `pilot_review.csv` restricts the review queue to actually checked waveform windows, with plot links where available. `tested_code/` preserves the tested implementation; `previous_parallel_code/` preserves the earlier parallel workflow.

Original production notebooks, existing trial outputs, and waveform files were not changed. The next step is visual pilot review before adopting thresholds or checking all waveforms.
