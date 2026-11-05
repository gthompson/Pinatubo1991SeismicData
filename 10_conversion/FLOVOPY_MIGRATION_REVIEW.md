# FLOVOpy migration review — selected scope

Updated November 3, 2026.

## Completed earlier

Generic time association, structural SEED-ID splitting and ObsPy comment lookup live in flovopy.core.event_utils. Local shared.py holds pipeline table/plot conventions and XB fallback IDs. Steps 40/41 and step 50 have consolidated notebooks.

## Selected and implemented

- **Single-event VDAP PHA ingestion:** parse_vdap_phase_line/read_vdap_pha_file in flovopy.core.vdap. Fixed-column and tokenized records, authoritative legacy ID decoding, raw file/line provenance, explicit diagnostics and real historical regression excerpts. I/O errors raise. Multi-event 10/100 delimiters are rejected. Step 20 uses this reader while retaining local deduplication/outlier policy. Monthly/yearly parsing is unchanged.
- **Catalog-to-REA export:** flovopy.seisanio.rea_export.export_catalog_to_rea. Uses public ObsPy Nordic writing without changing the working directory. Copies source events, accepts caller-defined metadata/anchor/waveform policies, creates unique deterministic anchor IDs, writes standard S-file names and records reuse, failures and optional filename shifts in a manifest. Step 52 is a thin local policy adapter. Both OLD and NEW Nordic formats are tested.
- **Step 40:** authoritative vdap.parse_hypo71_line fixes arithmetic rollover and allows blank/nonnumeric optional quality fields, retaining raw quality text. A local adapter preserves existing CSV names/types and raw provenance. No second HYPO71 parsing implementation remains in the active step.
- **Step 43:** direct ObsPy locations2degrees/degrees2kilometers calls replace the local wrapper. This keeps the existing spherical convention; it does not silently switch to ellipsoidal distances.

## Deliberately not selected

Representative-event-time promotion, generic pick deduplication/filtering, table-to-ObsPy builders and diagnostic migration are not part of this work. Time-anchor selection for Pinatubo Nordic export remains a local policy, not a new generic representative-time API.

## Policies that remain local

Monthly/yearly multi-event PHA formats and delimiters; original/annotated PINAALL parsing and historical time conventions; source priority and deduplication; event classes; trusted coverage windows; default XB network, paths and notebook orchestration.

## Export contract and remaining limitations

- Stable source event resource IDs and deterministic caller metadata are needed for repeat-run identity. A hidden manifest fingerprints prepared events/settings and checks exact output hashes before reuse.
- Default collision='error' reports conflicts. collision='shift' explicitly permits bounded filename-only shifts using valid standard names; origin/pick times are unchanged. Filename-based daily counts can differ at midnight. Every offset is reported.
- Existing untracked S-files are never overwritten, even with overwrite=True. Start a fresh export root when migrating an old archive. Only tracked files can be updated; changed origin times require a new export root to avoid stale filenames.
- Concurrent writers are unsupported. If a write succeeds but manifest persistence fails, inspect the reported failure and untracked output rather than blindly overwriting it.
- A timeless event is skipped with a reason. Per-event failures are returned; review them before downstream analysis. Optional select_all.out contains successful/reused exports from this invocation only.
- Real historical test excerpts establish the supported single-event PHA and HYPO71 dialects, not all possible VDAP variants. Strict PHA parsing now exposes malformed records instead of silently losing them.

No full historical archive conversion or scientific-product regeneration was performed as part of this migration.
