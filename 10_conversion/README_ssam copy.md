# Recovered SSAM and MUDFLOW code

Recovered unchanged from the earlier Pinatubo workspace implementation folder on 11 November 2026.

- audit_ssam_coverage.py reads SSAM.zip: dated ASCII rows with 16 numeric band values.
- audit_data_ssam_coverage.py reads DATA.zip using the same SSAM layout.
- audit_mudflow_coverage.py reads MUDFLOW.zip: dated ASCII rows with three numeric values.

These are working coverage-audit scripts, not completed reusable library readers. They parse values but retain coverage summaries rather than exporting all measurements. A row is considered nonmissing when at least one value is finite and not -998/-999. They preserve recorded calendar labels, make no timezone corrections and assume the station code is the second ZIP path component. They collect header text and parse/read errors.

The scripts contain original hard-coded ZIP paths and relative output folders. Do not run unchanged as a configured production pipeline. Syntax was checked during recovery; no original archive was reread and no existing audit output was overwritten. Refactoring should parameterize source/output paths, support unpacked folders, retain all per-band/channel measurements and flags, and remove the ZIP path-depth assumption.

No SSAM frequency-band boundaries or mudflow channel calibration are established by these coverage scripts; those require the archived headers/documentation. They do not produce spectrograms or calibrated mudflow measurements.

Existing audit outputs are in outputs/legacy-monitoring/{ssam,ssam-data-folder,mudflow} in the project review workspace. The code SHA-256 hashes and original locations are in recovery_manifest.json.
