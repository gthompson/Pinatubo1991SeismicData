# Superseded conversion code

Kept for the record. Nothing in the current notebooks imports it, and the test suite does not collect it. Paths and imports assume the files sat in `10_conversion`, so they do not run from here as they are.

| Item | Retired | Replaced by |
|---|---|---|
| `bob_pre_flovopy/` | Nov 2026 | Steps 60–69 |
| `legacy_bob/` (with its README) | 18 Nov 2026 | Steps 61–69 |
| `early_audits/`: SSAM, SSAM DATA and mudflow coverage scripts and their README | 19 Nov 2026 | Steps 62 and 65. The scripts read hard-coded Box ZIP paths and apply no clock rules. |
| `refactor_manifest.json` | 19 Nov 2026 | Record of a script-to-module refactor that was later undone; the modules it lists no longer exist. |
| `unpack_bob_archives.py` | 19 Nov 2026 | Step 60 (`60_prepare_bob_archives.ipynb`), whose definitions it ran from the command line. |
| `reviews/24_phase_pick_review.md`, `reviews/45_hypocenter_review.md` | 19 Nov 2026 | Written reviews of steps 20–23 and 40–44, snapshots of 11 Nov on the pre-UTC run (`converted/`). Their findings, and what was fixed, are in `CODE_REVIEW_2026-11-17.md` §3–§4 and `TIMING_AND_CLOCKS.md`. |
