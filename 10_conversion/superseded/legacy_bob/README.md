# Retired: `legacy_bob` command-line converter

Retired 18 Nov 2026. This package converted COSPEC, counts, radio maps, rain and tilt from the BOB archive to `OUTPUT_ROOT/legacy_bob` through `runner.run`. Conversion steps 61–69 replace it: they apply each acquisition PC's UTC rule from `clock_regimes.csv` and write QC-schema-2 products under `FAIR/BOB/<product>`. This package did not, and its modules differ slightly from the notebooks (−998 handling in counts, tilt labelled local time, the radio ditto column).

Kept for the record. Nothing imports it, and its tests (`tests/`) are not collected. They expect `legacy_bob` on the import path next to `pinatubo_steps`, so they do not run from here as they are.

The `legacy_bob` block in `conversion_config.json` is still used: step 66 reads its COSPEC units from it.
