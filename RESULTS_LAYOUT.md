# Reconciled external output layout

The laptop and Mac mini working copies have been combined without moving existing data.

- `10_conversion/conversion_config.json` remains authoritative for default conversion inputs and output_root, including all BOB products.
- `20_analysis/analysis_workflow.json` remains authoritative for analysis inputs and output_root.
- `pinatubo_project.py` is a compatibility facade for notebook runtime caches, testing, and historical artifact lookup; it does not silently substitute the Mac mini volume.
- `project_config.json` has no machine-specific defaults. Select `project_config.macmini.example.json` explicitly through PINATUBO_PROJECT_CONFIG to use that results layout, after checking its inputs and mounted volume.
- `PINATUBO_RESULTS_ROOT` selects isolated stage roots for testing. Conversion products, analysis products and runtime/test files all honor that override.
- No existing artifacts are moved or rewritten. A full pipeline rerun is a separate operation.

Hybrid diagnostics remain experimental, with independent settings and testing outputs. Configuration-backup files from the Mac mini are preserved in the original recovery archive, not promoted as active configuration. JOSS manuscript relocation from the laptop is retained.
