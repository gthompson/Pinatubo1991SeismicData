# FloVoPy event features implementation

This directory contains a modified workspace copy of FloVoPy and an updated
Pinatubo v6 pipeline. The original Developer repositories, downloaded archives,
and synced project references were not modified.

- `flovopy/flovopy/core/features.py`: independent reusable extraction API.
- `flovopy/docs/event_features.md`: formulas, conventions, QC, compatibility audit.
- `flovopy/tests/test_features.py`: synthetic and pipeline regression tests.
- `pinatubo_features_v6/config.features.example.json`: enables new features,
  disables duplicate legacy temporal calculations.
- `validation/pinatubo_smoke/`: real Pinatubo waveform smoke-test tables.
- `flovopy-features.patch`: changes relative to the original local FloVoPy commit.

Use the workspace copy with PYTHONPATH pointing to its `flovopy` directory, or
apply the patch in the FloVoPy repository with `git apply /path/to/flovopy-features.patch`.
The patch does not install dependencies. NumPy, SciPy, ObsPy, pandas and pytest
are required for tests. The tested environment was flovopy_asl (NumPy 2.4.6,
SciPy 1.18.0). The full v6 pipeline is included alongside the patch.

Run tests from the FloVoPy directory: `python -m pytest tests/test_features.py`.
Run the pipeline with `python pipeline.py --config config.features.example.json`,
after adjusting input/output paths to the desired dataset. Old configuration
files leave new features disabled; original temporal_features.py is unchanged.

Validation: 16 tests passed. A June 3, 1991 file produced 9 traces, 18 window rows,
and 9 feature QC rows with no processing errors. The five-second noise window
is too short for reliable LF threshold metrics under the default conservative
criterion. See the definitions document for that limitation and other QC flags.
No labeled classification validation or full archive reprocessing was performed.
