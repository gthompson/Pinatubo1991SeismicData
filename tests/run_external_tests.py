#!/usr/bin/env python3
"""Run conversion, network and analysis tests with artifacts on the results disk."""
from pathlib import Path
from datetime import datetime, timezone
import contextlib
import json
import os
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT), str(PROJECT / '20_analysis'), str(PROJECT / '10_conversion')]
from pinatubo_project import test_directory, runtime_environment


def main():
    run = test_directory('runs', datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    os.environ['PINATUBO_RESULTS_ROOT'] = str(run / 'sandbox')
    runtime_environment()
    temporary = run / 'tmp'
    temporary.mkdir()
    os.environ['TMPDIR'] = str(temporary)
    tempfile.tempdir = str(temporary)
    os.environ['MPLBACKEND'] = 'Agg'
    successful = False
    with (run / 'test_report.txt').open('w') as log, contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
        try:
            from network_history.reconstruct import build_all
            build_all()
            suite = unittest.TestSuite()
            for directory in [PROJECT / '20_analysis/tests', PROJECT / '10_conversion/pinatubo_steps/tests']:
                suite.addTests(unittest.TestLoader().discover(str(directory), pattern='test_*.py', top_level_dir=str(directory)))
            directory = PROJECT / '10_conversion/network_history'
            suite.addTests(unittest.TestLoader().discover(str(directory), pattern='test_reconstruction.py', top_level_dir=str(directory)))
            result = unittest.TextTestRunner(stream=log, verbosity=2).run(suite)
            successful = result.wasSuccessful()
            (run / 'summary.json').write_text(json.dumps(dict(tests=result.testsRun, failures=len(result.failures), errors=len(result.errors)), indent=2))
        except Exception:
            import traceback
            traceback.print_exc()
    print(('PASS' if successful else 'FAIL') + ': ' + str(run / 'test_report.txt'))
    return 0 if successful else 1


if __name__ == '__main__':
    raise SystemExit(main())
