#!/usr/bin/env python
"""Run the numbered notebooks in 10_conversion/ and 20_analysis/ in order.

Each notebook is executed with its own folder as the working directory. The executed
copy, with its outputs, is then saved under <output base>/executed_notebooks/<path> and
the repository copy is cleared of outputs (notebook_outputs.py), so committed notebooks
stay small; --keep-outputs leaves the executed notebook in place instead.
The run stops at the first failure unless --keep-going is given. A log and a
CSV summary go to <conversion OUTPUT_ROOT>/../run_logs/.

Examples (from the project root, in the flovopy_asl environment):

    python run_notebooks.py --dry-run                 # list what would run
    python run_notebooks.py                           # everything, conversion then analysis
    python run_notebooks.py --stage conversion --from 40 --to 54
    python run_notebooks.py --stage analysis --skip 45
    python run_notebooks.py --figures                 # then the paper and supplement figure notebooks
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from notebook_outputs import strip

ROOT = Path(__file__).resolve().parent
STAGES = {"conversion": ROOT / "10_conversion", "analysis": ROOT / "20_analysis"}
FIGURES = [ROOT / "papers/SRL/make_figures.ipynb", ROOT / "papers/SRL/supplement/make_supplement.ipynb"]
NUMBERED = re.compile(r"^(\d{2})_.*\.ipynb$")


def notebooks(stage: str) -> list[tuple[int, Path]]:
    found = []
    for p in sorted(STAGES[stage].glob("*.ipynb")):
        m = NUMBERED.match(p.name)
        if m:
            found.append((int(m.group(1)), p))
    return found


def plan(args) -> list[tuple[str, int, Path]]:
    stages = list(STAGES) if args.stage == "all" else [args.stage]
    skip = set(args.skip or [])
    out = []
    for stage in stages:
        for step, path in notebooks(stage):
            if args.start is not None and step < args.start:
                continue
            if args.end is not None and step > args.end:
                continue
            if f"{step:02d}" in skip or f"{stage}:{step:02d}" in skip:
                continue
            out.append((stage, step, path))
    if args.figures:
        out.extend(("figures", 99, f) for f in FIGURES)
    return out


def log_dir() -> Path:
    sys.path.insert(0, str(STAGES["conversion"]))
    try:
        from lib.config import pipeline_paths
        base = Path(pipeline_paths()["OUTPUT_ROOT"]).parent
    except Exception:  # configuration unavailable: keep logs beside the project
        base = ROOT
    d = base / "run_logs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def execute(path: Path, kernel: str, timeout: int) -> None:
    import nbformat
    from nbclient import NotebookClient

    nb = nbformat.read(path, as_version=4)
    client = NotebookClient(nb, kernel_name=kernel, timeout=timeout,
                            resources={"metadata": {"path": str(path.parent)}})
    try:
        client.execute()
    finally:
        nbformat.write(nb, path)  # keep outputs up to the failing cell for diagnosis


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stage", choices=["all", *STAGES], default="all")
    ap.add_argument("--from", dest="start", type=int, help="first step number to run (within each stage)")
    ap.add_argument("--to", dest="end", type=int, help="last step number to run (within each stage)")
    ap.add_argument("--skip", nargs="*", help="steps to skip, e.g. 45 or analysis:45")
    ap.add_argument("--figures", action="store_true", help="also run the paper and supplement figure notebooks at the end")
    ap.add_argument("--kernel", default="python3", help="Jupyter kernel name (default python3)")
    ap.add_argument("--timeout", type=int, default=-1, help="per-cell timeout in seconds (-1: none)")
    ap.add_argument("--keep-going", action="store_true", help="continue after a failing notebook")
    ap.add_argument("--dry-run", action="store_true", help="list the notebooks and exit")
    ap.add_argument("--keep-outputs", action="store_true",
                    help="leave outputs in the repository notebooks (default: archive the executed copy, then clear)")
    args = ap.parse_args()

    todo = plan(args)
    if not todo:
        print("Nothing selected.")
        return 1
    for stage, step, path in todo:
        print(f"{stage:10s} {step:02d}  {path.relative_to(ROOT)}")
    if args.dry_run:
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logs = log_dir()
    summary = logs / f"run_{stamp}.csv"
    text = logs / f"run_{stamp}.log"
    rows, failed = [], False
    with text.open("w") as log:
        for stage, step, path in todo:
            t0 = time.time()
            started = datetime.now(timezone.utc).isoformat(timespec="seconds")
            print(f"\n[{started}] {path.relative_to(ROOT)}", flush=True)
            status, error = "ok", ""
            try:
                execute(path, args.kernel, args.timeout)
            except Exception as exc:  # noqa: BLE001 - report any notebook failure
                status, error = "failed", f"{type(exc).__name__}: {str(exc).strip().splitlines()[-1] if str(exc).strip() else ''}"
                failed = True
            if not args.keep_outputs:
                _, copy = strip(path)
                if copy is not None:
                    print(f"executed copy: {copy}", flush=True)
            minutes = (time.time() - t0) / 60
            line = f"{started}  {status:6s}  {minutes:7.1f} min  {path.relative_to(ROOT)}  {error}"
            print(line, flush=True)
            log.write(line + "\n")
            log.flush()
            rows.append(dict(stage=stage, step=step, notebook=str(path.relative_to(ROOT)), started_utc=started,
                             minutes=round(minutes, 2), status=status, error=error))
            if failed and not args.keep_going:
                break
    with summary.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\nLog: {text}\nSummary: {summary}")
    if not args.keep_outputs:
        from notebook_outputs import archive_dir
        print(f"Executed notebooks: {archive_dir()}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
