#!/usr/bin/env python
"""Keep executed notebooks beside the output data and commit them without outputs.

Executed notebooks can be far larger than GitHub accepts (step 35 is over 100 MB with
outputs), so the repository holds notebooks with their outputs cleared, and the executed
copy of each is kept under <output base>/executed_notebooks/<path in the repository>, the
same base as run_logs/ (by default ~/work/Pinatubo1991Processing). run_notebooks.py does
this after every notebook it runs; this script does it by hand and checks commits.

    python notebook_outputs.py strip [NOTEBOOK ...]   # archive executed copies, then clear outputs
    python notebook_outputs.py strip --no-archive ... # clear outputs without archiving
    python notebook_outputs.py check [NOTEBOOK ...]   # exit 1 if any notebook has outputs
    python notebook_outputs.py install-hook           # git pre-commit hook running `check` on staged notebooks

Without NOTEBOOK arguments, every notebook tracked by git is used. Only the standard library
is needed, so the hook works outside the project environment. A notebook is rewritten only
when it has outputs, execution counts or execution timing metadata.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
HOOK = """#!/bin/sh
# Installed by notebook_outputs.py: refuse commits of notebooks that still have outputs.
files=$(git diff --cached --name-only --diff-filter=ACM -- '*.ipynb')
[ -z "$files" ] && exit 0
exec python3 notebook_outputs.py check --staged $files
"""


def output_base() -> Path:
    """<conversion OUTPUT_ROOT>/.. (where run_logs/ also goes), or the default work folder."""
    sys.path.insert(0, str(ROOT / "10_conversion"))
    try:
        from lib.config import pipeline_paths
        return Path(pipeline_paths()["OUTPUT_ROOT"]).parent
    except Exception:  # noqa: BLE001 - configuration unavailable (e.g. in the git hook)
        return Path("~/work/Pinatubo1991Processing").expanduser()


def archive_dir() -> Path:
    return output_base() / "executed_notebooks"


def tracked_notebooks() -> list[Path]:
    out = subprocess.check_output(["git", "ls-files", "*.ipynb"], cwd=ROOT, text=True)
    return [ROOT / p for p in out.splitlines()]


def dirty(nb: dict) -> bool:
    for cell in nb.get("cells", []):
        if cell.get("outputs") or cell.get("execution_count") is not None or "execution" in cell.get("metadata", {}):
            return True
    return False


def stripped(nb: dict) -> dict:
    nb = json.loads(json.dumps(nb))
    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        cell.get("metadata", {}).pop("execution", None)
    nb.get("metadata", {}).pop("widgets", None)
    return nb


def write(nb: dict, path: Path) -> None:
    path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


def archive(path: Path, dest_root: Path | None = None) -> Path:
    """Copy an executed notebook to <archive>/<path in repo>; returns the copy's path."""
    path = Path(path).resolve()
    dest = (dest_root or archive_dir()) / path.relative_to(ROOT)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dest)
    return dest


def strip(path: Path, *, keep_copy: bool = True, dest_root: Path | None = None) -> tuple[bool, Path | None]:
    """Archive (optionally) and clear one notebook's outputs: (cleared, archived copy or None)."""
    path = Path(path)
    nb = json.loads(path.read_text(encoding="utf-8"))
    if not dirty(nb):
        return False, None
    copy = archive(path, dest_root) if keep_copy else None
    write(stripped(nb), path)
    return True, copy


def staged_dirty(path: str) -> bool:
    blob = subprocess.check_output(["git", "show", f":{path}"], cwd=ROOT)
    return dirty(json.loads(blob))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("strip", help="archive executed copies, then clear outputs")
    s.add_argument("notebooks", nargs="*", type=Path)
    s.add_argument("--no-archive", action="store_true", help="do not keep an executed copy")
    c = sub.add_parser("check", help="exit 1 if any notebook has outputs")
    c.add_argument("notebooks", nargs="*")
    c.add_argument("--staged", action="store_true", help="check the versions staged for commit")
    sub.add_parser("install-hook", help="install the git pre-commit check")
    args = ap.parse_args()

    if args.cmd == "install-hook":
        hook = Path(subprocess.check_output(["git", "rev-parse", "--git-path", "hooks"], cwd=ROOT, text=True).strip())
        hook = (ROOT / hook if not hook.is_absolute() else hook) / "pre-commit"
        if hook.exists() and "notebook_outputs.py" not in hook.read_text():
            print(f"{hook} exists and is not ours; add `python3 notebook_outputs.py check --staged` to it by hand.")
            return 1
        hook.write_text(HOOK)
        hook.chmod(0o755)
        print(f"installed {hook}")
        return 0

    if args.cmd == "check":
        paths = args.notebooks or [str(p.relative_to(ROOT)) for p in tracked_notebooks()]
        bad = [p for p in paths if (staged_dirty(p) if args.staged else dirty(json.loads((ROOT / p).read_text())))]
        for p in bad:
            print(f"has outputs: {p}")
        if bad:
            print("Clear them with: python notebook_outputs.py strip " + " ".join(bad))
        return 1 if bad else 0

    paths = args.notebooks or tracked_notebooks()
    n = 0
    for p in paths:
        cleared, copy = strip(p, keep_copy=not args.no_archive)
        if cleared:
            n += 1
            print(f"cleared {p}" + (f" (executed copy: {copy})" if copy else ""))
    print(f"{n} notebook(s) cleared; executed copies under {archive_dir()}"
          f" ({datetime.now(timezone.utc).isoformat(timespec='seconds')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
