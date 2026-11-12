#!/usr/bin/env python3
"""CLI adapter to notebook 60: preview, then --extract to apply safe extraction."""
from pathlib import Path
from datetime import datetime, timezone
import argparse, hashlib, json
from pinatubo_steps.config import pipeline_paths

def run(config_file=None, *, extract=False):
    notebook=Path(__file__).with_name('60_prepare_bob_archives.ipynb')
    scope=dict(Path=Path,hashlib=hashlib)
    for cell in json.loads(notebook.read_text())['cells']:
        if 'pipeline_definitions' in cell.get('metadata',{}).get('tags',[]):
            exec(compile(''.join(cell['source']),str(notebook),'exec'),scope)
    paths=pipeline_paths(config_file=config_file)
    source=Path(paths['LEGACY_TOP'])/'BOB'
    report,remaining=scope['prepare_archives'](source,source.parent/'bob_zipfiles_obsolete',apply=extract)
    target=Path(paths['FAIR_TOP'])/'BOB/archive_preparation'
    target.mkdir(parents=True,exist_ok=True)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    path=target/f'{stamp}_{"applied" if extract else "preview"}.json'
    path.write_text(json.dumps(dict(applied=extract,archives=report,remaining=remaining),indent=2)+'\n')
    for row in report: print(row['status'],row['archive'])
    print('Report:',path,'Remaining ZIPs:',len(remaining))
    return report

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default=None)
    parser.add_argument('--extract',action='store_true')
    args=parser.parse_args()
    report=run(args.config,extract=args.extract)
    if any(r['status'] in ('failed_retained','retained_for_review') for r in report):
        raise SystemExit(1)
if __name__=='__main__': main()
