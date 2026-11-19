"""Step 71's controlled reconstruction checks, run with the rest of the conversion tests.

They rebuild the channel epochs from the station evidence (LEGACY/STATION_METADATA) and write
temporary StationXML; production network-history products are never touched.
"""
import json
import sys
from pathlib import Path

PIPELINE = Path(__file__).resolve().parents[1]
if str(PIPELINE) not in sys.path:
    sys.path.insert(0, str(PIPELINE))

_notebook = PIPELINE / '71_validate_network_history.ipynb'
_namespace = {'PIPELINE': PIPELINE, '__name__': 'step71_definitions'}
for _cell in json.loads(_notebook.read_text())['cells']:
    if _cell['cell_type'] == 'code' and 'pipeline_definitions' in _cell.get('metadata', {}).get('tags', []):
        exec(compile(''.join(_cell['source']), str(_notebook), 'exec'), _namespace)

ReconstructionTests = _namespace['ReconstructionTests']
