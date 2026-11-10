"""Read and plot preserved Pinatubo BOB COSPEC records without aggregation.

Run: python cospec_plotter.py --config cospec_config.json
Needs pandas and matplotlib, but does not import FLOVOpy or run waveform analysis.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sys

import pandas as pd

_PROJECT = Path(__file__).resolve().parent.parent
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))
from pinatubo_config import read_config, resolve_path, analysis_products_root, external_output_root

MONTHS = {name: i for i, name in enumerate(
    ('JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'), 1)}
# Minute has fixed width: accepts both '00:00  493.0' and '00:0013361.0'.
RECORD = re.compile(r'^\s*(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4}|\d{2})\s+'
                    r'(\d{2})[: ](\d{2})\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[EeDd][+-]?\d+)?)\s*$')
COLUMNS = ['record_id','source_line','recorded_time_gmt','cospec_value',
           'duplicate_timestamp','midnight_time','negative_value','raw_line']


def read_cospec(path, *, strict=True, two_digit_year_century=1900):
    """Preserve each source observation and its line; GMT is the file's stated basis.

    Midnight tags are retained without claiming an actual midnight measurement.
    Repeated dates/times are flagged, never averaged or silently deduplicated.
    """
    path = Path(path).expanduser().resolve()
    raw = path.read_bytes()
    rows, headers, errors = [], [], []
    for lineno, original in enumerate(raw.decode('cp1252').splitlines(), 1):
        line = original.replace('\x1a', '').strip()
        if not line:
            continue
        match = RECORD.fullmatch(line)
        if not match:
            if not rows and not re.match(r'^\d', line):
                headers.append(line)
                continue
            errors.append({'source_line':lineno, 'raw_line':original, 'reason':'unrecognized record'})
            continue
        try:
            day, month, year, hour, minute, value = match.groups()
            year = int(year) + (two_digit_year_century if len(year) == 2 else 0)
            timestamp = datetime(year, MONTHS[month.upper()], int(day), int(hour), int(minute), tzinfo=timezone.utc)
            value = float(value.replace('D','E').replace('d','e'))
            if not math.isfinite(value):
                raise ValueError('nonfinite measurement')
        except (ValueError, KeyError) as exc:
            errors.append({'source_line':lineno, 'raw_line':original, 'reason':str(exc)})
            continue
        rows.append(dict(record_id=f'COS_PEC:{lineno}',source_line=lineno,
            recorded_time_gmt=timestamp,cospec_value=value,duplicate_timestamp=False,
            midnight_time=timestamp.hour == 0 and timestamp.minute == 0,
            negative_value=value < 0,raw_line=original))
    if errors and strict:
        raise ValueError(f'{path}: {len(errors)} invalid record(s): {errors}')
    if not rows:
        raise ValueError(f'{path}: no COSPEC observations found')
    result = pd.DataFrame(rows, columns=COLUMNS)
    result['recorded_time_gmt'] = pd.to_datetime(result['recorded_time_gmt'], utc=True)
    result['duplicate_timestamp'] = result.recorded_time_gmt.duplicated(keep=False)
    result.attrs.update(source_file=str(path),source_sha256=hashlib.sha256(raw).hexdigest(),
        source_headers=headers,parse_errors=errors,two_digit_year_century=two_digit_year_century,
        time_basis='GMT as stated in the source; not independently calibrated',
        units_in_source=False)
    return result


def read_bob_plot_definitions(path):
    """Inventory BOB series/file/Fortran descriptors; do not execute their formats."""
    raw = Path(path).read_bytes()
    rows = []
    for lineno, original in enumerate(raw.decode('cp1252').splitlines(), 1):
        line=original.replace('\x1a','').strip()
        match=re.fullmatch(r'(\S+:\S+)\s+(\S+)\s+(\(.*\))',line)
        if match:
            rows.append(dict(bob_series=match[1],source_file=match[2],
                fortran_format=match[3],source_line=lineno))
    return pd.DataFrame(rows, columns=['bob_series','source_file','fortran_format','source_line'])


def plot_cospec(records, output_dir, *, units='legacy units',
                units_basis='Not specified in the source file.', reference_date='1991-06-15',
                zoom_start='1991-05-10', zoom_end='1991-06-17', dpi=220):
    """Scatter plots retain irregular sampling and duplicate times; no interpolation."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(2,1,figsize=(11,7),layout='constrained')
    reference = pd.Timestamp(reference_date, tz='UTC') if reference_date else None
    frame=records.sort_values(['recorded_time_gmt','source_line'], kind='stable')
    zoom_a, zoom_b = pd.Timestamp(zoom_start,tz='UTC'), pd.Timestamp(zoom_end,tz='UTC')
    panels = [(axes[0], frame, '(a) All recovered observations'),
              (axes[1], frame[frame.recorded_time_gmt.between(zoom_a,zoom_b)], '(b) May–June detail')]
    for ax, part, title in panels:
        ordinary=part[~part.duplicate_timestamp]; repeated=part[part.duplicate_timestamp]
        ax.scatter(ordinary.recorded_time_gmt,ordinary.cospec_value,s=35,color='#225e86',
                   zorder=3,label='Source records')
        if len(repeated):
            ax.scatter(repeated.recorded_time_gmt,repeated.cospec_value,s=55,facecolors='none',
                       edgecolors='#b94f24',linewidths=1.4,zorder=4,label='Repeated timestamp (both retained)')
        if reference is not None:
            ax.axvline(reference,color='#777777',ls='--',lw=.9,label='June 15 date marker')
        ax.set_title(title,loc='left',fontsize=11)
        ax.set_ylabel(f'COSPEC value ({units})');ax.grid(alpha=.18)
        ax.set_ylim(bottom=min(0,float(part.cospec_value.min())*.95) if len(part) else 0)
        ax.spines[['top','right']].set_visible(False)
    axes[0].xaxis.set_major_locator(mdates.MonthLocator())
    axes[0].xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    axes[0].legend(fontsize=8,loc='upper right')
    axes[1].set_xlim(zoom_a,zoom_b)
    axes[1].xaxis.set_major_locator(mdates.DayLocator(interval=5))
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    for _, row in frame[frame.duplicate_timestamp].iterrows():
        if zoom_a <= row.recorded_time_gmt <= zoom_b:
            axes[1].annotate(f'{row.cospec_value:g}',(row.recorded_time_gmt,row.cospec_value),
                             xytext=(9,7),textcoords='offset points',fontsize=8,color='#9b421e')
    axes[1].set_xlabel('Recorded date (source states GMT; all supplied time tags are 00:00)')
    fig.suptitle(f'Pinatubo COSPEC legacy series — {len(frame)} records on {frame.recorded_time_gmt.dt.date.nunique()} dates',fontsize=13)
    fig.supxlabel('Points only; missing days are not zeros. Units: '+units_basis,fontsize=8)
    paths=[]
    for suffix in ['png','pdf','svg']:
        path=output_dir/f'cospec_overview.{suffix}'
        fig.savefig(path,dpi=dpi,bbox_inches='tight');paths.append(path)
    plt.close(fig)
    return paths


def run(config_file=None, *, output_root=None):
    """Compatibility entry point: configuration now means conversion_config.json."""
    if output_root is not None:
        raise ValueError('Set output_root in conversion_config.json instead')
    conversion=Path(__file__).resolve().parents[1]/'10_conversion'
    if str(conversion) not in sys.path:sys.path.insert(0,str(conversion))
    from legacy_bob.runner import run as convert
    destinations=convert(config_file,products=['cospec'])
    destination=Path(destinations['cospec'])
    return {'output_dir':destination,'metadata':json.loads((destination/'cospec_metadata.json').read_text())}

def main(argv=None):
    parser=argparse.ArgumentParser(description='COSPEC compatibility entry point; uses conversion_config.json')
    parser.add_argument('--config',type=Path,default=None,help='Shared conversion_config.json')
    args=parser.parse_args(argv)
    print('Output:',run(args.config)['output_dir'])
if __name__=='__main__':main()
