"""Compile daily BOB RAIN files without clock correction or calibration."""
from pathlib import Path
from datetime import datetime
from collections import Counter
import csv,hashlib,json,re,zipfile

MONTHS={m:i for i,m in enumerate('JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC'.split(),1)}
RECORD=re.compile(r'^\s*(\d{1,2})[ -]([A-Za-z]{3})[ -](\d{2}|\d{4})\s+(\d{1,2})[: ]\s*(\d{1,2})\s+(.+?)\s*$')

def sources(source):
    source=Path(source)
    if source.is_file():
        with zipfile.ZipFile(source) as archive:
            return [(n,archive.read(n)) for n in sorted(archive.namelist())
                    if not n.startswith('__MACOSX/') and Path(n).suffix.upper()=='.DAT']
    return [(p.name,p.read_bytes()) for p in sorted(source.iterdir()) if p.is_file() and p.suffix.upper()=='.DAT']

def run(source,output):
    """One wide combined CSV; positional raw values and missing flags retained."""
    entries=sources(source)
    if not entries:raise ValueError('No daily RAIN .DAT files found')
    rows=[];inventory=[]
    for name,data in entries:
        headers=[];file_rows=[]
        for number,line in enumerate(data.decode('cp1252').splitlines(),1):
            clean=line.replace('\x1a','').strip()
            if not clean:continue
            if not clean[0].isdigit():headers.append(clean);continue
            row={'source_file':name,'source_line':number,'recorded_time':'','raw_date_time':'',
                 'time_basis':'unresolved','field_count':0,'quality_flags':'','duplicate_timestamp':False,
                 'source_header':'','raw_line':line}
            flags=[];match=RECORD.fullmatch(clean);values=[]
            if not match:flags.append('unparsed_record')
            else:
                day,month,year,hour,minute,tail=match.groups()
                row['raw_date_time']=clean[:match.start(6)].rstrip()
                tokens=tail.split();row['field_count']=len(tokens)
                try:
                    y=int(year)+(1900 if len(year)==2 else 0)
                    stamp=datetime(y,MONTHS[month.upper()],int(day),int(hour),int(minute))
                    row['recorded_time']=stamp.isoformat()
                    if y!=1991:flags.append('unexpected_year')
                    expected=Path(name).stem.upper()
                    if expected!=f'{int(day):02d}{month.upper()}{str(y)[-2:]}':flags.append('filename_date_mismatch')
                except (ValueError,KeyError):flags.append('invalid_datetime')
                for token in tokens:
                    try:values.append(float(token))
                    except ValueError:values.append(None);flags.append('invalid_numeric_value')
                if len(values)!=8:flags.append('unexpected_field_count')
            for i,value in enumerate(values,1):
                row[f'value_{i:02d}_raw']=value
                row[f'value_{i:02d}_missing']=value==-998 if value is not None else True
            row['quality_flags']=';'.join(sorted(set(flags)))
            file_rows.append(row)
        header=' | '.join(headers)
        for row in file_rows:row['source_header']=header
        inventory.append({'source_file':name,'sha256':hashlib.sha256(data).hexdigest(),'records':len(file_rows),'headers':headers})
        rows.extend(file_rows)
    duplicates=Counter(r['recorded_time'] for r in rows if r['recorded_time'])
    for row in rows:row['duplicate_timestamp']=bool(row['recorded_time'] and duplicates[row['recorded_time']]>1)
    rows.sort(key=lambda r:(r['recorded_time'] or '9999',r['source_file'],r['source_line']))
    width=max((r['field_count'] for r in rows),default=0)
    base=['source_file','source_line','recorded_time','raw_date_time','time_basis','field_count','quality_flags','duplicate_timestamp','source_header']
    fields=base+[key for i in range(1,width+1) for key in (f'value_{i:02d}_raw',f'value_{i:02d}_missing')]+['raw_line']
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    with (output/'rain_records.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    times=[r['recorded_time'] for r in rows if r['recorded_time']]
    summary={'files':len(entries),'records':len(rows),'first_recorded_time':min(times) if times else None,
             'last_recorded_time':max(times) if times else None,'field_counts':dict(Counter(r['field_count'] for r in rows)),
             'flagged_records':sum(bool(r['quality_flags']) for r in rows),'duplicate_timestamp_rows':sum(r['duplicate_timestamp'] for r in rows),
             'files_inventory':inventory,'time_basis':'Unresolved: no timezone assigned and no eight-hour shift or date repair applied.',
             'values':'Uncalibrated positional source values; -998 retained and flagged as missing. No interpolation, differencing or aggregation.',
             'channel_mapping':'Unresolved; positions are not assigned station or sensor identities.'}
    (output/'rain_metadata.json').write_text(json.dumps(summary,indent=2)+'\n')
    (output/'README.md').write_text('# Combined BOB RAIN records\n\nOne row per source record, sorted by recorded calendar/time label. This is an archive-wide compilation, not a complete year of observations. See rain_metadata.json for coverage and source hashes.\n\n`recorded_time` has no assigned timezone. `value_NN_raw` retains the original numeric value, including -998; the matching missing flag identifies this sentinel. Blank columns indicate absent fields. No rainfall calibration, counter differencing, carry-forward reconstruction, clock repair or deduplication has been applied. `raw_line`, source file and line number preserve provenance. Channel identities and whether values are carried-forward states or fresh updates remain unresolved.\n')
    return summary
