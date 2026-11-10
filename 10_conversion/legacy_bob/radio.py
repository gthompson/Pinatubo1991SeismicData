from pathlib import Path
import csv,json,re

def run(source, output):
    SRC=Path(source);OUT=Path(output);OUT.mkdir(parents=True,exist_ok=True)
    MONTHS={'MAY':5,'JUN':6,'JUL':7}
    all_rows=[]
    for name in ['SEIS_MAP.OLD','SEIS_MAP.CUR']:
        snapshot=None;has_attn=False;last=None;last_radio=''
        for lineno,line in enumerate((SRC/name).read_bytes().decode('cp1252').splitlines(),1):
            line=line.replace('\x1a','')
            date=re.search(r'SEISMIC STATION MAP \((\d{2}) ([A-Z]{3}) (\d{2})\)',line)
            if date:
                snapshot=f'19{date[3]}-{MONTHS[date[2]]:02d}-{int(date[1]):02d}'
                has_attn=False;last=None;last_radio='';continue
            if 'DISC' in line and 'RADIO' in line:has_attn='ATTN' in line;continue
            if re.match(r'^\s*\d{2}\s+',line):
                if snapshot is None:raise ValueError('Undated station row')
                slot=line[1:3].strip();station=line[6:10].strip()
                assigned=station!='----'
                site=line[12:42 if has_attn else 50].strip() if assigned else ''
                attn=line[42:50].strip() if assigned and has_attn else ''
                vco=line[50:56].strip() if assigned else ''
                radio=line[56:].strip() if assigned else ''
                route=''
                match=re.search(r'\s+to\s+(.+)$',radio,flags=re.I)
                if match:route=match[1];radio=radio[:match.start()].strip()
                ditto=radio=='"'
                resolved=last_radio if ditto else radio
                if assigned and radio and not ditto:last_radio=radio
                row=dict(snapshot_date=snapshot,source_file=name,source_line=lineno,disc_slot=slot,
                    legacy_id=station,assigned=assigned,site_as_written=site,
                    attenuation_as_written=attn,vco_as_written=vco,radio_as_written=radio,
                    radio_resolved_same_table=resolved,radio_ditto=ditto,relay_as_written=route,
                    raw_line=line)
                all_rows.append(row);last=row
            elif line.strip().upper().startswith('TO ') and last is not None:
                last['relay_as_written']=line.strip()[3:].strip()
    fields=list(all_rows[0])
    with (OUT/'radio_maps_all_source_rows.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(all_rows)
    # Retain one copy of each dated table for convenience; prove the repeated July table agrees.
    old=[r for r in all_rows if r['snapshot_date']=='1991-07-08' and r['source_file']=='SEIS_MAP.OLD']
    cur=[r for r in all_rows if r['source_file']=='SEIS_MAP.CUR']
    compare=[k for k in fields if k not in ['source_file','source_line','raw_line']]
    if [{k:r[k] for k in compare} for r in old] != [{k:r[k] for k in compare} for r in cur]:
        raise ValueError('July map copies differ; review before selecting unique snapshots')
    unique=[r for r in all_rows if r['source_file']=='SEIS_MAP.OLD']
    with (OUT/'radio_maps_unique_snapshots.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(unique)
    summary={'source_rows':len(all_rows),'unique_snapshot_rows':len(unique),
             'current_july_table_matches_old_copy':True,
             'snapshots':{date:{'slots':len([r for r in unique if r['snapshot_date']==date]),
                               'assigned':len([r for r in unique if r['snapshot_date']==date and r['assigned']])}
                          for date in sorted({r['snapshot_date'] for r in unique})},
             'units':'Not printed in these files; VCO Hz, radio MHz and attenuation dB are contextual interpretations.',
             'date_meaning':'Dated map snapshots, not installation or continuous operational epochs.'}
    (OUT/'radio-map-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
