"""Step-15 audit helpers; originals remain read-only and outputs are explicit."""
from pathlib import Path
from collections import defaultdict
import hashlib,json,shutil
import pandas as pd
from obspy import read,read_inventory
from obspy.io.stationxml.core import validate_stationxml


def sha256(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def scan_converted_headers(wav_root,outdir,*,years=(1991,),hash_waveforms=False):
    """Scan the exact converted WAV database, never a reconstructed DATA_TOP path."""
    root=Path(wav_root).expanduser().resolve();out=Path(outdir);out.mkdir(parents=True,exist_ok=True)
    if not root.is_dir():raise FileNotFoundError(root)
    rows=defaultdict(lambda:dict(files=0,first=None,last=None,sample_rates=set(),example_file=None));errors=[];manifest=[]
    files=sorted(p for year in years for p in (root/str(year)).rglob('*') if p.is_file() and not p.name.startswith('.'))
    if not files:raise FileNotFoundError(f'No converted files for {years} in {root}')
    for i,path in enumerate(files,1):
        stat=path.stat();entry={'path':str(path),'bytes':stat.st_size,'mtime_ns':stat.st_mtime_ns}
        if hash_waveforms:entry['sha256']=sha256(path)
        try:
            st=read(str(path),headonly=True);seen=set()
            if not st:raise ValueError('No waveform traces')
            for tr in st:
                s=tr.stats;month=path.relative_to(root).parts[1];key=(month,str(s.starttime.date),s.network,s.station,s.location,s.channel)
                r=rows[key]
                if key not in seen:r['files']+=1;seen.add(key)
                start,end=str(s.starttime),str(s.endtime)
                r['first']=min(r['first'],start) if r['first'] else start;r['last']=max(r['last'],end) if r['last'] else end
                r['sample_rates'].add(float(s.sampling_rate));r['example_file']=r['example_file'] or str(path)
            entry.update(status='read',traces=len(st))
        except Exception as exc:entry.update(status='failed',error=str(exc));errors.append({'path':str(path),'error':str(exc)})
        manifest.append(entry)
        if i%1000==0:print(f'Headers inspected: {i}/{len(files)}',flush=True)
    records=[dict(file_month=k[0],date=k[1],network=k[2],station=k[3],location=k[4],channel=k[5],**{**r,'sample_rates':','.join(map(str,sorted(r['sample_rates'])))}) for k,r in rows.items()]
    fields=['file_month','date','network','station','location','channel','files','first','last','sample_rates','example_file']
    frame=pd.DataFrame(records,columns=fields).sort_values(['date','station','channel']);frame.to_csv(out/'waveform_header_presence.csv',index=False)
    pd.DataFrame(errors,columns=['path','error']).to_csv(out/'header_scan_failures.csv',index=False)
    (out/'waveform_file_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    (out/'header_scan_summary.json').write_text(json.dumps({'source_root':str(root),'years':list(years),'files_read':len(files),'failures':len(errors),'waveform_content_hashing':hash_waveforms,'identity_only':True},indent=2)+'\n')
    return frame


def audit_inventory(path):
    inv=read_inventory(str(path));schema,errors=validate_stationxml(str(path));overlaps=[];undated=[];responses=[]
    for network in inv:
        for station in network:
            for i,a in enumerate(station):
                ident=f'{network.code}.{station.code}.{a.location_code}.{a.code}'
                if a.start_date is None:undated.append(ident)
                if a.response:responses.append({'id':ident,'input_units':a.response.instrument_sensitivity.input_units,'output_units':a.response.instrument_sensitivity.output_units})
                for b in station.channels[i+1:]:
                    if (a.code,a.location_code)!=(b.code,b.location_code):continue
                    starts=[t for t in (a.start_date,b.start_date) if t is not None];ends=[t for t in (a.end_date,b.end_date) if t is not None]
                    lo=max(starts) if starts else None;hi=min(ends) if ends else None
                    if lo is None or hi is None or lo<=hi:overlaps.append({'id':ident,'start':str(lo),'end':str(hi),'boundary_only':lo is not None and hi is not None and lo==hi})
    return {'schema_valid':schema,'schema_errors':list(errors),'overlaps':overlaps,'undated_channels':undated,'responses':responses,'stations':sum(len(n) for n in inv),'channel_epochs':sum(len(s) for n in inv for s in n)}


def exclusive_boundary_copy(source,destination):
    """Separate technical XML copy; shared end is 1 microsecond before next start.

    Source XML and documentary CSV are untouched. The epsilon encodes a selection
    convention, not measured temporal precision or a revised historical instant.
    """
    from obspy.core.inventory import Comment
    inv=read_inventory(str(source));changes=[]
    for net in inv:
        for station in net:
            for a in station:
                for b in station:
                    if a is b or (a.code,a.location_code)!=(b.code,b.location_code):continue
                    if a.end_date is not None and b.start_date is not None and a.end_date==b.start_date:
                        old=a.end_date;a.end_date=b.start_date-0.000001
                        a.comments.append(Comment('ENDPOINT CONVENTION: end set 1 microsecond before next epoch start for unambiguous selection; not observed time precision. Historical date/clock uncertainty remains.'))
                        changes.append({'id':f'{net.code}.{station.code}.{a.location_code}.{a.code}','documentary_end':str(old),'encoded_end':str(a.end_date)})
    inv.write(str(destination),format='STATIONXML',validate=True)
    return changes


def package_evidence(data_dir,out,config_file,*,code_files=()):
    data=Path(data_dir);out=Path(out);target=out/'source_metadata';target.mkdir(exist_ok=True)
    inputs={}
    for p in sorted(data.iterdir()):
        if p.is_file() and p.suffix.lower() in ('.json','.csv','.txt','.dat','.inp'):
            shutil.copy2(p,target/p.name);inputs[p.name]=sha256(p)
    inputs['config:'+str(config_file)]=sha256(config_file)
    manifest={'source_metadata_sha256':inputs,'code_sha256':{str(p):sha256(p) for p in code_files},'clock_correction_applied':False,'complete_count_response_verified':False}
    (out/'step15_provenance.json').write_text(json.dumps(manifest,indent=2)+'\n')
    # Repair the old output-relative source pointer without changing source code/docs.
    p=out/'chronology.md'
    if p.exists():p.write_text(p.read_text().replace('../data/sources.json','source_metadata/sources.json'))
    return manifest
