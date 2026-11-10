"""Read-only header inventory. Presence is not verified uptime or deployment."""
from pathlib import Path
from collections import defaultdict
import json
import pandas as pd
from obspy import read

def scan_headers(data_root,outdir=None,months=('06','07','08')):
    if outdir is None:
        import sys
        sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
        from pinatubo_project import network_products
        outdir=network_products()/'data'
    outdir=Path(outdir);outdir.mkdir(parents=True,exist_ok=True)
    root=Path(data_root)/'FAIR/SEISAN/WAV/PNTBO/1991'
    counts=defaultdict(lambda:dict(files=0,first=None,last=None,sample_rates=set(),example_file=None))
    failures=[];n=0
    for month in months:
        for path in sorted((root/month).iterdir()):
            if not path.is_file() or path.name.startswith('.'):continue
            n+=1
            try:
                stream=read(str(path),headonly=True)
                seen=set()
                for trace in stream:
                    s=trace.stats;day=str(s.starttime.date)
                    key=(month,day,s.network,s.station,s.location,s.channel)
                    row=counts[key]
                    if key not in seen:row['files']+=1;seen.add(key)
                    row['first']=min(row['first'],str(s.starttime)) if row['first'] else str(s.starttime)
                    row['last']=max(row['last'],str(s.endtime)) if row['last'] else str(s.endtime)
                    row['sample_rates'].add(s.sampling_rate)
                    row['example_file']=row['example_file'] or str(path)
            except Exception as exc:failures.append(dict(path=str(path),error=str(exc)))
            if n%1000==0:print('Headers read:',n,flush=True)
    rows=[dict(file_month=k[0],date=k[1],network=k[2],station=k[3],location=k[4],channel=k[5],
            **{**v,'sample_rates':','.join(str(r) for r in sorted(v['sample_rates']))}) for k,v in counts.items()]
    frame=pd.DataFrame(rows).sort_values(['date','station','channel'])
    frame.to_csv(outdir/'waveform_header_presence.csv',index=False)
    pd.DataFrame(failures,columns=['path','error']).to_csv(outdir/'header_scan_failures.csv',index=False)
    (outdir/'header_scan_summary.json').write_text(json.dumps(dict(files_read=n,failures=len(failures),
        source_root=str(root),months=list(months),qualification='Header identities only; stored timestamps not retimed; not uptime or install/removal dates.'),indent=2)+'\n')
    return frame

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--data-root',required=True);parser.add_argument('--outdir',default=None)
    args=parser.parse_args();frame=scan_headers(args.data_root,args.outdir)
    print(frame.groupby(['file_month','station']).files.sum().to_string())
