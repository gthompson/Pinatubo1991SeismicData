from pathlib import Path
from datetime import datetime
from collections import Counter
import re,csv,json

def run(source, output):
    SRC=Path(source);OUT=Path(output);OUT.mkdir(parents=True,exist_ok=True)
    rows=[];summary={}
    pattern=re.compile(r'^\s*(\d{1,2})\s+([A-Za-z]{3})\s+(\d{2})\s+(\d{2})[:. ](\d{2})(?:\s+([+-]?(?:\d+(?:\.\d*)?|\.\d+)))?\s*$')
    for name in ['CAB_CNT.DAT','CAB_HR.DAT','PIE_CNT.DAT']:
     records=[]
     for number,line in enumerate((SRC/name).read_text().splitlines(),1):
      s=line.strip().strip('\x1a').strip()
      if not s or not re.match(r'^\d',s):continue
      row=dict(source_file=name,source_line=number,recorded_time='',count='',status='',time_basis='GMT (source header)' if name.startswith('CAB') else 'unspecified',raw_line=line)
      m=pattern.match(s)
      if not m:row['status']='unparsed'
      else:
       d,mon,y,h,mi,value=m.groups()
       try:row['recorded_time']=datetime.strptime(f'{d} {mon} {1900+int(y)} {h}:{mi}','%d %b %Y %H:%M').isoformat()
       except ValueError:row['status']='invalid_datetime'
       if value is not None:row['count']=float(value)
       if row['recorded_time'] and not row['recorded_time'].startswith('1991-'):row['status']='unexpected_year'
       if not row['status']:row['status']='missing_value' if value is None else 'sentinel_minus999' if float(value)==-999 else 'negative_value' if float(value)<0 else 'numeric'
      records.append(row)
     times=Counter(r['recorded_time'] for r in records if r['recorded_time'])
     for r in records:r['duplicate_timestamp']=bool(r['recorded_time'] and times[r['recorded_time']]>1)
     summary[name]={'records':len(records),'status_counts':dict(Counter(r['status'] for r in records)),'duplicate_timestamps':{k:v for k,v in times.items() if v>1},'invalid_examples':[r['raw_line'] for r in records if r['status'] in ('invalid_datetime','unparsed')][:10]}
     rows.extend(records)
    with (OUT/'bob_count_records.csv').open('w') as f:
     w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    a={ (r['recorded_time'],r['count']) for r in rows if r['source_file']=='CAB_CNT.DAT' and r['status']=='numeric'}
    b={ (r['recorded_time'],r['count']) for r in rows if r['source_file']=='CAB_HR.DAT' and r['status']=='numeric'}
    summary['comparison']={'CAB_HR_numeric_pairs_not_in_CAB_CNT':len(b-a),'CAB_CNT_additional_numeric_pairs':len(a-b)}
    (OUT/'counts_summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(3,1,figsize=(11,8),layout='constrained')
    for ax,name,label in zip(axes,['CAB_CNT.DAT','CAB_HR.DAT','PIE_CNT.DAT'],['CAB_CNT: hourly earthquake counts; source GMT','CAB_HR: overlapping shorter copy; source GMT','PIE_CNT: daily date labels; time basis unspecified']):
     rr=[r for r in rows if r['source_file']==name and r['recorded_time'].startswith('1991-')]
     valid=[r for r in rr if r['status']=='numeric']
     ax.plot([datetime.fromisoformat(r['recorded_time']) for r in valid],[r['count'] for r in valid],'.',ms=2,color='#226999')
     missing=[r for r in rr if r['status']!='numeric']
     ax.plot([datetime.fromisoformat(r['recorded_time']) for r in missing],[0]*len(missing),'|',color='#c7522a',ms=6,label='Missing/sentinel (positioned at baseline)')
     ax.set_title(label,loc='left',fontsize=10);ax.set_ylabel('Reported count');ax.grid(alpha=.2)
     if missing:ax.legend(fontsize=7,loc='upper right')
     ax.tick_params(axis='x',labelsize=8)
    fig.suptitle('BOB legacy earthquake-count files — uncorrected source labels')
    for suffix in ('png','pdf'):fig.savefig(OUT/f'bob_counts_overview.{suffix}',dpi=180)
    plt.close(fig)
    return summary
