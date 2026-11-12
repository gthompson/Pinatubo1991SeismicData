from pathlib import Path
import zipfile,re,json,csv,collections,math
from datetime import datetime
source=Path('/Users/thompsong/Library/CloudStorage/Box-Box/thompsong/DATA/pinatubo1991/LEGACY/BOB/SSAM.zip')
out=Path('outputs/legacy-monitoring/ssam');out.mkdir(parents=True,exist_ok=True)
months={m:i for i,m in enumerate('JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC'.split(),1)}
pattern=re.compile(r'^\s*(\d{1,2})[- ]([A-Za-z]{3})[- ](\d{2})\s+(\d{1,2})[: ]\s*(\d{1,2})\s+(.*)$')
stations={};files=[];errors=[];headers=collections.Counter()
with zipfile.ZipFile(source) as z:
 for member in z.infolist():
  parts=Path(member.filename).parts
  if member.filename.startswith('__MACOSX') or not member.filename.upper().endswith('.DAT'):continue
  station=parts[1];s=stations.setdefault(station,{'files':0,'numeric_rows':0,'times':set(),'days':set(),'all_missing_rows':0,'steps':collections.Counter()});s['files']+=1
  try:blob=z.read(member)
  except Exception as e:errors.append([member.filename,str(e)]);continue
  times=[];valid=[]
  for i,line in enumerate(blob.decode('cp1252').splitlines(),1):
   line=line.replace('\x1a','').strip()
   if not line:continue
   m=pattern.fullmatch(line)
   if not m:
    if line[0].isdigit():errors.append([member.filename,i,line[:120]])
    else:headers[line]+=1
    continue
   try:
    d,mo,y,h,mi,tail=m.groups();t=datetime(1900+int(y),months[mo.upper()],int(d),int(h),int(mi))
    values=[float(v) for v in tail.split()]
    if len(values)!=16:raise ValueError(f'{len(values)} bands')
   except Exception as e:errors.append([member.filename,i,str(e)]);continue
   s['numeric_rows']+=1;times.append(t)
   if any(math.isfinite(v) and v not in (-998,-999) for v in values):
    s['times'].add(t);s['days'].add(t.date());valid.append(t)
   else:s['all_missing_rows']+=1
  s['steps'].update(int((b-a).total_seconds()) for a,b in zip(times,times[1:]) if b>a)
  files.append({'file':member.filename,'station':station,'parsed_rows':len(times),'nonmissing_rows':len(valid),'first':min(valid).isoformat() if valid else '', 'last':max(valid).isoformat() if valid else ''})
summary={}
for station,s in sorted(stations.items()):
 times=s.pop('times');days=s.pop('days');steps=s.pop('steps')
 summary[station]={**s,'unique_nonmissing_timestamps':len(times),'days_with_data':len(days),'first':min(times).isoformat() if times else None,'last':max(times).isoformat() if times else None,'common_positive_steps_seconds':steps.most_common(3)}
(out/'ssam_coverage_summary.json').write_text(json.dumps({'stations':summary,'headers':dict(headers),'parse_error_count':len(errors),'error_examples':errors[:20],'time_basis':'Recorded labels, not timezone corrected','nonmissing_policy':'At least one finite band value other than -998/-999; not a calibration or signal-quality test'},indent=2)+'\n')
with (out/'ssam_file_coverage.csv').open('w',newline='') as f:
 w=csv.DictWriter(f,fieldnames=list(files[0]));w.writeheader();w.writerows(files)
print(json.dumps(summary,indent=2));print('errors',len(errors),'examples',errors[:5]);print('headers',headers)
