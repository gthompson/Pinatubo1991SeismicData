import re
from datetime import datetime, timezone
from pathlib import Path
import ast,json,tempfile,zipfile,hashlib,os
import pandas as pd,numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def test_bob_notebook_workflow():
    root=Path(__file__).resolve().parent
    if not (root/'60_prepare_bob_archives.ipynb').exists(): root=root.parent
    ns=dict(datetime=datetime,timezone=timezone,Path=Path,json=json,hashlib=hashlib,pd=pd,np=np,plt=plt,os=os,re=__import__('re'),display=lambda *a:None)
    for path in root.glob('6[0-9]_*.ipynb'):
     j=json.loads(path.read_text())
     for i,c in enumerate(j['cells']):
      if c['cell_type']=='code':ast.parse(''.join(c['source']),filename=f'{path.name}:{i}')
    print('Syntax: 10 notebooks passed')
    j=json.loads((root/'60_prepare_bob_archives.ipynb').read_text())
    for c in j['cells']:
     if 'pipeline_definitions' in c['metadata'].get('tags',[]):exec(c['source'],ns)
    with tempfile.TemporaryDirectory() as tmp:
     b=Path(tmp)/'BOB';b.mkdir();ob=b.parent/'obsolete'
     def z(name,files):
      with zipfile.ZipFile(b/name,'w') as f:
       for k,v in files.items():f.writestr(k,v)
     z('good.zip',{'good.dat':b'abc'})
     z('clash.zip',{'old.dat':b'new'})
     (b/'old.dat').write_bytes(b'old')
     z('identical.zip',{'same.dat':b'same'});(b/'same.dat').write_bytes(b'same')
     z('unsafe.zip',{'../escape':b'bad'})
     inner=b.parent/'inner.zip'
     with zipfile.ZipFile(inner,'w') as f:f.writestr('nested.dat',b'nested')
     z('outer.zip',{'inside/inner.zip':inner.read_bytes()})
     report,left=ns['prepare_archives'](b,ob,apply=False)
     assert not (b/'good.dat').exists() and not ob.exists()
     report,left=ns['prepare_archives'](b,ob,apply=True)
     assert (b/'good.dat').read_bytes()==b'abc'
     assert (b/'old.dat').read_bytes()==b'old' and (b/'clash.zip').exists()
     assert (b/'unsafe.zip').exists() and not (b.parent/'escape').exists()
     assert (ob/'identical.zip').exists() and (ob/'good.zip').exists()
     assert (b/'inside/nested.dat').read_bytes()==b'nested'
     assert (ob/'inside/inner.zip').exists()
     assert len(left)==2
     report2,left2=ns['prepare_archives'](b,ob,apply=True)
     assert len(report2)==2 and set(left)==set(left2)
     print('Extraction: preview, preservation, identical/clashing members, unsafe paths, nested ZIPs and rerun passed')
     # Exercise actual library + local exports/plots with a small text fixture.
     j=json.loads((root/'66_cospec.ipynb').read_text())
     for c in j['cells']:
      if 'pipeline_definitions' in c['metadata'].get('tags',[]):exec(c['source'],ns)
     source=b/'COSPEC';source.mkdir();(source/'COS_PEC.DAT').write_text('13 MAY 91 00:00 100\n14 MAY 91 00:00 -998\n')
     ns.update(BOB_ROOT=b,BOB_OUTPUT=b.parent/'FAIR/BOB',BOB_CONFIG={},CONFIG={},OVERWRITE=False)
     ds=ns['read_cospec'](source)
     target=ns['export_product'](ds,'cospec','test notes')
     ns['plot_product'](ds,target)
     daily=pd.read_csv(target/'daily_coverage.csv');assert len(daily)==1
     try:ns['export_product'](ds,'cospec','test')
     except FileExistsError:pass
     else:raise AssertionError('Overwrite guard failed')
     with zipfile.ZipFile(source/'block.ZIP','w') as f:f.writestr('x','x')
     try:ns['product_source']('cospec','COSPEC')
     except RuntimeError:pass
     else:raise AssertionError('ZIP input guard failed')
     print('FLOVOpy imports, export, daily coverage, plot, overwrite and ZIP guards passed')


def test_bob_qc_date_filter_and_overview():
    root=Path(__file__).resolve().parent
    if not (root/'60_prepare_bob_archives.ipynb').exists():root=root.parent
    ns=dict(Path=Path,json=json,os=os,re=re,np=np,pd=pd,hashlib=hashlib,datetime=datetime,timezone=timezone,plt=plt,display=lambda *args:None)
    for p in root.glob('6[0-9]_*.ipynb'):
     j=json.loads(p.read_text())
     for c in j['cells']:
      if c['cell_type']=='code':ast.parse(''.join(c['source']),filename=p.name)
    print('Syntax passed for all 10 notebooks')
    for c in json.loads((root/'67_counts.ipynb').read_text())['cells']:
     if c['cell_type']=='code' and 'def export_product(' in c['source']:exec(c['source'],ns)
    with tempfile.TemporaryDirectory() as tmp:
     base=Path(tmp);source=base/'BOB/COUNTS';source.mkdir(parents=True)
     (source/'CAB_CNT.DAT').write_text('13 JUL 19 06:30 5\n13 JUL 91 06:30 7\n13 JUL 91 06:30 8\n20 SEO 91 04:30 1\n14 JUL 91 00:30 0\n')
     ns.update(BOB_ROOT=base/'BOB',BOB_OUTPUT=base/'FAIR/BOB',BOB_CONFIG={},CONFIG={},OVERWRITE=False)
     ds=ns['read_counts'](source);target=ns['export_product'](ds,'counts','fixture')
     coverage=pd.read_csv(target/'coverage.csv');print(coverage.to_string(index=False))
     assert pd.to_datetime(coverage['first']).min().year==1991
     allcov=pd.read_csv(target/'coverage_all_recorded_dates.csv');assert pd.to_datetime(allcov['first']).min().year==1919
     assert len(pd.read_csv(target/'date_review.csv'))==2
     assert len(pd.read_csv(target/'duplicate_records.csv'))==2
     assert ds.data.loc[0,'value_01']==5 and ds.data.loc[0,'recorded_time'].year==1919
     assert pd.read_csv(target/'daily_coverage.csv').records.sum()==3
     assert pd.read_csv(target/'daily_coverage.csv').unique_timestamps.sum()==2
     status=json.loads((target/'export_status.json').read_text());assert status['complete']
     ns['plot_product'](ds,target)
     assert (target/'availability_by_series.png').exists()
     try:ns['export_product'](ds,'counts','again')
     except FileExistsError:pass
     else:raise AssertionError('overwrite guard')
     for c in json.loads((root/'69_bob_coverage_and_review.ipynb').read_text())['cells']:
      if 'pipeline_definitions' in c.get('metadata',{}).get('tags',[]):exec(c['source'],ns)
     assemble=ns['assemble_coverage']
     _,tab,_,missing,_=assemble(base/'FAIR/BOB',{'COUNTS':['counts']},'1991-04-01','1991-12-01')
     assert tab.observed_days.iloc[0]==2 and not missing
     try:assemble(base/'FAIR/BOB',{'MUD':['mudflow']},'1991-04-01','1991-12-01')
     except FileNotFoundError:pass
     else:raise AssertionError('missing product')
     _,tab,_,missing,_=assemble(base/'FAIR/BOB',{'MUD':['mudflow']},'1991-04-01','1991-12-01',allow_partial=True)
     assert missing and pd.isna(tab.observed_days.iloc[0])
     status['run_id']='wrong';(target/'export_status.json').write_text(json.dumps(status))
     try:assemble(base/'FAIR/BOB',{'COUNTS':['counts']},'1991-04-01','1991-12-01')
     except FileNotFoundError:pass
     else:raise AssertionError('mixed run')
     print('Date flags/raw preservation, duplicates, coverage, plots, overwrite guard, missing and mixed-run overview checks passed')
