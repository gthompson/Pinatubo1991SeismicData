"""Pinatubo notebook orchestration, reviewed-window metrics and figure recipes.

FLOVOpy owns detection, features, amplitude statistics and spectrograms.
This local module owns Pinatubo selection, provenance, review policy and plots.
All waveform intervals are half-open. Recorded timestamps are never corrected.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import hashlib
import importlib
import json
import math
import os
import shutil
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import detrend, periodogram, welch
from scipy.signal.windows import dpss
from obspy import read, read_inventory, Stream, UTCDateTime
from flovopy.core.features import FeatureConfig, extract_features, VERSION as FEATURE_VERSION
from flovopy.enhanced.stream import EnhancedStream
from flovopy.processing.detection import PretriggerSettings
from flovopy.processing.spectrograms import compute_spectrogram_result, SpectrogramResult
from flovopy.processing.spectral_metrics import compute_spectral_metrics

sys.path.insert(0,str(Path(__file__).resolve().parents[1])) if str(Path(__file__).resolve().parents[1]) not in sys.path else None
from pinatubo_project import analysis_products, input_path, network_products, network_input, resolve_artifact, runtime_environment, output_path

def _products(ctx):
    return Path(ctx.get('products',ctx.get('products_root'))) if ctx.get('products',ctx.get('products_root')) else analysis_products(ctx.get('config',{}))


def _network_products(ctx):
    return Path(ctx['network_history_products']) if ctx.get('network_history_products') else network_products()


WORKFLOW_VERSION = 'pinatubo-notebooks-1'
FEATURE_KEYS = tuple(extract_features(np.zeros(16), 100)['values'])
APPROVAL_FIELDS = ['accepted_noise_start_file_s', 'accepted_noise_end_file_s',
                   'accepted_signal_start_file_s', 'accepted_signal_end_file_s']
STATIONS = ['BUG','BUR','CAB','CRW','DON','FNG','GRN','LIL','PI2','PIE','PPO','QAD','UBO']
ARTIFACT_FIELDS = ['status','detected','peak_hz','psd_ratio','power_fraction_search','nperseg']
AMPLITUDE_FIELDS = ['amplitude.rms_counts','amplitude.mean_abs_counts','amplitude.peak_abs_counts',
                    'amplitude.peak_to_peak_counts','amplitude.stdev_counts',
                    'amplitude.skewness','amplitude.kurtosis','amplitude.energy_counts2_s']
SAM_FIELDS = [f'{family}.{band}_{unit}' for family,unit in [('sam','counts'),('sem','counts2_s')] for band in ['lf','hf','full']] + ['sam.log2_hf_lf_ratio','sem.log2_hf_lf_ratio']
PSD_FIELDS = ['psd.peak_frequency_hz','psd.centroid_hz','psd.bandwidth_hz',
              'psd.lf_power_counts2','psd.hf_power_counts2','psd.log2_hf_lf_ratio']
BASE_FIELDS = ['window_policy','window_selection_method','window_flags','event_end_rule','event_end_record_limited','trace_key','record_id','trace_index','file','trace_id','station','channel',
               'recorded_starttime','gain_epoch_id','attenuation_db','epoch_status',
               'window','window_start_file_s','window_end_file_s','actual_start_file_s',
               'actual_end_file_s','window_npts','window_duration_s','units',
               'metric_status','metric_flags','feature_flags','timing_interpretation',
               'event_to_noise_rms_ratio','event_to_noise_rms_db','psd_segment_s',
               'welch_segment_count','feature_version']
METRIC_FIELDS = BASE_FIELDS + AMPLITUDE_FIELDS + SAM_FIELDS + PSD_FIELDS + ['feature.'+k for k in FEATURE_KEYS] + [
    'artifact_'+stage+'_'+k for stage in ('pre','post') for k in ARTIFACT_FIELDS]


def digest(value):
    if isinstance(value, bytes):
        return hashlib.sha256(value).hexdigest()
    return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()


def write_json(path, value):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    Path(path).write_text(json.dumps(value,indent=2,default=str,allow_nan=False)+'\n')


def apply_style():
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':8,'axes.titlesize':9,
        'axes.labelsize':8,'legend.fontsize':7,'xtick.labelsize':7,'ytick.labelsize':7,
        'pdf.fonttype':42,'svg.fonttype':'none','savefig.dpi':300,
        'axes.spines.top':False,'axes.spines.right':False})


def station_color(station):
    index=STATIONS.index(station) if station in STATIONS else int(digest(station)[:6],16)%20
    return plt.get_cmap('tab20')(index%20)


def save_figure(fig, stem, caption, dpi=300):
    stem=Path(stem);stem.parent.mkdir(parents=True,exist_ok=True)
    for suffix in ['png','pdf','svg']:
        fig.savefig(stem.with_suffix('.'+suffix),dpi=dpi,bbox_inches='tight')
    stem.with_suffix('.caption.md').write_text(caption+'\n')
    return stem.with_suffix('.png')


def load_context(analysis_dir, *, selection_override=None, products_directory=None):
    """Resolve only an explicit root or the known Box root; never probe stale disks."""
    base=Path(analysis_dir).resolve()
    cfg=json.loads((base/'analysis_workflow.json').read_text())
    if selection_override:cfg['selection']={**cfg['selection'],**selection_override}
    products=analysis_products(cfg,override=products_directory)
    # Analysis config remains authoritative; network products come from conversion configuration.
    runtime_environment()
    candidate=os.environ.get('PINATUBO_WAV_ROOT') or (input_path('waveform_root') if os.environ.get('PINATUBO_PROJECT_CONFIG') else cfg.get('wav_root'))
    root=Path(candidate).expanduser() if candidate else input_path('waveform_root')
    if not root.is_dir():
        raise FileNotFoundError('Set wav_root in analysis_workflow.json or PINATUBO_WAV_ROOT to the WAV/PNTBO directory.')
    selection=cfg['selection']
    if selection.get('explicit_files'):
        files=[Path(p).expanduser() if Path(p).is_absolute() else root/p for p in selection['explicit_files']]
        scope='explicit_selection'
    elif selection['mode']=='pilot':
        from pretrigger_qc import select_files
        files,_=select_files(root,selection['months'],selection['files_per_month']);scope='pilot_selected_recordings'
    elif selection['mode']=='sample':
        count=selection['files_per_month']
        if isinstance(count,bool) or not isinstance(count,int) or count<1:raise ValueError('files_per_month must be a positive integer')
        files=[]
        for month in selection['months']:
            if not __import__('re').fullmatch(r'\d{4}/\d{2}',month):raise ValueError('Months must have form YYYY/MM')
            folder=root/month
            candidates=sorted(p for p in folder.iterdir() if p.is_file() and not p.name.startswith('.')) if folder.is_dir() else []
            picks=np.unique(np.rint(np.linspace(0,len(candidates)-1,min(count,len(candidates)))).astype(int)) if candidates else []
            files.extend(candidates[i] for i in picks)
        scope='evenly_spread_files_per_month'
    elif selection['mode']=='all':
        files=sorted(p for p in root.glob('????/??/*') if p.is_file() and not p.name.startswith('.'))
        scope='all_discovered_waveform_candidates'
    else:
        raise ValueError("selection.mode must be 'pilot', 'sample' or 'all'")
    if selection.get('max_files') is not None:
        limit=selection['max_files']
        if isinstance(limit,bool) or not isinstance(limit,int) or limit<1:raise ValueError('max_files must be a positive integer')
        picks=np.unique(np.rint(np.linspace(0,len(files)-1,min(limit,len(files)))).astype(int)) if files else []
        files=[files[i] for i in picks];scope+='__limited'
    files=[p.resolve() for p in files]
    if not files or len(set(files))!=len(files):raise ValueError('Select at least one unique waveform file')
    for key in ['minimum_window_s','noise_guard_s']:
        if not np.isfinite(cfg['processing'][key]) or cfg['processing'][key]<=0:raise ValueError(key+' must be positive')
    manifest=[]
    for p in files:
        stat=p.stat();rid=digest(str(p).encode())[:12]
        manifest.append(dict(file=str(p),record_id=rid,size=stat.st_size,mtime_ns=stat.st_mtime_ns,
                             source_fingerprint=digest([str(p),stat.st_size,stat.st_mtime_ns])))
    module_hashes={}
    for name in ['flovopy.core.features','flovopy.enhanced.stream','flovopy.processing.event_windows',
                 'flovopy.processing.spectrograms','flovopy.processing.spectral_metrics']:
        mod=importlib.import_module(name);module_hashes[name]=digest(Path(mod.__file__).read_bytes())
    payload=dict(version=WORKFLOW_VERSION,config=cfg,scope=scope,files=manifest,
                 helper_sha256=digest(Path(__file__).read_bytes()),flovopy_source_sha256=module_hashes)
    run_id=digest(payload)[:12];out=products/'notebook_workflow'/run_id
    out.mkdir(parents=True,exist_ok=True)
    write_json(out/'run_manifest.json',payload)
    apply_style()
    return dict(base=base,root=root,config=cfg,files=files,manifest=pd.DataFrame(manifest),
                out=out,products=products,run_id=run_id,scope=scope,history=base.parent/'10_conversion/network_history')


def describe_context(ctx):
    return pd.DataFrame([dict(run_id=ctx['run_id'],scope=ctx['scope'],selected_files=len(ctx['files']),
        waveform_root=str(ctx['root']),products=str(ctx['out']),
        time_basis='RECORDED_CLOCK_UNCORRECTED',amplitude_units='counts; unknown end-to-end response')])


def waveform_traces(stream):
    return [tr for tr in stream if tr.stats.npts>0 and not (tr.stats.station or '').upper().startswith('IRIG')]


def finite_data(tr):
    data=np.array(np.ma.asarray(tr.data,dtype=float).filled(np.nan),copy=True)
    data[~np.isfinite(data)]=np.nan
    return data


def runs(mask):
    edges=np.diff(np.r_[False,mask,False].astype(int))
    return list(zip(np.flatnonzero(edges==1),np.flatnonzero(edges==-1)))


def flat_prefix(x):
    if not len(x) or not np.isfinite(x[0]):return 0
    changed=np.flatnonzero((x!=x[0])|~np.isfinite(x))
    return int(changed[0]) if len(changed) else len(x)


def narrowband_audit(data, fs):
    """Legacy 40.5-Hz screen; ratios are evidence, not instrument identification."""
    row=dict(status='not_testable',detected=np.nan,peak_hz=np.nan,psd_ratio=np.nan,
             power_fraction_search=np.nan,nperseg=np.nan)
    x=np.asarray(data,dtype=float)
    if len(x)<16 or not np.isfinite(x).all() or fs/2<=43:
        row['status']='too_short_nyquist_or_nonfinite';return row
    n=min(2048,len(x));f,p=welch(x,fs=fs,nperseg=n,detrend='linear',scaling='density')
    search=(f>=38)&(f<=43);background=(f>=33.5)&(f<=47.5)&(np.abs(f-40.5)>=3)
    if search.sum()<2 or background.sum()<3:
        row['status']='insufficient_frequency_bins';return row
    k=np.flatnonzero(search)[np.argmax(p[search])];bg=np.median(p[background])
    if bg<=0:row['status']='zero_background';return row
    ratio=float(p[k]/bg)
    row.update(status='ok',detected=bool(ratio>=10),peak_hz=float(f[k]),psd_ratio=ratio,
               power_fraction_search=float(p[search].sum()/p.sum()) if p.sum()>0 else np.nan,nperseg=n)
    return row


def epoch_metadata(ctx, station, channel, start):
    path=network_input('pinatubo_channel_epochs.csv',ctx['history']/'products')
    if not path.is_file():return dict(gain_epoch_id='UNKNOWN',attenuation_db=np.nan,epoch_status='missing_history_products')
    if '_epoch_frame' not in ctx:ctx['_epoch_frame']=pd.read_csv(path).fillna('')
    frame=ctx['_epoch_frame'];matches=[]
    for i,row in frame.iterrows():
        if row.station!=station or row.channel!=channel:continue
        if row.start and start<UTCDateTime(row.start):continue
        if row.end and start>=UTCDateTime(row.end):continue
        matches.append((i,row))
    if len(matches)!=1:return dict(gain_epoch_id='UNKNOWN',attenuation_db=np.nan,epoch_status='unmatched' if not matches else 'ambiguous')
    i,row=matches[0]
    return dict(gain_epoch_id=f'{station}.{channel}:{row.start}:{row.end}',
                attenuation_db=pd.to_numeric(row.attenuation_db,errors='coerce'),epoch_status='provisional_recorded_time_match')


def build_inventory(ctx, force=False):
    """Read selected records sequentially; never equate triggered coverage with uptime."""
    for item in ctx['manifest'].to_dict('records'):
        stat=Path(item['file']).stat()
        if digest([item['file'],stat.st_size,stat.st_mtime_ns])!=item['source_fingerprint']:
            raise ValueError('Waveform source changed; reload context and review the new run')
    out=ctx['out'];dest=out/'trace_inventory.csv'
    if dest.exists() and not force:return pd.read_csv(dest),pd.read_csv(out/'file_inventory.csv')
    rows=[];file_rows=[];gaps=[]
    import inspect
    from flovopy.enhanced.serialization import atomic_json
    epoch_path=network_input('pinatubo_channel_epochs.csv',ctx['history']/'products')
    inventory_signature=dict(version='raw-inventory-1',epoch_sha256=digest(epoch_path.read_bytes()) if epoch_path.exists() else None,
        code={name:digest(inspect.getsource(globals()[name]).encode()) for name in ['build_inventory','epoch_metadata','narrowband_audit','finite_data','flat_prefix']})
    # Reuse verified earlier run inventories, avoiding a one-time archive reread.
    legacy_frame=pd.DataFrame();legacy_files=pd.DataFrame();legacy_gaps=pd.DataFrame();covered=set()
    best=None;best_count=0;wanted=set(ctx['manifest'].source_fingerprint)
    for candidate in (ctx['products']/'notebook_workflow').glob('*/trace_inventory.csv'):
        if candidate==dest:continue
        manifest_path=candidate.parent/'run_manifest.json';provenance_path=candidate.parent/'inventory_provenance.json'
        if not manifest_path.exists():continue
        metadata=json.loads(manifest_path.read_text())
        trusted=(metadata.get('helper_sha256')=='a91d2051a146fc9dfef663b96cf61cc694c2fb216453ebbdeb4918a4fddab20b')
        if trusted and epoch_path.exists():trusted=epoch_path.stat().st_mtime_ns<=candidate.stat().st_mtime_ns
        if provenance_path.exists():
            trusted=json.loads(provenance_path.read_text()).get('signature')==inventory_signature
        if not trusted:continue
        overlap=len(wanted.intersection(row['source_fingerprint'] for row in metadata['files']))
        if overlap>best_count:best=candidate;best_count=overlap
    if best is not None:
        previous_files=pd.read_csv(best.parent/'file_inventory.csv')
        legacy_files=previous_files[previous_files.source_fingerprint.isin(wanted)&previous_files.status.eq('read_ok')].copy()
        covered=set(legacy_files.source_fingerprint)
        previous=pd.read_csv(best,low_memory=False)
        legacy_frame=previous[previous.source_fingerprint.isin(covered)].copy()
        gap_path=best.parent/'segment_gaps.csv'
        if gap_path.exists():
            previous=pd.read_csv(gap_path);legacy_gaps=previous[previous.record_id.isin(legacy_files.record_id)].copy()
    for item in ctx['manifest'].to_dict('records'):
        if item['source_fingerprint'] in covered:continue
        path=Path(item['file']);rid=item['record_id']
        cache=ctx['products']/'file_inventory_cache'/rid/(digest([item['source_fingerprint'],inventory_signature])+'.json')
        if cache.exists() and not force:
            payload=json.loads(cache.read_text());rows.extend(payload['rows']);file_rows.append(payload['file']);gaps.extend(payload['gaps']);continue
        first_row=len(rows);first_gap=len(gaps)
        try:stream=read(str(path))
        except Exception as exc:
            file_rows.append({**item,'status':'read_error','error':str(exc),'waveform_traces':0});continue
        usable=waveform_traces(stream);origin=min((tr.stats.starttime for tr in usable),default=None)
        index=0
        for raw_index,tr in enumerate(stream):
            x=finite_data(tr);v=x[np.isfinite(x)];fs=float(tr.stats.sampling_rate)
            is_waveform=bool(tr.stats.npts>0 and not (tr.stats.station or '').upper().startswith('IRIG'))
            ti=index if is_waveform else -1
            if is_waveform:index+=1
            equal_extremes=np.isfinite(x)&((x==v.min())|(x==v.max())) if len(v) and np.ptp(v)>0 else np.zeros(len(x),dtype=bool)
            longest=max((b-a for a,b in runs(equal_extremes)),default=0)
            flags=[]
            if len(v)!=len(x):flags.append('missing_or_masked_samples')
            if flat_prefix(x)/fs>=.5:flags.append('leading_flat_possible_padding')
            if flat_prefix(x[::-1])/fs>=.5:flags.append('trailing_flat_possible_padding')
            if len(v) and np.ptp(v)==0:flags.append('constant_trace')
            if longest>=3:flags.append('repeated_extremes_possible_clipping')
            audit=narrowband_audit(x,fs)
            epoch=epoch_metadata(ctx,tr.stats.station,tr.stats.channel,tr.stats.starttime)
            rows.append({**item,'trace_key':f'{rid}:{ti}' if is_waveform else f'{rid}:aux:{raw_index}',
                'trace_index':ti,'stream_index':raw_index,'trace_id':tr.id,'station':tr.stats.station,
                'channel':tr.stats.channel,'network':tr.stats.network,'is_waveform':is_waveform,
                'recorded_starttime':str(tr.stats.starttime),'record_reference_time':str(origin),
                'trace_start_offset_file_s':float(tr.stats.starttime-origin) if origin else np.nan,
                'sampling_rate_hz':fs,'npts':len(x),'duration_s':len(x)/fs,'missing_samples':len(x)-len(v),
                'flat_prefix_s':flat_prefix(x)/fs,'flat_suffix_s':flat_prefix(x[::-1])/fs,
                'repeated_extreme_run_samples':longest,'raw_min':float(v.min()) if len(v) else np.nan,
                'raw_max':float(v.max()) if len(v) else np.nan,'qc_flags':';'.join(flags),
                **epoch,**{'artifact_'+k:val for k,val in audit.items()}})
        for gap in Stream(usable).get_gaps():
            gaps.append(dict(record_id=rid,trace_id='.'.join(gap[:4]),start=str(gap[4]),end=str(gap[5]),
                             gap_seconds=gap[6],gap_samples=gap[7]))
        file_rows.append({**item,'status':'read_ok','error':'','waveform_traces':len(usable),'input_traces':len(stream)})
        atomic_json(cache,dict(rows=rows[first_row:],file=file_rows[-1],gaps=gaps[first_gap:]))
    frame=pd.concat([legacy_frame,pd.DataFrame(rows)],ignore_index=True);files=pd.concat([legacy_files,pd.DataFrame(file_rows)],ignore_index=True)
    frame.to_csv(dest,index=False);files.to_csv(out/'file_inventory.csv',index=False)
    gap_frame=pd.DataFrame(gaps,columns=['record_id','trace_id','start','end','gap_seconds','gap_samples'])
    pd.concat([legacy_gaps,gap_frame],ignore_index=True).to_csv(out/'segment_gaps.csv',index=False)
    write_json(out/'inventory_provenance.json',dict(signature=inventory_signature,reused_legacy_files=len(covered)))
    return frame,files


def plot_inventory(ctx, inventory):
    frame=inventory[inventory.is_waveform].copy();frame['day']=pd.to_datetime(frame.recorded_starttime,utc=True).dt.floor('D')
    fig,axes=plt.subplots(2,2,figsize=(7.2,5),layout='constrained')
    coverage=frame.groupby(['trace_id','day']).size().unstack(fill_value=0)
    image=axes[0,0].imshow(coverage.to_numpy(),aspect='auto',cmap='Blues')
    axes[0,0].set_yticks(range(len(coverage)),coverage.index,fontsize=6)
    axes[0,0].set_xticks(range(len(coverage.columns)),[v.strftime('%m-%d') for v in coverage.columns],rotation=45)
    axes[0,0].set_title('Recorded traces per observed day');cb=fig.colorbar(image,ax=axes[0,0],label='Trace count',shrink=.7)
    from matplotlib.ticker import MaxNLocator
    cb.locator=MaxNLocator(integer=True);cb.update_ticks()
    axes[0,1].hist(frame.duration_s,bins=min(20,max(3,len(frame)//2)),color='#4477aa');axes[0,1].set(xlabel='Stored support (s)',ylabel='Traces')
    rates=frame.sampling_rate_hz.value_counts().sort_index();axes[1,0].bar(rates.index.astype(str),rates.values,color='#228833');axes[1,0].set(xlabel='Sampling rate (Hz)',ylabel='Traces')
    flags={'Missing':int((frame.missing_samples>0).sum()),'Leading flat':int((frame.flat_prefix_s>=.5).sum()),
           'Repeated extrema':int((frame.repeated_extreme_run_samples>=3).sum()),
           '40.5-Hz screen':int(pd.to_numeric(frame.artifact_detected,errors='coerce').fillna(0).sum())}
    axes[1,1].barh(list(flags),list(flags.values()),color='#cc6677');axes[1,1].set(xlabel='Flagged traces (screens, not diagnoses)')
    fig.suptitle('Selected waveform inventory — '+ctx['scope'].replace('__limited',' (limited)').replace('_',' '))
    caption='Recorded-file coverage in the configured selection; not continuous station uptime or earthquake rate. Dates use uncorrected recorded clocks. Repeated extrema and 40.5-Hz ratios are screening flags.'
    save_figure(fig,ctx['out']/'figures/archive_inventory',caption,ctx['config']['plots']['dpi'])
    return fig


def review_network(ctx, rebuild=True):
    """Use the existing reconstruction; keep it independent of ASL/FlowLoc."""
    history=ctx['history']
    if not history.is_dir():raise FileNotFoundError('Expected 10_conversion/network_history')
    sys.path.insert(0,str(history.parent)) if str(history.parent) not in sys.path else None
    if rebuild:
        from network_history.reconstruct import build_all
        build_all(ctx['out']/'network_history')
    else:
        shutil.copytree(network_products() if network_products().exists() else history/'products',ctx['out']/'network_history',dirs_exist_ok=True)
    dest=ctx['out']/'network_history';inv=read_inventory(str(dest/'pinatubo_provisional.xml'))
    rows=[]
    for net in inv:
        for sta in net:
            for ch in sta:
                rows.append(dict(trace_id=f'{net.code}.{sta.code}.{ch.location_code}.{ch.code}',start=str(ch.start_date),
                    end=str(ch.end_date),latitude=ch.latitude,longitude=ch.longitude,elevation_m=ch.elevation,
                    response_present=ch.response is not None,response_status='NO_END_TO_END_COUNTS_RESPONSE'))
    provenance={p.name:digest(p.read_bytes()) for p in (history/'data').glob('*') if p.is_file()}
    write_json(dest/'network_source_hashes.json',provenance)
    return pd.DataFrame(rows),inv


def configure_selection(analysis_dir, *, mode='sample', files_per_month=50,
                        months=('1991/05','1991/06','1991/07','1991/08'), max_files=None):
    """Persist explicit notebook selection, backing up prior shared settings."""
    path=Path(analysis_dir)/'analysis_workflow.json';cfg=json.loads(path.read_text())
    if mode not in ['pilot','sample','all']:raise ValueError('Unknown selection mode')
    if isinstance(files_per_month,bool) or not isinstance(files_per_month,int) or files_per_month<1:raise ValueError('files_per_month must be positive integer')
    if max_files is not None and (isinstance(max_files,bool) or not isinstance(max_files,int) or max_files<1):raise ValueError('max_files must be positive integer or None')
    selection=dict(mode=mode,months=list(months),files_per_month=files_per_month,explicit_files=[],max_files=max_files)
    if cfg['selection']!=selection:
        backup=output_path('20_analysis','config_backups',create=True)/('analysis_workflow.before_'+digest(path.read_bytes())[:12]+'.json')
        if not backup.exists():shutil.copy2(path,backup)
        cfg['selection']=selection;write_json(path,cfg)
    return cfg


def run_window_proposals(ctx, settings=None, *, force=False, max_detail_files=None, progress_every=25):
    """Resume per-file caches; retain arrays only for spread-out plot examples.

    Numerical work remains FLOVOpy's detect_pretrigger_window via the local
    export adapter. Combined outputs contain lightweight evidence/QC tables;
    one-second/frequency/sample arrays remain in each file's cache directory.
    """
    from pretrigger_qc import analyse_files, ALGORITHM_VERSION
    inventory,_=build_inventory(ctx)
    settings=settings or PretriggerSettings(noise_guard_s=ctx['config']['processing']['noise_guard_s'])
    count=len(ctx['files']);keep=ctx['config']['plots']['max_examples'] if max_detail_files is None else max_detail_files
    if isinstance(keep,bool) or not isinstance(keep,int) or keep<0:raise ValueError('max_detail_files must be a nonnegative integer')
    detail_indices=set(np.unique(np.rint(np.linspace(0,count-1,min(keep,count))).astype(int))) if keep else set()
    batch=ctx['out']/'pretrigger'/('batch_'+digest(dict(settings=asdict(settings),algorithm_version=ALGORITHM_VERSION))[:12])
    batch.mkdir(parents=True,exist_ok=True)
    engine=importlib.import_module('flovopy.processing.event_windows')
    engine_signature=digest([ALGORITHM_VERSION,digest(Path(engine.__file__).read_bytes()),np.__version__,pd.__version__])[:12]
    file_root=ctx['products']/'pretrigger_file_cache'/engine_signature
    frames={name:[] for name in ['qc','network','files','evidence']};details={};cache_rows=[]
    cached=failed=0
    def read_table(path):
        try:return pd.read_csv(path)
        except pd.errors.EmptyDataError:return pd.DataFrame()
    for index,path in enumerate(ctx['files']):
        stat=path.stat();manifest=[dict(path=str(path),size=stat.st_size,mtime_ns=stat.st_mtime_ns)]
        payload=dict(settings=asdict(settings),files=manifest,algorithm_version=ALGORITHM_VERSION)
        file_cache=file_root/digest(payload)[:12]
        cached_ok=(file_cache/'manual_window_review.csv').exists() and (file_cache/'run_manifest.json').exists()
        try:
            if force or not cached_ok or index in detail_indices:
                result=analyse_files([path],file_root,settings)
                file_cache=result['output']
                for name in frames:frames[name].append(result[name])
                if not result['files'].status.isin(['analysed']).all():failed+=1
                if index in detail_indices:details.update({key:rows for key,rows in result['details'].items() if rows})
            else:
                for name,filename in [('qc','trace_qc_and_candidates.csv'),('network','network_event_context.csv'),
                                      ('files','file_inventory.csv'),('evidence','onset_evidence.csv')]:
                    frames[name].append(read_table(file_cache/filename))
                cached+=1
            cache_rows.append(dict(file=str(path),output=str(file_cache),status='available'))
        except Exception as exc:
            failed+=1
            frames['files'].append(pd.DataFrame([dict(file=str(path),status='analysis_error',error=f'{type(exc).__name__}: {exc}')]))
            cache_rows.append(dict(file=str(path),output=str(file_cache),status='failed',error=str(exc)))
        if (index+1)%progress_every==0 or index+1==count:
            print(f'Window analysis: {index+1}/{count} files; {cached} reused caches; {failed} failures',flush=True)
            write_json(batch/'progress.json',dict(completed=index+1,total=count,reused=cached,failures=failed))
    combined={name:pd.concat([f for f in tables if len(f)],ignore_index=True) if any(len(f) for f in tables) else pd.DataFrame() for name,tables in frames.items()}
    q=combined['qc'];sources=inventory[inventory.is_waveform][['trace_key','source_fingerprint']]
    if len(q):
        q['trace_key']=q.record_id.astype(str)+':'+q.trace_index.astype(int).astype(str)
        fields=[c for c in ['file','record_id','trace_index','trace_id','trace_key','selected_onset_file_s','selected_onset_method',
            'onset_support_start_file_s','onset_support_end_file_s','proposed_noise_start_file_s','proposed_noise_end_file_s',
            'noise_proposal_status','qc_flags'] if c in q]
        template=q[fields].merge(sources,on='trace_key',validate='one_to_one')
    else:template=pd.DataFrame(columns=['trace_key','source_fingerprint'])
    canonical=ctx['out']/'window_review.csv'
    manual=['review_status',*APPROVAL_FIELDS,'reviewer_notes']
    if canonical.exists():
        old=pd.read_csv(canonical)
        if old.trace_key.duplicated().any():raise ValueError('Duplicate trace keys in existing review')
        template=template.merge(old[['trace_key',*manual]],on='trace_key',how='left',validate='one_to_one')
    else:
        saved=_review_records(ctx)
        template=template.merge(saved[['trace_key','source_fingerprint',*manual]],on=['trace_key','source_fingerprint'],how='left',validate='one_to_one')
    template['review_status']=template.review_status.fillna('UNREVIEWED')
    text=template.to_csv(index=False)
    if not canonical.exists() or canonical.read_text()!=text:canonical.write_text(text)
    for name,filename in [('qc','trace_qc_and_candidates.csv'),('network','network_event_context.csv'),
                          ('files','file_inventory.csv'),('evidence','onset_evidence.csv')]:combined[name].to_csv(batch/filename,index=False)
    pd.DataFrame(cache_rows).to_csv(batch/'file_cache_index.csv',index=False)
    write_json(ctx['out']/'window_proposal_run.json',dict(output=str(batch),settings=asdict(settings),
        detail_files_retained=len(details),selected_files=count,failures=failed,
        arrays_policy='Only spread-out example files retained; all per-file measurements cached on disk'))
    return dict(output=batch,**combined,details=details,settings=settings,metrics=pd.DataFrame(),frequency=pd.DataFrame()),pd.read_csv(canonical)


def save_review_decisions(ctx, updates):
    path=ctx['out']/'window_review.csv';review=pd.read_csv(path).fillna('').astype(object)
    allowed={'review_status','reviewer_notes',*APPROVAL_FIELDS}
    for key,decision in updates.items():
        if set(decision)-allowed:raise ValueError('Only explicit review fields can be edited')
        mask=review.trace_key.eq(key)
        if mask.sum()!=1:raise ValueError('Unknown or duplicate trace_key: '+key)
        status=decision.get('review_status',review.loc[mask,'review_status'].iloc[0])
        if status not in ['UNREVIEWED','ACCEPTED','REJECTED','UNRESOLVED']:raise ValueError('Invalid review status')
        for field,value in decision.items():review.loc[mask,field]=value
    # Validate proposed edits before replacing the canonical review file.
    approved,issues=validate_review(ctx,review=review,write=False)
    invalid=set(issues.loc[issues.severity.eq('error'),'trace_key']) if len(issues) else set()
    if invalid.intersection(updates):raise ValueError('Review edits contain invalid intervals; inspect validate_review results')
    if updates:
        backup=path.with_name('window_review.before_'+digest(path.read_bytes())[:12]+'.csv')
        if not backup.exists():shutil.copy2(path,backup)
        review.to_csv(path,index=False)
        _persist_review_registry(ctx,review,updates.keys())
    return review


def validate_review(ctx, review=None, write=True):
    inventory,_=build_inventory(ctx);lookup=inventory[inventory.is_waveform].set_index('trace_key')
    path=ctx['out']/'window_review.csv'
    if review is None:
        if path.exists():review=pd.read_csv(path).fillna('').astype(object)
        else:
            review=_review_records(ctx)
            review.to_csv(path,index=False)
    approved=[];issues=[]
    def issue(key,reason,severity='error'):issues.append(dict(trace_key=key,severity=severity,reason=reason))
    if not review.empty and review.trace_key.duplicated().any():raise ValueError('Duplicate trace keys in review table')
    for row in review.to_dict('records'):
        key=row['trace_key'];status=str(row.get('review_status','')).upper()
        if status!='ACCEPTED':continue
        if key not in lookup.index:issue(key,'unknown_trace');continue
        meta=lookup.loc[key]
        if row.get('source_fingerprint')!=meta.source_fingerprint:issue(key,'source_fingerprint_mismatch');continue
        parsed=[];bad=False
        for label,prefix in [('noise','noise'),('event','signal')]:
            start=pd.to_numeric(row.get(f'accepted_{prefix}_start_file_s'),errors='coerce')
            end=pd.to_numeric(row.get(f'accepted_{prefix}_end_file_s'),errors='coerce')
            if not np.isfinite(start) and not np.isfinite(end):continue
            if not np.isfinite(start) or not np.isfinite(end):issue(key,label+'_incomplete_bounds');bad=True;continue
            support_start=float(meta.trace_start_offset_file_s);support_end=support_start+float(meta.duration_s)
            if start<support_start-1e-7 or end>support_end+1e-7 or end<=start:
                issue(key,label+'_outside_trace_or_empty');bad=True;continue
            if end-start<ctx['config']['processing']['minimum_window_s']:
                issue(key,label+'_too_short');bad=True;continue
            parsed.append(dict(trace_key=key,window=label,window_start_file_s=float(start),window_end_file_s=float(end)))
        by_label={r['window']:r for r in parsed}
        if len(by_label)==2 and by_label['event']['window_start_file_s']-by_label['noise']['window_end_file_s']<ctx['config']['processing']['noise_guard_s']-1e-7:
            issue(key,'noise_event_overlap_or_guard_violation');bad=True
        if not parsed:issue(key,'accepted_without_any_complete_window');bad=True
        if not bad:
            approved.extend(parsed)
            if len(parsed)==1:issue(key,'only_one_window_accepted','info')
    approved=pd.DataFrame(approved,columns=['trace_key','window','window_start_file_s','window_end_file_s'])
    issues=pd.DataFrame(issues,columns=['trace_key','severity','reason'])
    if write:
        approved.to_csv(ctx['out']/'approved_windows.csv',index=False)
        issues.to_csv(ctx['out']/'review_validation.csv',index=False)
    return approved,issues


def cut_half_open(trace, begin_file_s, end_file_s, reference_time):
    """Select actual sample centres in [begin,end), without padding/rounding outward."""
    fs=float(trace.stats.sampling_rate);offset=float(trace.stats.starttime-reference_time)
    a=max(0,int(np.ceil((begin_file_s-offset)*fs-1e-7)))
    b=min(len(trace),int(np.ceil((end_file_s-offset)*fs-1e-7)))
    if b<=a:raise ValueError('Empty sample selection')
    cut=trace.copy();cut.data=finite_data(trace)[a:b].copy();cut.stats.starttime=trace.stats.starttime+a/fs
    return cut


def processed_copy(trace, cfg):
    cut=trace.copy();x=finite_data(cut)
    if not np.isfinite(x).all():raise ValueError('Missing samples: no interpolation or time compression permitted')
    cut.data=detrend(x,type='linear');cut.stats.units='counts'
    lowpass=cfg['processing'].get('lowpass_hz')
    if lowpass is not None:
        if not 0<lowpass<cut.stats.sampling_rate/2:raise ValueError('Lowpass must be below Nyquist')
        cut.filter('lowpass',freq=lowpass,corners=6,zerophase=True)
    return cut


def matched_psds(traces, cfg):
    """Matched Hann-Welch and density-normalized, equal-weight DPSS estimates."""
    fs=float(traces[0].stats.sampling_rate)
    if any(float(t.stats.sampling_rate)!=fs for t in traces):raise ValueError('Sampling rates must match')
    if any(not np.isfinite(finite_data(t)).all() for t in traces):raise ValueError('PSD input contains gaps')
    n=min(int(round(cfg['spectra']['segment_s']*fs)),min(len(t) for t in traces))
    if n<16:raise ValueError('Too few samples for matched PSDs')
    step=max(1,int(round(n*(1-cfg['spectra']['overlap']))));overlap=n-step
    if not 0<=overlap<n:raise ValueError('Invalid spectral overlap')
    nw=float(cfg['spectra']['multitaper_nw']);k=max(1,int(2*nw)-1)
    if not 0<nw<n/2:raise ValueError('Invalid DPSS time-bandwidth product')
    tapers=dpss(n,NW=nw,Kmax=k,sym=False)
    result=[]
    for tr in traces:
        x=finite_data(tr);f,p=welch(x,fs=fs,window='hann',nperseg=n,noverlap=overlap,
                                  nfft=n,detrend='constant',scaling='density')
        spectra=[]
        for a in range(0,len(x)-n+1,step):
            block=x[a:a+n]-x[a:a+n].mean()
            spectra.append(np.mean([periodogram(block,fs=fs,window=taper,detrend=False,nfft=n,
                scaling='density',return_onesided=True)[1] for taper in tapers],axis=0))
        result.append(dict(frequency_hz=f,welch_psd=p,multitaper_psd=np.mean(spectra,axis=0),
            segment_s=n/fs,segment_count=len(spectra),df_hz=fs/n,nw=nw,tapers=k,
            multitaper_bandwidth_hz=2*nw/(n/fs)))
    return result


def scalar_psd_metrics(spec, trace_id, cfg):
    f=spec['frequency_hz'];p=spec['welch_psd']
    result=SpectrogramResult(trace_id,np.array([0.]),f,p[:,None],np.ones(1),units='counts²/Hz')
    low=cfg['processing']['lf_band'];high=cfg['processing']['hf_band']
    if high[1]>f[-1]:return {key:np.nan for key in PSD_FIELDS},'psd_band_above_nyquist'
    row=compute_spectral_metrics(result,low_band=low,high_band=high).iloc[0]
    return dict(zip(PSD_FIELDS,[row.peak_frequency,row.spectral_centroid,row.spectral_bandwidth,
                              row.low_band_power,row.high_band_power,row.fratio])),''


def measure_trace_windows(raw_trace, windows, reference_time, cfg, *, return_payload=False):
    """Both windows receive the same FLOVOpy amplitude/feature implementations."""
    cuts={};raw_cuts={};rows=[];payloads=[]
    for window in windows:
        label=window['window'];raw=cut_half_open(raw_trace,window['window_start_file_s'],window['window_end_file_s'],reference_time)
        raw_cuts[label]=raw
        try:cuts[label]=processed_copy(raw,cfg)
        except ValueError:pass
    spectra={}
    if cuts:
        specs=matched_psds(list(cuts.values()),cfg)
        spectra=dict(zip(cuts,specs))
    features_cfg=FeatureConfig(lf_band=tuple(cfg['processing']['lf_band']),hf_band=tuple(cfg['processing']['hf_band']))
    for window in windows:
        label=window['window'];raw=raw_cuts[label];fs=float(raw.stats.sampling_rate)
        row={key:np.nan for key in METRIC_FIELDS};flags=[]
        row.update(window_policy=window.get('window_policy','reviewed'),
            window_selection_method=window.get('window_selection_method','manual'),window_flags=window.get('window_flags',''),
            event_end_rule=window.get('event_end_rule','manual'),event_end_record_limited=window.get('event_end_record_limited',False),
            window=label,window_start_file_s=window['window_start_file_s'],window_end_file_s=window['window_end_file_s'],
            actual_start_file_s=float(raw.stats.starttime-reference_time),
            actual_end_file_s=float(raw.stats.starttime-reference_time)+len(raw)/fs,
            window_npts=len(raw),window_duration_s=len(raw)/fs,units='counts',feature_version=FEATURE_VERSION,
            timing_interpretation='WITHIN_WINDOW_ENERGY_DISTRIBUTION_NOT_PHYSICAL_EVENT_DURATION')
        if row['window_flags']:flags.append(row['window_flags'])
        if row['event_end_record_limited']:flags.append('record_end_is_not_a_coda_pick')
        before=narrowband_audit(finite_data(raw),fs)
        row.update({'artifact_pre_'+k:v for k,v in before.items()})
        if label not in cuts:
            row.update(metric_status='REJECTED_DATA',metric_flags='missing_samples_no_interpolation',feature_flags='not_computed')
            rows.append(row);payloads.append(dict(summary=row,enhancedstream={},features={},psd={},artifacts=dict(pre=before)))
            continue
        cut=cuts[label]
        est=EnhancedStream(Stream([cut.copy()]))
        bands_valid=features_cfg.hf_band[1]<fs/2
        est.ampengfft(allow_uncalibrated=True,compute_spectral=True,compute_ssam=True,compute_bandratios=bands_valid,compute_sam=bands_valid,compute_sem=bands_valid,
            bands=(*features_cfg.lf_band,*features_cfg.hf_band))
        m=est[0].stats.metrics
        amp=[m.get(k,np.nan) for k in ['sample_rms','meanamp','peakamp','sample_peak_to_peak','sample_stdev','skewness','kurtosis','energy']]
        # RMS/shape use FLOVOpy; absolute amplitudes have explicit sample definitions.
        x=finite_data(est[0]);amp[1]=float(np.mean(np.abs(x)));amp[2]=float(np.max(np.abs(x)));amp[3]=float(np.ptp(x))
        row.update(dict(zip(AMPLITUDE_FIELDS,amp)))
        for family in ['sam','sem']:
            values=m.get(family,{}).get('values',{})
            unit='counts' if family=='sam' else 'counts2_s'
            for band,old in [('lf','low'),('hf','high'),('full','full')]:
                row[f'{family}.{band}_{unit}']=values.get(old,np.nan)
            row[f'{family}.log2_hf_lf_ratio']=values.get('ratio',np.nan)
            # Match the conservative band-edge support policy used by features.
            for band,minimum_frequency in [('lf',features_cfg.lf_band[0]),('hf',features_cfg.hf_band[0]),('full',features_cfg.lf_band[0])]:
                if len(cut)/fs<=2*features_cfg.edge_cycles/minimum_frequency:
                    row[f'{family}.{band}_{unit}']=np.nan
                    row[f'{family}.log2_hf_lf_ratio']=np.nan
                    flags.append(f'{family}_{band}_too_short_for_band_edges')
        if not bands_valid:flags.append('sam_sem_band_above_nyquist')
        noise=cuts.get('noise') if label=='event' else None
        features=extract_features(cut.data,fs,noise=None if noise is None else noise.data,config=features_cfg,units='counts')
        row.update({'feature.'+key:value for key,value in features['values'].items()})
        # Noise-only timing has no external reference; do not compare noise to itself.
        row['feature_flags']=';'.join(features['qc_flags'])
        psd,flag=scalar_psd_metrics(spectra[label],raw.id,cfg);row.update(psd)
        if flag:flags.append(flag)
        if not np.any(x):flags.append('zero_variance')
        if 'too_short_for_band' in row['feature_flags']:flags.append('short_window_band_features_missing')
        row.update(psd_segment_s=spectra[label]['segment_s'],welch_segment_count=spectra[label]['segment_count'],
            metric_status='COMPUTED_WITH_FLAGS' if flags or features['qc_flags'] else 'COMPUTED',metric_flags=';'.join(flags))
        row.update({'artifact_post_'+k:v for k,v in narrowband_audit(x,fs).items()})
        payloads.append(dict(summary=row,
            enhancedstream=dict(metrics=m,spectral=getattr(est[0].stats,'spectral',{}),
                spectrum=getattr(est[0].stats,'spectrum',{}),station_metrics=est.station_metrics,
                processing=list(est[0].stats.get('processing',[])),
                spectral_array_definition='Amplitude array is abs(rFFT) on detrended samples; not PSD or normalized physical amplitude'),
            features=features,psd={**spectra[label],'quantity':'psd','units':'counts²/Hz'},artifacts=dict(pre=before,post=narrowband_audit(x,fs)),
            reliability_note='Full original computed values retained; curated summary masks short-band values. Inspect summary/feature flags before use.'))
        rows.append(row)
    by_label={r['window']:r for r in rows}
    if set(by_label)=={'noise','event'}:
        n=by_label['noise']['amplitude.rms_counts'];e=by_label['event']['amplitude.rms_counts']
        if np.isfinite(n) and n>0 and np.isfinite(e) and e>0:
            by_label['event']['event_to_noise_rms_ratio']=e/n
            by_label['event']['event_to_noise_rms_db']=20*np.log10(e/n)
    return (rows,payloads) if return_payload else rows


def _review_records(ctx):
    """Carry explicit decisions across selection/run changes, matching source IDs.

    Conflicting historical decisions remain unreviewed and are reported, rather
    than resolving scientific choices from filesystem modification times.
    """
    inventory,_=build_inventory(ctx)
    sources=inventory[inventory.is_waveform][['trace_key','source_fingerprint','trace_id','record_id','trace_index','file','recorded_starttime']]
    paths=[ctx['products']/'window_review_registry.csv',*sorted((ctx['products']/'notebook_workflow').glob('*/window_review.csv'))]
    decisions={};conflicts=set();fields=['review_status',*APPROVAL_FIELDS,'reviewer_notes']
    registry_path=paths[0]
    for path in paths:
        if not path.exists():continue
        try:frame=pd.read_csv(path,usecols=lambda col:col in ['trace_key','source_fingerprint',*fields]).fillna('')
        except pd.errors.EmptyDataError:continue
        if not {'trace_key','source_fingerprint',*fields}.issubset(frame.columns):continue
        active=frame.review_status.ne('UNREVIEWED')|frame.reviewer_notes.ne('')|frame[APPROVAL_FIELDS].ne('').any(axis=1)
        frame=frame[active].merge(sources[['trace_key','source_fingerprint']],on=['trace_key','source_fingerprint'],how='inner')
        for row in frame.to_dict('records'):
            key=(row['trace_key'],row['source_fingerprint'])
            # Registry decisions were explicitly saved and override historical copies.
            if path!=registry_path and key in decisions and decisions[key][0]=='registry':continue
            choice={field:row[field] for field in fields}
            if choice['review_status']=='UNREVIEWED' and not choice['reviewer_notes'] and not any(choice[f]!='' for f in APPROVAL_FIELDS):continue
            if key in decisions and decisions[key][1]!=choice:
                conflicts.add(key);continue
            decisions[key]=('registry' if path==registry_path else 'history',choice)
    template=sources.copy()
    for field in fields:template[field]='UNREVIEWED' if field=='review_status' else ''
    template=template.astype({field:object for field in fields})
    if decisions:
        positions={key:index for index,key in enumerate(zip(template.trace_key,template.source_fingerprint))}
        for key,(_,choice) in decisions.items():
            if key in positions and key not in conflicts:
                for field,value in choice.items():template.at[positions[key],field]=value
    pd.DataFrame([dict(trace_key=k[0],source_fingerprint=k[1],reason='conflicting_historical_review_decisions') for k in conflicts],
                 columns=['trace_key','source_fingerprint','reason']).to_csv(ctx['out']/'review_import_conflicts.csv',index=False)
    return template


def _persist_review_registry(ctx, review, updated_keys):
    path=ctx['products']/'window_review_registry.csv'
    fields=['trace_key','source_fingerprint','review_status',*APPROVAL_FIELDS,'reviewer_notes']
    incoming=review.loc[review.trace_key.isin(updated_keys),fields]
    old=pd.read_csv(path).fillna('') if path.exists() else pd.DataFrame(columns=fields)
    keys=set(zip(incoming.trace_key,incoming.source_fingerprint))
    old=old[[key not in keys for key in zip(old.trace_key,old.source_fingerprint)]]
    merged=pd.concat([old,incoming],ignore_index=True)
    path.parent.mkdir(parents=True,exist_ok=True)
    text=merged.to_csv(index=False)
    if not path.exists() or path.read_text()!=text:path.write_text(text)


def _metric_signature(ctx, item, windows, inventory_rows):
    """Identity depends on this file's measurements, never the selected ensemble."""
    import inspect
    from flovopy.enhanced.serialization import json_compatible
    names=['measure_trace_windows','processed_copy','cut_half_open','matched_psds','scalar_psd_metrics','narrowband_audit']
    if '_metric_code_identity' not in ctx:
        code={name:digest(inspect.getsource(globals()[name]).encode()) for name in names}
        libraries={}
        for name in ['flovopy.core.features','flovopy.core.spectral','flovopy.enhanced.stream','flovopy.processing.spectral_metrics']:
            module=importlib.import_module(name);libraries[name]=digest(Path(module.__file__).read_bytes())
        ctx['_metric_code_identity']=(code,libraries)
    code,libraries=ctx['_metric_code_identity']
    bounds=windows[[c for c in ['trace_key','window','window_start_file_s','window_end_file_s','window_policy','window_selection_method','window_flags','event_end_rule','event_end_record_limited'] if c in windows]].sort_values(['trace_key','window']).to_dict('records')
    epoch_fields=['trace_key','gain_epoch_id','attenuation_db','epoch_status']
    return dict(schema='pinatubo-full-event-metrics-2',selection_policy=ctx.get('_active_window_policy',{'policy':'reviewed'}),source_fingerprint=item['source_fingerprint'],
        accepted_windows=json_compatible(bounds),processing=ctx['config']['processing'],
        spectra={key:ctx['config']['spectra'][key] for key in ['segment_s','overlap','multitaper_nw']},
        computation_sha256=code,flovopy_sha256=libraries,feature_version=FEATURE_VERSION,
        epochs=json_compatible(inventory_rows[epoch_fields].sort_values('trace_key').to_dict('records')),
        versions=dict(numpy=np.__version__,pandas=pd.__version__,scipy=__import__('scipy').__version__))


def load_event_metrics_catalog(ctx, *, include_trace_metrics=True):
    from flovopy.enhanced.catalog import EnhancedCatalog
    path=ctx['out']/'event_metrics_index.csv'
    if not path.exists():return EnhancedCatalog()
    index=pd.read_csv(path);paths=[str(resolve_artifact(value)) for value in index.loc[index.status.ne('failed'),'sidecar_path'].dropna()]
    if include_trace_metrics:return EnhancedCatalog.from_sidecars(paths,require_quakeml=False)
    return _lightweight_catalog(paths)


def export_event_metrics(ctx, catalog=None, *, write_quakeml=True):
    """Rebuild ensemble exports from active JSON sidecars, without waveform I/O."""
    if catalog is None:return _stream_event_exports(ctx,write_quakeml=write_quakeml)
    rows=[]
    for event in catalog.records:
        for trace in event.meta.traces:
            if 'summary' in trace:rows.append(trace['summary'])
    curated=pd.DataFrame(rows,columns=METRIC_FIELDS)
    text_fields={'trace_key','record_id','file','trace_id','station','channel','recorded_starttime',
        'gain_epoch_id','epoch_status','window','units','metric_status','metric_flags','feature_flags',
        'window_policy','window_selection_method','window_flags','event_end_rule',
        'timing_interpretation','feature_version','artifact_pre_status','artifact_post_status'}
    for field in curated.columns:
        if field in text_fields:continue
        if field in ['artifact_pre_detected','artifact_post_detected','event_end_record_limited']:
            curated[field]=curated[field].astype('boolean')
        else:curated[field]=pd.to_numeric(curated[field],errors='coerce')
    curated.to_csv(ctx['out']/'window_metrics.csv',index=False)
    catalog.export_metrics_csv(ctx['out']/'all_window_metrics.csv',level='trace')
    catalog.export_metrics_csv(ctx['out']/'events_metadata.csv',level='event')
    catalog.export_csv(str(ctx['out']/'catalog_summary.csv'))
    full=catalog.metrics_dataframe(level='trace')
    pd.DataFrame(dict(column=full.columns,dtype=[str(full[c].dtype) for c in full])).to_csv(ctx['out']/'all_metrics_schema.csv',index=False)
    if write_quakeml and len(catalog):catalog.write(str(ctx['out']/'events.qml'),format='QUAKEML')
    return curated


def window_policy(ctx):
    defaults=dict(policy='reviewed',noise_seconds=5.,detected_guard_s=1.,detected_fallback='fixed',event_end='record_end')
    policy={**defaults,**ctx['config'].get('windows',{})}
    if policy['policy'] not in ['fixed','detected','reviewed']:raise ValueError('Window policy must be fixed, detected or reviewed')
    if not np.isfinite(policy['noise_seconds']) or policy['noise_seconds']<=0:raise ValueError('noise_seconds must be positive')
    if not np.isfinite(policy['detected_guard_s']) or policy['detected_guard_s']<0:raise ValueError('detected_guard_s must be nonnegative')
    if policy['detected_fallback'] not in ['fixed','skip']:raise ValueError('detected_fallback must be fixed or skip')
    if policy['event_end']!='record_end':raise ValueError('Only explicit record_end event windows are currently implemented')
    return policy


def configure_window_policy(analysis_dir, *, policy='fixed', noise_seconds=5., detected_guard_s=1., detected_fallback='fixed'):
    path=Path(analysis_dir)/'analysis_workflow.json';cfg=json.loads(path.read_text())
    value=dict(policy=policy,noise_seconds=float(noise_seconds),detected_guard_s=float(detected_guard_s),
               detected_fallback=detected_fallback,event_end='record_end')
    window_policy(dict(config={'windows':value}))
    if cfg.get('windows')!=value:
        backup=output_path('20_analysis','config_backups',create=True)/('analysis_workflow.before_'+digest(path.read_bytes())[:12]+'.json')
        if not backup.exists():shutil.copy2(path,backup)
        cfg['windows']=value;write_json(path,cfg)
    return cfg


def _automatic_proposals(ctx, policy):
    """Reuse compatible aggregate tables, then calculate only missing records."""
    from pretrigger_qc import analyse_files,ALGORITHM_VERSION
    settings=PretriggerSettings(reference_end_s=policy['noise_seconds'],minimum_noise_s=policy['noise_seconds'],
        noise_guard_s=max(policy['detected_guard_s'],1e-9),compute_frequency=False,compute_hilbert=False)
    engine=importlib.import_module('flovopy.processing.event_windows');engine_sha=digest(Path(engine.__file__).read_bytes())
    desired={k:v for k,v in asdict(settings).items() if k not in ['plot_seconds','compute_frequency','compute_hilbert']}
    wanted=dict(zip(ctx['manifest'].record_id,ctx['manifest'].source_fingerprint));best=None;best_ids=set()
    fields=['trace_key','record_id','trace_index','selected_onset_file_s','proposed_noise_start_file_s',
            'proposed_noise_end_file_s','noise_proposal_status','qc_flags']
    for pointer in (ctx['products']/'notebook_workflow').glob('*/window_proposal_run.json'):
        manifest_path=pointer.parent/'run_manifest.json'
        if not manifest_path.exists():continue
        info=json.loads(pointer.read_text());manifest=json.loads(manifest_path.read_text())
        if manifest.get('flovopy_source_sha256',{}).get('flovopy.processing.event_windows')!=engine_sha:continue
        actual={k:v for k,v in info.get('settings',{}).items() if k not in ['plot_seconds','compute_frequency','compute_hilbert']}
        if actual!=desired:continue
        source={v['record_id']:v['source_fingerprint'] for v in manifest['files']}
        ids={key for key,value in wanted.items() if source.get(key)==value}
        path=resolve_artifact(info['output'])/'trace_qc_and_candidates.csv'
        if len(ids)>len(best_ids) and path.exists():best=path;best_ids=ids
    tables=[]
    if best is not None:
        table=pd.read_csv(best,usecols=lambda k:k in fields,dtype={'record_id':str,'trace_key':str})
        tables.append(table[table.record_id.isin(best_ids)])
    inventory,_=build_inventory(ctx);required=set(inventory.loc[inventory.is_waveform,'record_id'])
    seen=set(tables[0].record_id) if tables else set()
    missing=required-seen
    root=ctx['products']/'pretrigger_file_cache'/digest([ALGORITHM_VERSION,engine_sha,np.__version__,pd.__version__])[:12]
    for item in ctx['manifest'].to_dict('records'):
        if item['record_id'] not in missing:continue
        path=Path(item['file']);stat=path.stat()
        payload=dict(settings=asdict(settings),files=[dict(path=str(path),size=stat.st_size,mtime_ns=stat.st_mtime_ns)],algorithm_version=ALGORITHM_VERSION)
        cached=root/digest(payload)[:12]/'trace_qc_and_candidates.csv'
        try:
            if cached.exists():table=pd.read_csv(cached,usecols=lambda k:k in fields,dtype={'record_id':str,'trace_key':str})
            else:table=analyse_files([path],root,settings)['qc']
            if len(table):tables.append(table)
        except Exception:
            # The explicit fallback policy below decides what to do, and records it.
            continue
    result=pd.concat(tables,ignore_index=True) if tables else pd.DataFrame(columns=fields)
    if len(result) and 'trace_key' not in result:result['trace_key']=result.record_id.astype(str)+':'+result.trace_index.astype(int).astype(str)
    elif len(result):
        missing_key=result.trace_key.isna()
        result.loc[missing_key,'trace_key']=result.loc[missing_key,'record_id'].astype(str)+':'+result.loc[missing_key,'trace_index'].astype(int).astype(str)
    return result


def select_analysis_windows(ctx, *, proposals=None):
    """Select automatic-policy windows without changing human review decisions."""
    policy=window_policy(ctx)
    if policy['policy']=='reviewed':
        windows,issues=validate_review(ctx)
        windows=windows.assign(window_policy='reviewed',window_selection_method='manual',window_flags='',
                               event_end_rule='manual',event_end_record_limited=False)
    else:
        inventory,_=build_inventory(ctx);waveforms=inventory[inventory.is_waveform]
        if policy['policy']=='detected':
            proposals=_automatic_proposals(ctx,policy) if proposals is None else proposals
            lookup=proposals.set_index('trace_key').to_dict('index') if len(proposals) else {}
        else:lookup={}
        rows=[];issues=[];minimum=ctx['config']['processing']['minimum_window_s']
        for meta in waveforms.to_dict('records'):
            offset=float(meta['trace_start_offset_file_s']);end=offset+float(meta['duration_s'])
            noise_start=offset;noise_end=offset+policy['noise_seconds'];event_start=noise_end
            method='fixed';flags=[]
            if policy['policy']=='detected':
                candidate=lookup.get(meta['trace_key'],{})
                status=candidate.get('noise_proposal_status','missing_detection')
                ns=pd.to_numeric(candidate.get('proposed_noise_start_file_s'),errors='coerce')
                ne=pd.to_numeric(candidate.get('proposed_noise_end_file_s'),errors='coerce')
                onset=pd.to_numeric(candidate.get('selected_onset_file_s'),errors='coerce')
                valid=(status in ['EXTENDED_NETWORK_SUPPORTED_REVIEW','SHORTENED_FOR_EARLIER_ACTIVITY_REVIEW']
                    and np.isfinite([ns,ne,onset]).all() and ns>=offset-1e-7 and ne<=onset-policy['detected_guard_s']+1e-7
                    and ne-ns>=minimum and onset>=offset and onset<end)
                if valid:
                    noise_start,noise_end,event_start=float(ns),float(ne),float(onset);method='detected'
                    if status=='SHORTENED_FOR_EARLIER_ACTIVITY_REVIEW':flags.append('noise_shortened_for_earlier_activity')
                    if candidate.get('qc_flags'):flags.append('detector:'+str(candidate['qc_flags']))
                else:
                    flags.append('unresolved_detection:'+str(status))
                    if policy['detected_fallback']=='skip':
                        issues.append(dict(trace_key=meta['trace_key'],severity='info',reason='unresolved_detection_skipped'));continue
                    method='fixed_fallback'
            for label,start,stop in [('noise',noise_start,noise_end),('event',event_start,end)]:
                if start<offset-1e-7 or stop>end+1e-7 or stop-start<minimum:
                    issues.append(dict(trace_key=meta['trace_key'],severity='info',reason=label+'_out_of_support_or_too_short'));continue
                rows.append(dict(trace_key=meta['trace_key'],window=label,window_start_file_s=start,window_end_file_s=stop,
                    window_policy=policy['policy'],window_selection_method=method,window_flags=';'.join(flags),
                    event_end_rule='record_end' if label=='event' else 'noise_boundary',event_end_record_limited=label=='event'))
        windows=pd.DataFrame(rows,columns=['trace_key','window','window_start_file_s','window_end_file_s','window_policy',
            'window_selection_method','window_flags','event_end_rule','event_end_record_limited'])
        issues=pd.DataFrame(issues,columns=['trace_key','severity','reason'])
    windows.to_csv(ctx['out']/'selected_windows.csv',index=False)
    issues.to_csv(ctx['out']/'window_selection_issues.csv',index=False)
    write_json(ctx['out']/'window_policy.json',policy)
    return windows,issues


def compute_metrics(ctx, *, force=False, write_quakeml=True):
    windows,issues=select_analysis_windows(ctx)
    active=dict(ctx);active['_active_window_policy']=window_policy(ctx)
    if active['_active_window_policy']['policy']=='detected':
        module=importlib.import_module('flovopy.processing.event_windows')
        active['_active_window_policy']={**active['_active_window_policy'],'detector_sha256':digest(Path(module.__file__).read_bytes())}
    return compute_reviewed_metrics(active,force=force,write_quakeml=write_quakeml,selected_windows=windows)


def compute_reviewed_metrics(ctx, *, force=False, write_quakeml=True, selected_windows=None):
    """Compute only new/changed files; persist all outputs in EnhancedEvent JSON.

    Each waveform file is a recording record, not an inferred earthquake origin.
    Caches live outside run-ID directories, with independent source/settings/
    accepted-window signatures. Run indexes select exactly one active version.
    """
    from flovopy.enhanced.event import EnhancedEvent, EnhancedEventMeta
    from flovopy.enhanced.catalog import EnhancedCatalog
    from flovopy.enhanced.serialization import json_compatible
    from obspy.core.event import Comment
    if selected_windows is None:approved,issues=validate_review(ctx)
    else:approved=selected_windows.copy();issues=pd.DataFrame()
    inventory,_=build_inventory(ctx)
    waveform=inventory[inventory.is_waveform];lookup=waveform.set_index('trace_key')
    current_review=pd.read_csv(ctx['out']/'window_review.csv').fillna('') if (ctx['out']/'window_review.csv').exists() else pd.DataFrame()
    trace_groups=waveform.groupby('record_id',sort=False).indices
    approved=approved.assign(record_id=approved.trace_key.map(lookup.record_id))
    window_groups=approved.groupby('record_id',sort=False).indices
    if len(current_review):current_review=current_review.assign(record_id=current_review.trace_key.map(lookup.record_id))
    review_groups=current_review.groupby('record_id',sort=False).indices if len(current_review) else {}
    index=[];errors=[];events=[]
    for file_number,item in enumerate(ctx['manifest'].to_dict('records'),1):
        rid=item['record_id'];meta_rows=waveform.iloc[trace_groups.get(rid,[])]
        windows=approved.iloc[window_groups.get(rid,[])]
        signature=_metric_signature(ctx,item,windows,meta_rows);key=digest(signature)
        base=ctx['products']/'event_metrics_cache'/rid/key/'event'
        path=base.with_suffix('.json')
        review_snapshot=current_review.iloc[review_groups.get(rid,[])].to_dict('records') if len(current_review) else []
        try:
            reused=False
            if path.exists() and not force:
                try:
                    cached=EnhancedEvent.load(str(base),require_quakeml=False)
                    if cached.meta.metrics.get('computation_signature')==signature and cached.meta.metrics.get('status')!='partial_error':
                        event=cached;reused=True
                except (OSError,ValueError,TypeError,AttributeError):
                    # A broken cache is not a failed measurement; reconstruct it.
                    reused=False
            if not reused:
                event=EnhancedEvent(resource_id=f'smi:local/pinatubo-record/{rid}',
                    meta=EnhancedEventMeta(wav_paths=[item['file']]))
                event.comments.append(Comment(text='Waveform-record metadata only. No earthquake origin, location or magnitude inferred.'))
                origin=min((UTCDateTime(t) for t in meta_rows.recorded_starttime),default=None)
                event.meta.metrics=dict(computation_signature=signature,source=item,
                    record_reference_recorded_time=str(origin) if origin is not None else None,
                    status='complete' if len(windows) else 'no_selected_windows',
                    window_selection_policy=ctx.get('_active_window_policy',{'policy':'reviewed'}),
                    inventory=json_compatible(meta_rows.to_dict('records')),review_decisions=json_compatible(review_snapshot),errors=[])
                if len(windows):
                    traces=waveform_traces(read(item['file']))
                    origin=min(tr.stats.starttime for tr in traces)
                    for trace_key,selected in windows.groupby('trace_key'):
                        metadata=lookup.loc[trace_key];tr=traces[int(metadata.trace_index)]
                        try:
                            measured,payloads=measure_trace_windows(tr,selected.to_dict('records'),origin,ctx['config'],return_payload=True)
                            for row,payload in zip(measured,payloads):
                                for field in ['trace_key','record_id','trace_index','file','trace_id','station','channel',
                                              'recorded_starttime','gain_epoch_id','attenuation_db','epoch_status']:
                                    row[field]=trace_key if field=='trace_key' else metadata[field]
                                if str(metadata.qc_flags) not in ['','nan']:
                                    row['metric_flags']=';'.join(filter(None,[row['metric_flags'],'raw_trace:'+str(metadata.qc_flags)]))
                                event.meta.traces.append(dict(id=tr.id,trace_key=trace_key,trace_index=int(metadata.trace_index),
                                    window=row['window'],header=dict(network=tr.stats.network,station=tr.stats.station,
                                    location=tr.stats.location,channel=tr.stats.channel,starttime=str(tr.stats.starttime),
                                    sampling_rate_hz=float(tr.stats.sampling_rate),units='counts'),**payload))
                        except Exception as exc:
                            event.meta.metrics['errors'].append(dict(trace_key=trace_key,error=f'{type(exc).__name__}: {exc}'))
                            event.meta.metrics['status']='partial_error'
                event.save(str(base.parent),base.name,write_quakeml=False,include_trajectory_in_sidecar=False)
            elif event.meta.metrics.get('review_decisions')!=json_compatible(review_snapshot):
                # Notes/status metadata can change without recalculating numerical features.
                event.meta.metrics['review_decisions']=json_compatible(review_snapshot)
                event.save(str(base.parent),base.name,write_quakeml=False,include_trajectory_in_sidecar=False)
            errors.extend(event.meta.metrics.get('errors',[]))
            index.append(dict(record_id=rid,file=item['file'],sidecar_path=str(path),signature=key,
                status='reused' if reused else ('computed' if len(windows) else 'metadata_only'),
                window_rows=len(event.meta.traces),source_fingerprint=item['source_fingerprint']))
        except Exception as exc:
            errors.append(dict(trace_key=rid,error=f'{type(exc).__name__}: {exc}'))
            index.append(dict(record_id=rid,file=item['file'],sidecar_path=str(path),signature=key,status='failed',window_rows=0))
        if file_number%500==0 or file_number==len(ctx['manifest']):
            counts=pd.DataFrame(index).status.value_counts().to_dict()
            print(f'Event metric cache: {file_number}/{len(ctx["manifest"])} files; {counts}',flush=True)
            write_json(ctx['out']/'metric_cache_progress.json',dict(completed=file_number,total=len(ctx['manifest']),counts=counts))
    pd.DataFrame(index,columns=['record_id','file','sidecar_path','signature','status','window_rows','source_fingerprint']).to_csv(ctx['out']/'event_metrics_index.csv',index=False)
    metrics=export_event_metrics(ctx,write_quakeml=write_quakeml)
    pd.DataFrame(errors,columns=['trace_key','error']).to_csv(ctx['out']/'metric_errors.csv',index=False)
    write_json(ctx['out']/'metric_provenance.json',dict(version='pinatubo-full-event-metrics-1',feature_version=FEATURE_VERSION,
        config=ctx['config'],window_policy=ctx.get('_active_window_policy',{'policy':'reviewed'}),review_sha256=digest((ctx['out']/'window_review.csv').read_bytes()) if (ctx['out']/'window_review.csv').exists() else None,
        selected_windows=len(approved),approved_windows=len(approved) if ctx.get('_active_window_policy',{'policy':'reviewed'})['policy']=='reviewed' else 0,metric_rows=len(metrics),errors=len(errors),
        cache_counts=pd.DataFrame(index).status.value_counts().to_dict() if index else {},
        source_of_truth='EnhancedEvent JSON sidecars selected by event_metrics_index.csv',
        preprocessing='Independent-window linear detrend; optional lowpass; complete EnhancedStream + feature trees retained',
        units='counts; no physical calibration or UTC correction'))
    return metrics,pd.DataFrame(errors,columns=['trace_key','error'])


def load_example(ctx, trace_key=None):
    inventory,_=build_inventory(ctx);choices=inventory[inventory.is_waveform]
    if trace_key is not None:choices=choices[choices.trace_key.eq(trace_key)]
    if choices.empty:raise ValueError('No matching waveform trace in this selection')
    meta=choices.iloc[0];tr=waveform_traces(read(meta.file))[int(meta.trace_index)]
    tr.stats.units='counts'
    return tr,meta


def multitaper_spectrogram(trace, fft_seconds, step_seconds, nw):
    """Gap-rejecting density-normalized DPSS spectrogram; no legacy dB scaling."""
    x=finite_data(trace);fs=float(trace.stats.sampling_rate);n=int(round(fs*fft_seconds));step=max(1,int(round(fs*step_seconds)))
    if n<4 or n>len(x):raise ValueError('FFT window does not fit the trace')
    k=max(1,int(2*nw)-1);tapers=dpss(n,NW=nw,Kmax=k,sym=False)
    offsets=np.arange(0,len(x)-n+1,step);f=np.fft.rfftfreq(n,1/fs)
    values=np.full((len(f),len(offsets)),np.nan);coverage=np.zeros(len(offsets))
    for j,a in enumerate(offsets):
        block=x[a:a+n];coverage[j]=np.isfinite(block).mean()
        if coverage[j]<1:continue
        block=block-block.mean()
        values[:,j]=np.mean([periodogram(block,fs=fs,window=taper,nfft=n,detrend=False,
                              scaling='density',return_onesided=True)[1] for taper in tapers],axis=0)
    return SpectrogramResult(trace.id,float(trace.stats.starttime.timestamp)+(offsets+(n-1)/2)/fs,
        f,values,coverage,quantity='psd',units='counts²/Hz',metadata=dict(fft_seconds=n/fs,
        step_seconds=step/fs,nw=nw,tapers=k,estimator='equal_weight_DPSS_density',bandwidth_hz=2*nw/(n/fs)))


def spectral_examples(ctx, trace_key=None, stem_prefix='spectral_example', caption_prefix='Exploratory example.'):
    trace,meta=load_example(ctx,trace_key);fs=float(trace.stats.sampling_rate)
    approved,_=select_analysis_windows(ctx);selected=approved[approved.trace_key.eq(meta.trace_key)]
    paired=set(selected.window)=={'noise','event'}
    origin=UTCDateTime(meta.record_reference_time)
    if paired:
        labels=['noise','event'];cuts=[]
        for label in labels:
            row=selected[selected.window.eq(label)].iloc[0]
            cuts.append(processed_copy(cut_half_open(trace,row.window_start_file_s,row.window_end_file_s,origin),ctx['config']))
    else:
        labels=['whole_record_unreviewed'];cuts=[trace.copy()]
        # Whole-record diagnostics may contain gaps; PSD comparison cannot bridge them.
        if np.isfinite(finite_data(trace)).all():cuts=[processed_copy(trace,ctx['config'])]
    out=ctx['out']/'figures';base=f'{stem_prefix}_{meta.record_id}_{int(meta.trace_index):02d}'
    table=[];paths=[];figs=[]
    try:
        spectra=matched_psds(cuts,ctx['config'])
        fig,ax=plt.subplots(figsize=(7.2,3),layout='constrained')
        for label,spec in zip(labels,spectra):
            for method,linestyle in [('welch_psd','-'),('multitaper_psd','--')]:
                p=spec[method];positive=(spec['frequency_hz']>0)&(p>0)
                ax.plot(spec['frequency_hz'][positive],10*np.log10(p[positive]),ls=linestyle,lw=.9,label=f'{label}: {method}')
                table.extend(dict(window=label,method=method,frequency_hz=float(f),psd_counts2_per_hz=float(v)) for f,v in zip(spec['frequency_hz'],p))
        ax.set(xscale='log',xlim=ctx['config']['plots']['frequency_limits_hz'],xlabel='Frequency (Hz)',
               ylabel='PSD (dB re 1 count²/Hz)',title=f'{trace.id} — matched segment lengths and frequency grids')
        ax.legend();ax.grid(alpha=.15)
        caption=caption_prefix+f' {trace.id}; '+('policy-selected noise/event intervals.' if paired else 'whole-record estimator comparison, not policy-selected noise/event windows.')+f" Hann-Welch and equal-weight DPSS PSD use {spectra[0]['segment_s']:.3g}-s segments, Δf={spectra[0]['df_hz']:.3g} Hz; DPSS NW={spectra[0]['nw']}, K={spectra[0]['tapers']}, approximate full smoothing bandwidth {spectra[0]['multitaper_bandwidth_hz']:.3g} Hz. Segment counts may differ between windows. Counts are uncalibrated."
        paths.append(save_figure(fig,out/(base+'_psd'),caption,ctx['config']['plots']['dpi']));figs.append(fig)
        pd.DataFrame(table).to_csv(ctx['out']/(base+'_psd.csv'),index=False)
    except ValueError as exc:
        write_json(ctx['out']/(base+'_psd_unavailable.json'),dict(reason=str(exc)))
    results=[];titles=[]
    for requested in ctx['config']['spectra']['fft_windows_s']:
        length=max(2,round(requested*fs))/fs;step=max(1,round(ctx['config']['spectra']['step_s']*fs))/fs
        if length>len(trace)/fs:continue
        result=compute_spectrogram_result(trace,fft_seconds=length,step_seconds=step,align='start',
                                         window='hann',quantity='psd',detrend='constant',units='counts')
        results.append(result);titles.append(f'Hann PSD: {length:g} s; Δf={1/length:.3g} Hz')
    if not results:raise ValueError('No FFT window fits the selected example')
    length=max(4,round(ctx['config']['spectra']['segment_s']*fs))/fs
    if length<=len(trace)/fs:
        mt=multitaper_spectrogram(trace,length,ctx['config']['spectra']['step_s'],ctx['config']['spectra']['multitaper_nw'])
        results.append(mt);titles.append(f'DPSS PSD: {length:g} s; NW={mt.metadata["nw"]}, K={mt.metadata["tapers"]}')
    fig,axes=plt.subplots(1+len(results),1,figsize=(7.2,2+1.35*len(results)),sharex=True,layout='constrained')
    t=np.arange(len(trace))/fs;axes[0].plot(t,finite_data(trace),color=station_color(meta.station),lw=.5)
    axes[0].set(ylabel='Counts',title=f'{trace.id} — recorded {trace.stats.starttime}; clock uncorrected')
    if paired:
        offset=float(trace.stats.starttime-origin)
        for row in selected.itertuples():
            axes[0].axvspan(row.window_start_file_s-offset,row.window_end_file_s-offset,
                           color='#228833' if row.window=='noise' else '#eecc66',alpha=.25,label='Selected '+row.window)
        axes[0].legend(loc='upper right')
    lo,hi=ctx['config']['plots']['frequency_limits_hz'];clim=ctx['config']['plots']['psd_db_limits']
    for ax,result,title in zip(axes[1:],results,titles):
        with np.errstate(divide='ignore',invalid='ignore'):z=10*np.log10(result.values)
        x=result.times-float(trace.stats.starttime.timestamp)
        mesh=ax.pcolormesh(x,result.frequencies,np.ma.masked_invalid(z),shading='auto',cmap='viridis',
                            vmin=clim[0],vmax=clim[1],rasterized=True)
        ax.set(ylim=(lo,min(hi,fs/2)),ylabel='Hz');ax.text(.01,.94,title,transform=ax.transAxes,va='top',
            fontsize=7,bbox=dict(facecolor='white',edgecolor='none',alpha=.85,pad=1))
    axes[-1].set_xlabel('Seconds from recorded trace start')
    fig.colorbar(mesh,ax=list(axes[1:]),label='PSD (dB re 1 count²/Hz)',shrink=.8,pad=.02)
    caption=caption_prefix+f' {trace.id}. Same PSD units and fixed colour limits ({clim[0]:g}, {clim[1]:g} dB re 1 count²/Hz) across panels; values outside are colour-clipped only. Complete windows only; gaps remain missing. Window centres set times. Step is {ctx["config"]["spectra"]["step_s"]:g} s before sample rounding. Longer windows trade timing for frequency discrimination. Greater overlap and denser FFT bins do not add independent resolution. '+('Shading shows policy-selected intervals.' if paired else 'No policy-selected noise/event pair: whole-record example only.')
    paths.append(save_figure(fig,out/(base+'_spectrograms'),caption,ctx['config']['plots']['dpi']));figs.append(fig)
    for i,result in enumerate(results):
        np.savez_compressed(ctx['out']/f'{base}_spectrogram_{i}.npz',times_recorded_unix_s=result.times,
            frequency_hz=result.frequencies,psd_counts2_per_hz=result.values,coverage=result.coverage)
    # FLOVOpy spectral descriptors of the middle Hann grid, with explicit band definitions.
    chosen=results[min(1,len(results)-1)]
    high=ctx['config']['processing']['hf_band']
    if high[1]<=chosen.frequencies[-1]:
        metrics=compute_spectral_metrics(chosen,low_band=ctx['config']['processing']['lf_band'],high_band=high)
        metrics['time_trace_s']=metrics.time-float(trace.stats.starttime.timestamp)
        metrics.to_csv(ctx['out']/(base+'_spectral_evolution.csv'),index=False)
    write_json(ctx['out']/(base+'_settings.json'),dict(trace_key=meta.trace_key,source_fingerprint=meta.source_fingerprint,
        accepted_pair=paired,config=ctx['config'],spectrogram_metadata=[r.metadata for r in results]))
    return paths,figs


def load_metrics(ctx, require_current=True):
    path=ctx['out']/'window_metrics.csv'
    if not path.exists():return pd.DataFrame(columns=METRIC_FIELDS)
    if require_current:
        provenance=json.loads((ctx['out']/'metric_provenance.json').read_text())
        review=ctx['out']/'window_review.csv'
        current=digest(review.read_bytes()) if review.exists() else None
        policy=provenance.get('window_policy',{'policy':'reviewed'})
        if policy['policy']=='reviewed' and provenance['review_sha256']!=current:raise ValueError('Review decisions changed: rerun notebook 30 before summarizing metrics')
        if policy['policy']!='reviewed' and any(policy.get(k)!=v for k,v in window_policy(ctx).items()):raise ValueError('Window policy changed: rerun notebook 30')
    return pd.read_csv(path)


def plot_metric_summary(ctx, metrics=None, stem='accepted_window_metrics'):
    metrics=load_metrics(ctx) if metrics is None else metrics
    fig,axes=plt.subplots(1,2,figsize=(7.2,3.2),layout='constrained')
    if metrics.empty:
        for ax in axes:
            ax.axis('off');ax.text(.5,.5,'No selected-window metrics yet.\nChoose a window policy,\nthen run notebook 30.',ha='center',va='center',transform=ax.transAxes)
    else:
        for (trace_id,epoch,label),group in metrics.groupby(['trace_id','gain_epoch_id','window'],dropna=False):
            group=group.copy();time=pd.to_datetime(group.recorded_starttime,utc=True)
            color=station_color(group.station.iloc[0]);rms=pd.to_numeric(group['amplitude.rms_counts'],errors='coerce')
            valid=rms.gt(0)&np.isfinite(rms)
            axes[0].scatter(time[valid],rms[valid],s=14,c=[color],marker='o' if label=='event' else 'x',
                            label=f'{trace_id} {label}')
            centroid=pd.to_numeric(group['psd.centroid_hz'],errors='coerce')
            axes[1].scatter(rms[valid],centroid[valid],s=14,c=[color],marker='o' if label=='event' else 'x')
        axes[0].set(yscale='log',ylabel='Window RMS (counts)',xlabel='Recorded date (uncorrected)')
        axes[1].set(xscale='log',xlabel='Window RMS (counts)',ylabel='Welch PSD centroid (Hz)')
        fig.autofmt_xdate(rotation=30)
    caption='Policy-selected window descriptive metrics for the configured selection. Trigger-preceding noise is a selected sample, not an unbiased ambient-noise survey. Comparisons require matching station/channel and gain epoch; unknown electronics prevent physical cross-station amplitude comparison. Energy durations describe each selected window and may be censored by record bounds.'
    save_figure(fig,ctx['out']/'figures'/stem,caption,ctx['config']['plots']['dpi'])
    return fig


def archive_summary(ctx):
    inventory,files=build_inventory(ctx);metrics=load_metrics(ctx)
    waveform=inventory[inventory.is_waveform]
    summary=dict(scope=ctx['scope'],selected_files=len(ctx['files']),read_ok=int(files.status.eq('read_ok').sum()),
        waveform_traces=len(waveform),unique_channels=int(waveform.trace_id.nunique()),
        accepted_metric_rows=len(metrics),response_units='counts',recorded_clock_correction='NONE')
    write_json(ctx['out']/'archive_summary.json',summary)
    # Count each trace once per QC flag; window/feature flags are separate populations.
    flags=[]
    for population,frame,columns in [('raw_trace',waveform,['qc_flags']),('accepted_window',metrics,['metric_flags','feature_flags'])]:
        for row in frame.to_dict('records'):
            values=set()
            for col in columns:values.update(v for v in str(row.get(col,'')).split(';') if v and v!='nan')
            flags.extend(dict(population=population,flag=v) for v in values)
    counts=pd.DataFrame(flags,columns=['population','flag'])
    if len(counts):counts=counts.groupby(['population','flag']).size().rename('count').reset_index()
    else:counts['count']=pd.Series(dtype=int)
    counts.to_csv(ctx['out']/'qc_flag_counts.csv',index=False)
    return summary,counts


def publication_figures(ctx):
    """Explicit frozen draft selection; never silently choose different examples."""
    selection_path=ctx['products']/'publication/publication_selection.json'
    if not selection_path.exists() and (ctx['base']/'publication_selection.json').exists():
        selection_path.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(ctx['base']/'publication_selection.json',selection_path)
    if not selection_path.exists():
        inventory,_=build_inventory(ctx);waveform=inventory[inventory.is_waveform]
        selected=waveform.groupby('record_id',sort=False).head(1).head(ctx['config']['plots']['max_examples'])
        write_json(selection_path,dict(status='DRAFT_REQUIRES_AUTHOR_SELECTION',run_id=ctx['run_id'],
            example_traces=[dict(trace_key=row.trace_key,source_fingerprint=row.source_fingerprint) for row in selected.itertuples()],
            style=ctx['config']['plots'],spectra=ctx['config']['spectra']))
    selection=json.loads(selection_path.read_text())
    if selection['run_id']!=ctx['run_id']:raise ValueError('Publication selection belongs to another run. Review/update publication_selection.json explicitly.')
    if selection['style']!=ctx['config']['plots'] or selection['spectra']!=ctx['config']['spectra']:
        raise ValueError('Frozen figure settings differ from current configuration')
    inventory,_=build_inventory(ctx);lookup=inventory.set_index('trace_key')
    dest=ctx['out']/'publication';dest.mkdir(exist_ok=True);assets=[]
    fig=plot_inventory(ctx,inventory);plt.close(fig)
    fig=plot_metric_summary(ctx);plt.close(fig)
    for item in selection['example_traces']:
        if item['trace_key'] not in lookup.index or lookup.loc[item['trace_key'],'source_fingerprint']!=item['source_fingerprint']:
            raise ValueError('Publication example changed or is unavailable')
        paths,figs=spectral_examples(ctx,item['trace_key'],stem_prefix='publication',caption_prefix='DRAFT: fixed representative selection; author approval pending.')
        for fig in figs:plt.close(fig)
    # Copy generated vector/raster figures with their captions, including existing historical maps.
    for path in (ctx['out']/'figures').glob('*'):
        if path.name.startswith('publication_') or any(path.name.startswith(stem+'.') for stem in ['archive_inventory','accepted_window_metrics']):
            shutil.copy2(path,dest/path.name);assets.append(path.name)
    history=ctx['out']/'network_history'
    for stem in ['network_early_june','network_july_august','telemetry','pvo_operations']:
        for suffix in ['png','svg']:
            path=history/f'{stem}.{suffix}'
            if path.exists():shutil.copy2(path,dest/path.name);assets.append(path.name)
        assets.append(f'{stem}.caption.md')
        (dest/f'{stem}.caption.md').write_text('Historical evidence draft reproduced from network_history. Modern relief is not the 1991 surface; deployment bounds, coordinates and electronics remain provisional. See chronology and timing ledger.\n')
    write_json(dest/'figure_manifest.json',dict(status=selection['status'],run_id=ctx['run_id'],selection=selection,
        files={name:digest((dest/name).read_bytes()) for name in sorted(set(assets))},
        review_sha256=digest((ctx['out']/'window_review.csv').read_bytes()) if (ctx['out']/'window_review.csv').exists() else None))
    return dest,pd.DataFrame({'figure_file':sorted(set(assets))})


def schema_table():
    rows=[]
    for field in METRIC_FIELDS:
        units='text or identifier';definition='Provenance or quality-control metadata.'
        if field=='event_end_record_limited':
            units='boolean';definition='Selected event data reaches the stored record boundary; not a picked physical coda end.'
        elif field in ['window_npts','welch_segment_count','trace_index']:
            units='samples' if field=='window_npts' else 'integer';definition='Explicit count; not a duration or amplitude.'
        elif field.endswith('_file_s') or field in ['window_duration_s','psd_segment_s']:
            units='s';definition='Recorded file-relative time or measured support duration; half-open sample selection.'
        elif field=='attenuation_db':units='dB';definition='Documented nominal attenuation; unknown electronics remain uncalibrated.'
        elif field.startswith('amplitude.'):
            units='counts² s' if 'energy' in field else ('1' if field.endswith(('skewness','kurtosis')) else 'counts')
            definition='Descriptive statistic of linearly detrended selected samples; energy is sum(x²)/fs, not joules.'
        elif field.startswith(('sam.','sem.')):
            units='1' if field.endswith('ratio') else ('counts² s' if field.startswith('sem.') else 'counts')
            definition='FLOVOpy zero-phase bandpassed mean absolute amplitude (SAM) or sum(x²)/fs (SEM); short bands masked using conservative edge support. Ratios are log2(HF/LF).'
        elif field.startswith('psd.'):
            units='Hz' if field.endswith('_hz') else ('counts²' if field.endswith('_counts2') else '1')
            definition='FLOVOpy PSD descriptor from Hann-Welch density; centroid/bandwidth/peak use the full DC–Nyquist grid; LF/HF powers use configured bands.'
        elif field.startswith('feature.'):
            key=field[len('feature.'):];units='1'
            if key.endswith('_s') or (key.startswith('cepstral_squared.') and key.endswith(('centroid','spread'))):units='s'
            if key.endswith('_hz') or (key.startswith('spectral_power.') and key.endswith(('centroid','spread'))):units='Hz'
            if key.endswith('per_s'):units='s⁻¹'
            definition='FLOVOpy features v'+FEATURE_VERSION+'; periodogram/shape definitions retained. Timing is relative to the selected window, not an acquisition trigger.'
        elif field.startswith('artifact_'):
            units='Hz' if field.endswith('_hz') else ('samples' if field.endswith('nperseg') else '1 or status')
            definition='40.5-Hz narrowband screen before/after processing. Detected is missing when not testable; no instrumental attribution is implied.'
        elif field in ['event_to_noise_rms_ratio','event_to_noise_rms_db']:
            units='dB' if field.endswith('_db') else '1';definition='Paired event/noise RMS ratio; dB = 20 log10(ratio). Missing for invalid/absent pairs.'
        rows.append(dict(column=field,units=units,definition=definition))
    return pd.DataFrame(rows)

def noise_window_time_tables(ctx, qc, review=None):
    """Trace lengths and equal-station file summaries; minimum-only is not a pick."""
    inventory,_=build_inventory(ctx)
    if qc.empty:raise ValueError('No analysed traces to plot; inspect the file inventory/read failures')
    table=qc.copy()
    if 'trace_key' not in table:table['trace_key']=table.record_id.astype(str)+':'+table.trace_index.astype(int).astype(str)
    meta=inventory[inventory.is_waveform][['trace_key','station','network','channel','source_fingerprint']]
    table=table.merge(meta,on='trace_key',validate='one_to_one')
    table['recorded_event_time']=pd.to_datetime(table.file_reference_recorded_time,utc=True,errors='coerce')
    table['noise_length_s']=pd.to_numeric(table.proposed_noise_end_trace_s,errors='coerce')-pd.to_numeric(table.proposed_noise_start_trace_s,errors='coerce')
    table['proposal_class']='invalid_or_unresolved'
    table.loc[table.noise_proposal_status.str.startswith('MINIMUM_ONLY',na=False),'proposal_class']='minimum_only'
    finite=np.isfinite(table.noise_length_s)
    table.loc[finite&table.noise_proposal_status.eq('EXTENDED_NETWORK_SUPPORTED_REVIEW'),'proposal_class']='supported_extension'
    table.loc[finite&table.noise_proposal_status.eq('SHORTENED_FOR_EARLIER_ACTIVITY_REVIEW'),'proposal_class']='shortened_for_activity'
    table['supported_noise_proposal']=table.proposal_class.isin(['supported_extension','shortened_for_activity'])
    if review is not None and not review.empty:
        table=table.merge(review[['trace_key','review_status',*APPROVAL_FIELDS]],on='trace_key',how='left',validate='one_to_one')
        accepted=table.review_status.eq('ACCEPTED')
        table['accepted_noise_length_s']=(pd.to_numeric(table.accepted_noise_end_file_s,errors='coerce')-pd.to_numeric(table.accepted_noise_start_file_s,errors='coerce')).where(accepted)
    records=[]
    for rid,group in table.groupby('record_id',sort=False):
        supported=group[group.supported_noise_proposal]
        # Collapse components first so a three-component station gets one vote.
        station_lengths=supported.groupby(['network','station'],dropna=False).noise_length_s.median()
        records.append(dict(record_id=rid,recorded_event_time=group.recorded_event_time.iloc[0],trace_count=len(group),
            supported_traces=len(supported),supported_stations=len(station_lengths),
            minimum_only_traces=int(group.proposal_class.eq('minimum_only').sum()),
            invalid_traces=int(group.proposal_class.eq('invalid_or_unresolved').sum()),
            noise_station_median_s=float(station_lengths.median()) if len(station_lengths) else np.nan,
            noise_station_p10_s=float(station_lengths.quantile(.1)) if len(station_lengths) else np.nan,
            noise_station_p90_s=float(station_lengths.quantile(.9)) if len(station_lengths) else np.nan,
            earliest_selected_onset_file_s=pd.to_numeric(group.selected_onset_file_s,errors='coerce').min()))
    events=pd.DataFrame(records).sort_values('recorded_event_time')
    table.to_csv(ctx['out']/'noise_windows_through_time.csv',index=False)
    events.to_csv(ctx['out']/'event_noise_window_summary.csv',index=False)
    return table,events


def plot_noise_windows_through_time(ctx, qc, review=None):
    from matplotlib.lines import Line2D
    from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
    table,events=noise_window_time_tables(ctx,qc,review)
    fig,axes=plt.subplots(3,1,figsize=(8.5,7.8),sharex=True,layout='constrained',
                          gridspec_kw={'height_ratios':[2,2,1.2]})
    supported=table[table.supported_noise_proposal&table.recorded_event_time.notna()]
    for station,group in supported.groupby('station'):
        axes[0].scatter(group.recorded_event_time,group.noise_length_s,s=9,alpha=.5,
                        color=station_color(station),label=station,rasterized=True)
    minimum=table[table.proposal_class.eq('minimum_only')&table.recorded_event_time.notna()]
    axes[0].scatter(minimum.recorded_event_time,minimum.noise_length_s,s=8,c='0.65',marker='x',alpha=.3,rasterized=True)
    if 'accepted_noise_length_s' in table:
        accepted=table[table.accepted_noise_length_s.notna()]
        axes[0].scatter(accepted.recorded_event_time,accepted.accepted_noise_length_s,s=18,facecolors='none',edgecolors='black',marker='D')
    axes[0].set(ylabel='Proposed noise length (s)',title='Per-trace noise prefixes; grey × = assumed minimum only')
    if len(supported):axes[0].legend(ncol=min(7,supported.station.nunique()),fontsize=6,loc='upper right',framealpha=.85)
    finite=events.noise_station_median_s.notna()&events.recorded_event_time.notna();group=events[finite]
    if len(group):
        axes[1].vlines(group.recorded_event_time,group.noise_station_p10_s,group.noise_station_p90_s,color='#4477aa',alpha=.25,lw=.7)
        axes[1].scatter(group.recorded_event_time,group.noise_station_median_s,s=10,color='#4477aa',label='Noise median across stations')
    arrivals=events[events.earliest_selected_onset_file_s.notna()&events.recorded_event_time.notna()]
    axes[1].scatter(arrivals.recorded_event_time,arrivals.earliest_selected_onset_file_s,s=12,marker='^',color='#cc6677',alpha=.6,label='Earliest selected onset from file start')
    axes[1].set(ylabel='Seconds',title='One point per file; blue bars = 10–90% station spread, not confidence limits')
    axes[1].legend(fontsize=7,loc='upper right')
    for ax in axes[:2]:
        for value in [5,10]:ax.axhline(value,color='0.35',ls='--',lw=.6,zorder=0)
        ax.set_ylim(bottom=0);ax.grid(alpha=.12)
    dated=table[table.recorded_event_time.notna()].copy()
    dated['week']=dated.recorded_event_time.dt.normalize()-pd.to_timedelta(dated.recorded_event_time.dt.weekday,unit='D')
    counts=dated.groupby(['week','proposal_class']).size().unstack(fill_value=0).sort_index()
    bottom=np.zeros(len(counts))
    for name,color,label in [('supported_extension','#4477aa','Supported extension'),('shortened_for_activity','#eeaa33','Shortened'),
                              ('minimum_only','0.7','Minimum only'),('invalid_or_unresolved','#cc6677','Invalid/unresolved')]:
        values=counts[name].to_numpy() if name in counts else np.zeros(len(counts))
        axes[2].bar(counts.index,values,bottom=bottom,width=5.5,color=color,label=label);bottom+=values
    axes[2].set(ylabel='Traces / week',xlabel='Recorded event-file date (uncorrected clock)')
    axes[2].legend(ncol=4,fontsize=6,loc='upper left')
    locator=AutoDateLocator();axes[2].xaxis.set_major_locator(locator);axes[2].xaxis.set_major_formatter(ConciseDateFormatter(locator))
    fig.suptitle(f"Noise-window proposals through time — {len(events)} analysed files, {len(table)} traces")
    caption='Proposed initial-noise lengths, not measured acquisition pretrigger settings. Supported/shortened windows are unreviewed unless explicitly accepted. Grey minimum-only values are assumptions and are excluded from file medians. Components are collapsed to one median per station before file summaries; station spread is not picking uncertainty. Earliest selected onset uses file-relative time and remains distinct from trace-relative noise length. Noise length includes the guard and may be shortened by earlier activity. Dashed 5/10-s lines are visual references, not fitted detector settings. Dates use uncorrected recorded clocks. Files without successful trace analysis are reported separately in file_inventory.csv.'
    save_figure(fig,ctx['out']/'figures/noise_windows_through_time',caption,ctx['config']['plots']['dpi'])
    return fig,table,events

def _lightweight_catalog(paths):
    from flovopy.enhanced.event import EnhancedEvent
    from flovopy.enhanced.catalog import EnhancedCatalog
    events=[]
    for path in paths:
        event=EnhancedEvent.load(str(Path(path).with_suffix('')),require_quakeml=False)
        event.meta.traces=[]
        event.meta.metrics={key:value for key,value in event.meta.metrics.items() if key in
            ['record_reference_recorded_time','status','classification','event_type','mainclass','subclass']}
        events.append(event)
    return EnhancedCatalog(events=events,records=events)


def _stream_event_exports(ctx, *, write_quakeml=True):
    import csv
    from flovopy.enhanced.catalog import EnhancedCatalog
    from flovopy.enhanced.serialization import json_compatible
    index=pd.read_csv(ctx['out']/'event_metrics_index.csv')
    paths=[str(resolve_artifact(value)) for value in index.loc[index.status.ne('failed'),'sidecar_path'].dropna()]
    temporary=ctx['out']/'window_metrics.csv.tmp'
    with open(temporary,'w',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=METRIC_FIELDS);writer.writeheader()
        for path in paths:
            payload=json.loads(Path(path).read_text())
            for trace in payload.get('traces',[]):
                summary=trace.get('summary',{})
                writer.writerow({key:json_compatible(summary.get(key)) for key in METRIC_FIELDS})
    temporary.replace(ctx['out']/'window_metrics.csv')
    print('Rebuilding complete trace/window CSV from sidecars...',flush=True)
    EnhancedCatalog.export_sidecars_csv(paths,ctx['out']/'all_window_metrics.csv',level='trace')
    EnhancedCatalog.export_sidecars_csv(paths,ctx['out']/'events_metadata.csv',level='event',include_trace_summaries=False)
    catalog=_lightweight_catalog(paths);catalog.export_csv(str(ctx['out']/'catalog_summary.csv'))
    if write_quakeml and len(catalog):catalog.write(str(ctx['out']/'events.qml'),format='QUAKEML')
    fields=pd.read_csv(ctx['out']/'all_window_metrics.csv',nrows=0).columns
    pd.DataFrame(dict(column=fields,storage='scalar or JSON-valued cell; see sidecar')).to_csv(ctx['out']/'all_metrics_schema.csv',index=False)
    # Only the scalar plotting table is materialized as an ensemble DataFrame.
    return pd.read_csv(ctx['out']/'window_metrics.csv',dtype={'record_id':str,'trace_key':str,
        'feature_version':str,'artifact_pre_detected':'boolean','artifact_post_detected':'boolean','event_end_record_limited':'boolean'})
