"""Experimental HF-before-LF screens, not validated earthquake classifications.

Reuse FLOVOpy's temporal features and signed Hilbert diagnostic. Candidate scores
remain in the testing tree; the production event catalog is never relabelled.
"""
from pathlib import Path
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import butter,sosfiltfilt,welch,hilbert,detrend
from scipy.ndimage import uniform_filter1d
from obspy import Trace
from flovopy.core.features import FeatureConfig,extract_features
from flovopy.processing.event_windows import PretriggerSettings,hilbert_diagnostic
from flovopy.enhanced.serialization import atomic_json
from analysis_workflow import (finite_data,runs,digest,save_figure,station_color,
    load_context,build_inventory,select_analysis_windows,waveform_traces,cut_half_open)
from pinatubo_project import test_directory


def validate_settings(cfg):
    for key in ['lf_band_hz','hf_band_hz','if_band_hz']:
        if len(cfg[key])!=2 or not 0<cfg[key][0]<cfg[key][1]:raise ValueError('Invalid '+key)
    if cfg['lf_band_hz'][1]>cfg['hf_band_hz'][0]:raise ValueError('LF/HF bands overlap')
    if not 0<cfg['peak_delay_range_s'][0]<=cfg['peak_delay_range_s'][1]:raise ValueError('Invalid delay range')
    for key in ['if_minimum_coverage','if_maximum_negative_fraction']:
        if not 0<cfg[key]<=1:raise ValueError('Invalid '+key)
    for key in ['minimum_peak_band_snr','if_window_s','if_step_s','if_sustain_s','if_minimum_drop_hz','if_envelope_noise_ratio']:
        if not np.isfinite(cfg[key]) or cfg[key]<=0:raise ValueError('Invalid '+key)
    if cfg['minimum_stations']<1:raise ValueError('minimum_stations must be positive')


def _filtered(x,fs,band):
    if band[1]>=fs/2:raise ValueError('Band reaches/exceeds Nyquist')
    return sosfiltfilt(butter(4,band,btype='bandpass',fs=fs,output='sos'),detrend(x,type='linear'))


def _peak_band_snr(event,noise,fs,peak,band):
    """Matched density estimates around the peak versus preceding noise.

    This avoids treating a five-second LF noise-envelope estimate as edge-free.
    It is a finite-window screening ratio, not calibrated source amplitude.
    """
    n=min(round(2.56*fs),len(event),len(noise))
    if n<16 or not np.isfinite(peak):return np.nan
    a=max(0,min(len(event)-n,int(round(peak*fs))-n//2));segment=event[a:a+n]
    f,p=welch(segment,fs=fs,window='hann',nperseg=n,noverlap=n//2,detrend='constant',scaling='density')
    _,pn=welch(noise,fs=fs,window='hann',nperseg=n,noverlap=n//2,detrend='constant',scaling='density')
    band_mask=(f>=band[0])&(f<band[1]);df=fs/n
    reference=float(pn[band_mask].sum()*df);signal=float(p[band_mask].sum()*df)
    return float(np.sqrt(signal/reference)) if reference>0 else np.nan


def _if_frames(event,noise,fs,cfg):
    y=_filtered(event,fs,cfg['if_band_hz']);n=_filtered(noise,fs,cfg['if_band_hz'])
    h=hilbert_diagnostic(y,fs,PretriggerSettings(hilbert_envelope_floor_fraction=.05))
    # Noise baseline affects only an explicit amplitude gate, never IF centring.
    noise_envelope=np.abs(hilbert(n));edge=min(round(fs),len(n)//4)
    reference=float(np.median(noise_envelope[edge:len(n)-edge]))
    width=max(1,round(cfg['if_window_s']*fs));step=max(1,round(cfg['if_step_s']*fs))
    rows=[]
    for a in range(0,len(event)-width+1,step):
        block=h[(h.time_trace_s>=a/fs)&(h.time_trace_s<(a+width)/fs)]
        values=block.instantaneous_hz_valid.dropna();coverage=len(values)/width
        negative=float((block.instantaneous_hz_raw<0).mean()) if len(block) else np.nan
        median=float(values.median()) if len(values) else np.nan
        envelope=float(block.envelope_counts.median()) if len(block) else np.nan
        ratio=envelope/reference if reference>0 else np.nan
        reliable=coverage>=cfg['if_minimum_coverage'] and negative<=cfg['if_maximum_negative_fraction']
        rows.append(dict(time_s=(a+width/2)/fs,median_hz=median,valid_fraction=coverage,
            negative_fraction=negative,envelope_ratio=ratio,reliable=bool(reliable)))
    frame=pd.DataFrame(rows)
    return frame,h


def _if_change(frame,cfg):
    valid=frame.reliable&frame.envelope_ratio.ge(cfg['if_envelope_noise_ratio'])
    high=valid&frame.median_hz.ge(cfg['hf_band_hz'][0])
    low=valid&frame.median_hz.gt(0)&frame.median_hz.le(cfg['lf_band_hz'][1])
    step=float(np.median(np.diff(frame.time_s))) if len(frame)>1 else cfg['if_step_s']
    required=max(1,int(np.ceil(cfg['if_sustain_s']/step-1e-9)))
    highs=[(a,b) for a,b in runs(high.to_numpy()) if b-a>=required]
    lows=[(a,b) for a,b in runs(low.to_numpy()) if b-a>=required]
    result=dict(if_candidate=None,if_high_time_s=np.nan,if_low_time_s=np.nan,if_transition_delay_s=np.nan,
                if_drop_hz=np.nan,if_testable_frames=int(valid.sum()))
    if not highs:return result
    result['if_candidate']=False
    for a,b in highs:
        early=frame.iloc[a:a+required];early_time=float(early.time_s.median());early_hz=float(early.median_hz.median())
        for c,d in lows:
            late=frame.iloc[c:c+required];late_time=float(late.time_s.median());delay=late_time-early_time
            drop=early_hz-float(late.median_hz.median())
            if cfg['peak_delay_range_s'][0]<=delay<=cfg['peak_delay_range_s'][1] and drop>=cfg['if_minimum_drop_hz']:
                return {**result,'if_candidate':True,'if_high_time_s':early_time,'if_low_time_s':late_time,
                        'if_transition_delay_s':delay,'if_drop_hz':drop}
    return result


def analyse_hybrid_trace(event_trace,noise_trace,cfg):
    validate_settings(cfg)
    fs=float(event_trace.stats.sampling_rate);event=finite_data(event_trace);noise=finite_data(noise_trace)
    if fs!=float(noise_trace.stats.sampling_rate):raise ValueError('Noise/event sampling rates differ')
    if cfg['event_search_seconds'] is not None:event=event[:int(round(cfg['event_search_seconds']*fs))]
    result=dict(trace_id=event_trace.id,peak_delay_candidate=None,if_candidate=None,screen_class='not_testable',
        lf_peak_time_s=np.nan,hf_peak_time_s=np.nan,lf_minus_hf_peak_delay_s=np.nan,
        lf_peak_band_snr=np.nan,hf_peak_band_snr=np.nan,diagnostic_duration_s=len(event)/fs,flags='')
    if len(event)<16 or len(noise)<16 or not np.isfinite(event).all() or not np.isfinite(noise).all():
        result['flags']='short_or_missing_samples';return result,{}
    feature_cfg=FeatureConfig(lf_band=tuple(cfg['lf_band_hz']),hf_band=tuple(cfg['hf_band_hz']))
    event=detrend(event,type='linear');noise=detrend(noise,type='linear')
    features=extract_features(event,fs,noise=noise,config=feature_cfg,units='counts');v=features['values']
    result.update(lf_peak_time_s=v['lf.peak_time_s'],hf_peak_time_s=v['hf.peak_time_s'],
                  lf_minus_hf_peak_delay_s=v['hf_lf.peak_delay_s'],flags=';'.join(features['qc_flags']))
    lf_snr=_peak_band_snr(event,noise,fs,v['lf.peak_time_s'],cfg['lf_band_hz'])
    hf_snr=_peak_band_snr(event,noise,fs,v['hf.peak_time_s'],cfg['hf_band_hz'])
    result.update(lf_peak_band_snr=lf_snr,hf_peak_band_snr=hf_snr)
    veto=any(flag in features['qc_flags'] for flag in ['lf:too_short_for_band','hf:too_short_for_band',
        'lf:invalid_nyquist','hf:invalid_nyquist','lf:peak_near_edge','hf:peak_near_edge'])
    delay=v['hf_lf.peak_delay_s']
    if np.isfinite([delay,lf_snr,hf_snr]).all() and not veto:
        result['peak_delay_candidate']=bool(cfg['peak_delay_range_s'][0]<=delay<=cfg['peak_delay_range_s'][1]
            and min(lf_snr,hf_snr)>=cfg['minimum_peak_band_snr'])
    details=dict(event=event,noise=noise,sampling_rate_hz=fs,features=features)
    try:
        frames,h=_if_frames(event,noise,fs,cfg);result.update(_if_change(frames,cfg));details.update(if_frames=frames,hilbert=h)
        for label,band in [('lf',cfg['lf_band_hz']),('hf',cfg['hf_band_hz'])]:
            y=_filtered(event,fs,band);width=max(1,round(feature_cfg.smoothing_seconds*fs));width+=width%2==0
            details[label+'_envelope']=uniform_filter1d(np.abs(hilbert(y)),width,mode='reflect')
    except ValueError as exc:result['flags']+=';if_not_testable:'+str(exc)
    peak=result['peak_delay_candidate'];frequency=result['if_candidate']
    result['screen_class']='both' if peak is True and frequency is True else ('peak_only' if peak is True else
        ('if_only' if frequency is True else ('not_testable' if peak is None and frequency is None else 'neither')))
    return result,details


def plot_hybrid(result,details,dest,cfg):
    if not details:return None
    fs=details['sampling_rate_hz'];t=np.arange(len(details['event']))/fs
    fig,axes=plt.subplots(4,1,figsize=(8,7),sharex=True,layout='constrained')
    axes[0].plot(t,details['event'],lw=.6,color='0.25');axes[0].set_ylabel('Counts')
    axes[0].set_title(f"{result['trace_id']} — experimental screen: {result['screen_class']}")
    for label,color in [('hf','#cc6677'),('lf','#4477aa')]:
        if label+'_envelope' in details:
            env=details[label+'_envelope'];axes[1].plot(t,env,label=label.upper(),color=color,lw=.8)
            peak=result[label+'_peak_time_s']
            if np.isfinite(peak):axes[1].axvline(peak,color=color,ls='--',lw=.8)
    axes[1].legend();axes[1].set_ylabel('Band envelope\n(counts)')
    if 'hilbert' in details:
        h=details['hilbert'];axes[2].plot(h.time_trace_s,h.instantaneous_hz_smoothed,color='0.6',lw=.35,alpha=.6)
        f=details['if_frames'];axes[2].plot(f.time_s,f.median_hz,lw=.9,color='#228833',label='Signed IF median')
        axes[2].axhline(cfg['lf_band_hz'][1],ls=':',color='0.3');axes[2].set_ylabel('Band-limited\nIF (Hz)');axes[2].legend()
        axes[3].plot(f.time_s,f.valid_fraction,label='Valid fraction',lw=.8)
        axes[3].plot(f.time_s,np.minimum(f.envelope_ratio,5)/5,label='Envelope/noise ÷ 5 (capped)',lw=.8)
        axes[3].legend(fontsize=7);axes[3].set(ylabel='Reliability/gate',ylim=(0,1.05))
    axes[-1].set_xlabel('Seconds from selected event-window start')
    caption='Experimental screening only. LF minus HF smoothed-envelope peak lag is positive for HF first. A 1–10 s lag can also reflect ordinary phase/coda structure, filtering or overlapping events; it is not a unique hybrid-earthquake diagnosis. Signed Hilbert IF is computed after the configured common bandpass, with coverage/negative-frequency/amplitude gates. IF state-transition timing is distinct from peak lag. Cropped record-end windows do not establish physical coda duration.'
    save_figure(fig,dest,caption)
    return fig


def synthetic_controls(cfg,out):
    out=Path(out);out.mkdir(parents=True,exist_ok=True);fs=100.;t=np.arange(4000)/fs
    noise=np.random.default_rng(55).normal(0,.1,1000);rows=[]
    for name,hf,lf in [('hf_then_lf',4.,8.),('lf_then_hf',8.,4.),('synchronous',5.,5.),('hf_only',5.,None),('lf_only',None,5.),('noise_only',None,None)]:
        x=np.random.default_rng(42).normal(0,.1,len(t))
        if hf is not None:x+=3*np.exp(-.5*((t-hf)/.65)**2)*np.sin(2*np.pi*10*t)
        if lf is not None:x+=3*np.exp(-.5*((t-lf)/.8)**2)*np.sin(2*np.pi*2*t)
        tr=Trace(x,header={'sampling_rate':fs,'station':name[:5],'channel':'EHZ'})
        nt=Trace(noise.copy(),header={'sampling_rate':fs})
        result,details=analyse_hybrid_trace(tr,nt,cfg);rows.append(dict(control=name,**result))
        fig=plot_hybrid(result,details,out/name,cfg)
        if fig is not None:plt.close(fig)
    frame=pd.DataFrame(rows);frame.to_csv(out/'synthetic_controls.csv',index=False)
    return frame


def run_hybrid_lab(analysis_dir, *, overrides=None):
    analysis_dir=Path(analysis_dir);cfg=json.loads((analysis_dir/'hybrid_test_config.json').read_text())
    if overrides:cfg.update(overrides)
    validate_settings(cfg)
    run=digest(dict(config=cfg,source_sha256=digest(Path(__file__).read_bytes())))[:12]
    out=test_directory('hybrid',run)
    ctx=load_context(analysis_dir,selection_override=dict(max_files=cfg['max_files']),products_directory=out/'workflow')
    windows,issues=select_analysis_windows(ctx);inventory,_=build_inventory(ctx)
    lookup=inventory[inventory.is_waveform].set_index('trace_key');rows=[];errors=[];figures=0
    from obspy import read
    selected=windows.assign(file=windows.trace_key.map(lookup.file))
    for file,group in selected.groupby('file',sort=False):
        traces=waveform_traces(read(file));origin=min(tr.stats.starttime for tr in traces)
        for key,pair in group.groupby('trace_key'):
            if set(pair.window)!={'noise','event'}:continue
            tr=traces[int(lookup.loc[key].trace_index)]
            try:
                cuts={row.window:cut_half_open(tr,row.window_start_file_s,row.window_end_file_s,origin) for row in pair.itertuples()}
                result,details=analyse_hybrid_trace(cuts['event'],cuts['noise'],cfg)
                result.update(trace_key=key,record_id=lookup.loc[key].record_id,station=tr.stats.station,
                    recorded_starttime=str(tr.stats.starttime),window_policy=pair.window_policy.iloc[0])
                result.update(event_window_start_file_s=float(cuts['event'].stats.starttime-origin),
                    noise_window_start_file_s=float(cuts['noise'].stats.starttime-origin),
                    noise_window_end_file_s=float(cuts['noise'].stats.starttime-origin)+len(cuts['noise'])/cuts['noise'].stats.sampling_rate)
                rows.append(result)
                atomic_json(out/'traces'/(key.replace(':','_')+'.json'),dict(result=result,features=details.get('features'),config=cfg))
                if figures<cfg['max_figures'] and details:
                    fig=plot_hybrid(result,details,out/'figures'/key.replace(':','_'),cfg)
                    if fig is not None:plt.close(fig)
                    figures+=1
                if details.get('if_frames') is not None:details['if_frames'].to_csv(out/(key.replace(':','_')+'_if_frames.csv'),index=False)
            except Exception as exc:errors.append(dict(trace_key=key,error=f'{type(exc).__name__}: {exc}'))
    scores=pd.DataFrame(rows);scores.to_csv(out/'trace_hybrid_screens.csv',index=False)
    pd.DataFrame(errors,columns=['trace_key','error']).to_csv(out/'errors.csv',index=False)
    events=[]
    if len(scores):
        for rid,group in scores.groupby('record_id'):
            # Each station can vote once regardless of components.
            station=group.groupby('station').agg(peak=('peak_delay_candidate',lambda v:any(x is True or x==True for x in v if pd.notna(x))),
                frequency=('if_candidate',lambda v:any(x is True or x==True for x in v if pd.notna(x))))
            joint=int((station.peak&station.frequency).sum())
            events.append(dict(record_id=rid,traces=len(group),peak_stations=int(station.peak.sum()),
                if_stations=int(station.frequency.sum()),joint_stations=joint,
                multi_station_candidate=joint>=cfg['minimum_stations'],classification_status='EXPERIMENTAL_SCREEN_NOT_HYBRID_LABEL'))
    pd.DataFrame(events).to_csv(out/'event_hybrid_screens.csv',index=False)
    atomic_json(out/'run_manifest.json',dict(config=cfg,selected_files=[str(p) for p in ctx['files']],
        source_sha256=digest(Path(__file__).read_bytes()),window_policy=ctx['config'].get('windows'),
        status='EXPERIMENTAL; no production classification changed',real_traces=len(scores),errors=len(errors)))
    # Labels are optional human inputs, never generated from the screens.
    labels=out/'labels_template.csv'
    if not labels.exists() and len(scores):scores[['trace_key','record_id']].assign(is_hybrid='',reviewer_notes='').to_csv(labels,index=False)
    if cfg.get('labels_csv'):
        evaluate_labels(scores,Path(cfg['labels_csv']).expanduser()).to_csv(out/'label_evaluation.csv',index=False)
    return out,scores,pd.DataFrame(events),pd.DataFrame(errors)


def evaluate_labels(scores, labels_csv):
    """Evaluate only explicit binary human labels, with unresolved coverage shown."""
    labels=pd.read_csv(labels_csv)
    if labels.trace_key.duplicated().any():raise ValueError('Duplicate labelled trace_key values')
    def label(value):
        if pd.isna(value) or str(value).strip()=='':return np.nan
        text=str(value).strip().lower()
        if text in ['1','1.0','true','yes']:return True
        if text in ['0','0.0','false','no']:return False
        raise ValueError('Labels must be explicit binary values, not inferred classes')
    labels['truth']=labels.is_hybrid.map(label)
    joined=scores.merge(labels[['trace_key','truth']],on='trace_key',validate='one_to_one').dropna(subset=['truth'])
    rows=[]
    for method,column in [('peak_delay','peak_delay_candidate'),('instantaneous_frequency','if_candidate')]:
        resolved=joined.dropna(subset=[column]);truth=resolved.truth.astype(bool);positive=resolved[column].astype(bool)
        tp=int((truth&positive).sum());fp=int((~truth&positive).sum());tn=int((~truth&~positive).sum());fn=int((truth&~positive).sum())
        rows.append(dict(method=method,labelled=len(joined),resolved=len(resolved),unresolved=len(joined)-len(resolved),
            true_positive=tp,false_positive=fp,true_negative=tn,false_negative=fn,
            precision=tp/(tp+fp) if tp+fp else np.nan,recall=tp/(tp+fn) if tp+fn else np.nan))
    return pd.DataFrame(rows)
