"""Small-sample archive QC. Candidate arrivals are not acquisition trigger times.

All timing below uses recorded trace times, without UTC corrections. Raw samples
are preserved. Noise-reference windows are assumptions for screening only.
"""
from pathlib import Path
from dataclasses import dataclass, asdict
import hashlib
import json
import math
import re
import numpy as np
import pandas as pd
from obspy import read, Stream
from flovopy.processing.detection import PretriggerSettings as Settings, detect_pretrigger_window
# Compatibility for existing diagnostic tests; notebook analysis uses the public API.
from flovopy.processing.event_windows import (
    ALGORITHM_VERSION, _analyse_trace as analyse_trace, frequency_checks, hilbert_diagnostic, _summarize_hilbert,
)

def select_files(wav_root, months=('1991/05','1991/06','1991/07','1991/08'), per_month=2):
    """Choose evenly spread recording days, then a middle file on each day.

    This is a reproducible exploratory sample, not a statistical archive sample.
    No waveforms are read during selection. Supply explicit files for focused work.
    """
    if isinstance(per_month,bool) or int(per_month)!=per_month or per_month<1:raise ValueError('per_month must be a positive integer')
    selected=[];report=[]
    for month in months:
        if not re.fullmatch(r'\d{4}/\d{2}',month):raise ValueError('Months must have form YYYY/MM')
        folder=Path(wav_root)/month
        files=sorted(p for p in folder.iterdir() if p.is_file() and not p.name.startswith('.')) if folder.is_dir() else []
        days={}
        for p in files:
            m=re.match(r'\d{4}-\d{2}-\d{2}',p.name)
            if m:days.setdefault(m[0],[]).append(p)
        keys=sorted(days);count=min(per_month,len(keys))
        indices=np.unique(np.round(np.linspace(0,len(keys)-1,count)).astype(int)) if count>1 else ([len(keys)//2] if count else [])
        picks=[days[keys[i]][len(days[keys[i]])//2] for i in indices]
        selected.extend(picks);report.append(dict(month=month,files_available=len(files),recognized_days=len(keys),files_selected=len(picks),status='selected' if picks else 'missing_or_no_recognized_files'))
    return selected,pd.DataFrame(report)


def analyse_files(files,output_root,settings=None):
    cfg=settings or Settings();files=[Path(p).expanduser().resolve() for p in files]
    if not files:raise ValueError('No files selected')
    if len(set(files))!=len(files):raise ValueError('Duplicate file selection')
    manifest=[dict(path=str(p),size=p.stat().st_size,mtime_ns=p.stat().st_mtime_ns) for p in files]
    payload=dict(settings=asdict(cfg),files=manifest,algorithm_version=ALGORITHM_VERSION)
    run_id=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()[:12]
    out=Path(output_root)/run_id;out.mkdir(parents=True,exist_ok=True)
    (out/'run_manifest.json').write_text(json.dumps(payload,indent=2)+'\n')
    qc=[];metrics=[];frequencies=[];file_rows=[];details={};networks=[];candidates=[]
    for path in files:
        record_id=hashlib.sha256(str(path).encode()).hexdigest()[:12]
        try:stream=read(str(path))
        except Exception as exc:
            file_rows.append(dict(file=str(path),record_id=record_id,status='read_error',error=str(exc)));continue
        usable=[tr for tr in stream if not (tr.stats.station or '').upper().startswith('IRIG') and tr.stats.npts>0]
        detection=detect_pretrigger_window(Stream(usable),settings=cfg)
        records=detection['details']
        def file_columns(mapping):
            return {key.replace('_record_s','_file_s'):value for key,value in mapping.items()}
        context=file_columns(detection['network'])
        networks.append(dict(file=str(path),record_id=record_id,**context))
        origin=detection['reference_time']
        for record in records:
            index=record['qc']['trace_index'];base=dict(file=str(path),record_id=record_id,trace_index=index)
            offset=record['record_offset_s'];record['file_offset_s']=offset
            row={**base,**file_columns(record['qc']), 'trace_start_offset_file_s':offset,
                 'file_reference_recorded_time':str(origin)}
            sample_dir=out/'hilbert_samples';sample_dir.mkdir(exist_ok=True)
            sample_path=sample_dir/f'{record_id}_{index:02d}.npz'
            np.savez_compressed(sample_path,**{c:record['hilbert'][c].to_numpy() for c in record['hilbert'].columns},
                file_offset_s=np.array(offset),sampling_rate_hz=np.array(row['sampling_rate_hz']))
            row['hilbert_samples_path']=str(sample_path);record['qc']=row;qc.append(row)
            for table_key,destination in [('frequency',frequencies),('one_second',metrics)]:
                table=record[table_key].copy()
                for key,value in base.items():table[key]=value
                table['trace_id']=row['trace_id']
                for key in ['window_start_s','window_end_s']:
                    if key in table:table[key[:-2]+'_file_s']=table[key]+offset
                destination.append(table)
            for candidate in record['candidates']:
                candidates.append(dict(record_id=record_id,trace_index=index,trace_id=row['trace_id'],
                    time_file_s=candidate['time_trace_s']+offset,**candidate))
        details[record_id]=records
        file_rows.append(dict(file=str(path),record_id=record_id,status='analysed' if records else 'no_analysable_traces',
            input_traces=len(stream),analysed_traces=len(records),excluded_empty_or_irig=len(stream)-len(usable)))
    pd.DataFrame(networks).to_csv(out/'network_event_context.csv',index=False)
    pd.DataFrame(candidates).to_csv(out/'onset_evidence.csv',index=False)
    qc_frame=pd.DataFrame(qc);metric_frame=pd.concat(metrics,ignore_index=True) if metrics else pd.DataFrame()
    qc_frame.to_csv(out/'trace_qc_and_candidates.csv',index=False);metric_frame.to_csv(out/'one_second_metrics.csv',index=False)
    pd.DataFrame(file_rows).to_csv(out/'file_inventory.csv',index=False)
    frequency_frame=pd.concat(frequencies,ignore_index=True) if frequencies else pd.DataFrame()
    frequency_frame.to_csv(out/'frequency_checks.csv',index=False)
    review=qc_frame[[c for c in ['file','record_id','trace_index','trace_id','amplitude_onset_file_s','sta_lta_onset_file_s','selected_onset_file_s','selected_onset_method','onset_support_start_file_s','onset_support_end_file_s','frequency_change_file_s','proposed_noise_start_file_s','proposed_noise_end_file_s','noise_proposal_status','qc_flags'] if c in qc_frame]].copy()
    for c in ['review_status','accepted_noise_start_file_s','accepted_noise_end_file_s','accepted_signal_start_file_s','accepted_signal_end_file_s','reviewer_notes']:review[c]=''
    review['review_status']='UNREVIEWED'
    if not (out/'manual_window_review.csv').exists():review.to_csv(out/'manual_window_review.csv',index=False)
    return dict(output=out,qc=qc_frame,metrics=metric_frame,files=pd.DataFrame(file_rows),details=details,frequency=frequency_frame,network=pd.DataFrame(networks),evidence=pd.DataFrame(candidates),settings=cfg)


def plot_record(records,settings,outfile,max_channels=12):
    import matplotlib.pyplot as plt
    cfg=settings;records=records[:max_channels]
    if not records:return None
    fig,axes=plt.subplots(4,1,figsize=(12,11),sharex=True,gridspec_kw={'height_ratios':[2.2,1,1,1]})
    colours=plt.get_cmap('tab10');labels=[]
    for i,r in enumerate(records):
        colour=colours(i%10);offset=r['file_offset_s'];t=r['t']+offset;x=r['centred'];limit=(t<=cfg.plot_seconds)&np.isfinite(x)
        scale=float(np.percentile(np.abs(x[limit]),95)) if limit.any() else 1.;scale=max(scale,1e-12)
        axes[0].fill_between([offset+cfg.reference_start_s,offset+cfg.reference_end_s],i*4-1.8,i*4+1.8,color='grey',alpha=.12,zorder=0)
        axes[0].plot(t,np.clip(x/scale,-2,2)+i*4,lw=.45,color=colour);labels.append(r['qc']['trace_id'])
        noise_end=r['qc']['proposed_noise_end_file_s']
        if np.isfinite(noise_end):axes[0].plot([noise_end,noise_end],[i*4-1.8,i*4+1.8],color=colour,ls='--',lw=.8)
        onset=r['qc'].get('selected_onset_file_s',np.nan)
        if np.isfinite(onset):axes[0].plot(onset,i*4,'o',ms=4,color=colour)
        table=r['one_second'];level=r['qc']['reference_rsam_counts'];ref=table[(table.window_start_s>=cfg.reference_start_s)&(table.window_end_s<=cfg.reference_end_s)]
        rms_level=ref.rms.median()
        for ax,key,den in [(axes[1],'rsam',level),(axes[2],'rms',rms_level)]:
            if np.isfinite(den) and den>0:ax.plot(table.centre_s+offset,table[key]/den,'.-',lw=.8,ms=3,color=colour,label=labels[-1])
        axes[3].plot(t,r['sta_lta'],lw=.65,color=colour)
    axes[0].set_yticks(np.arange(len(records))*4,labels);axes[0].set_ylabel('Waveforms\nscaled per trace')
    axes[0].set_title(Path(records[0]['qc']['file']).name+' — candidates are arrivals, not trigger times',fontsize=11)
    for ax in axes:
        if np.isfinite(records[0]['qc'].get('network_anchor_file_s',np.nan)):
            ax.axvspan(records[0]['qc']['network_search_start_file_s'],records[0]['qc']['network_search_end_file_s'],color='#4477AA',alpha=.035)
        for value in (5,10):ax.axvline(value,color='black',ls='--',lw=.8,alpha=.7)
        ax.set_xlim(0,cfg.plot_seconds);ax.grid(alpha=.15)
    # Reference windows differ if original trace starts differ; show per-channel offsets above in tables.
    axes[1].set(ylabel='1-s mean |x|\n/reference',yscale='log');axes[2].set(ylabel='1-s RMS\n/reference',yscale='log')
    axes[1].axhline(cfg.amplitude_factor,color='grey',ls=':',lw=.7)
    axes[3].axhline(cfg.sta_lta_threshold,color='grey',ls=':',label='STA/LTA threshold')
    axes[3].set(ylabel='Causal STA/LTA',xlabel='Seconds after earliest stored non-IRIG trace start',ylim=(0,max(6,cfg.sta_lta_threshold*1.5)))
    fig.text(.02,.015,'Dashed: 5 and 10 s hypotheses. Dots: selected onset candidates. Grey: five-second reference. Short coloured dashes: proposed noise ends. Display clips scaled waveforms at ±2.\nRaw data; only assumed-reference median removed. Flat padding/clipping and reference contamination require review. No UTC correction applied.',fontsize=8)
    fig.tight_layout(rect=(0,.065,1,1));path=Path(outfile);path.parent.mkdir(parents=True,exist_ok=True)
    for ext in ['png','pdf','svg']:fig.savefig(path.with_suffix('.'+ext),dpi=180)
    return fig


def plot_frequency_record(records,settings,outfile,max_channels=8):
    import matplotlib.pyplot as plt
    records=records[:max_channels]
    if not records:return None
    fig,axes=plt.subplots(4,1,figsize=(12,11),sharex=True)
    colours=plt.get_cmap('tab10')
    for i,r in enumerate(records):
        frame=r['frequency'];offset=r['file_offset_s'];colour=colours(i%10)
        if not len(frame):continue
        times=frame.window_end_s+offset
        axes[0].plot(times,frame.centroid_hz,color=colour,lw=.9,label=r['qc']['trace_id'])
        axes[1].plot(times,frame.js_divergence,color=colour,lw=.9)
        axes[2].plot(times,frame.zero_crossing_hz,color=colour,lw=.9)
        good=frame.hilbert_median_hz.where(frame.hilbert_valid_fraction>=.5)
        axes[3].plot(times,good,color=colour,lw=.9)
        change=r['qc']['frequency_change_file_s']
        if np.isfinite(change):
            for ax in axes:ax.axvline(change,color=colour,ls=':',lw=.7,alpha=.7)
    axes[0].set_ylabel('PSD centroid (Hz)\n0.8–20 Hz default')
    axes[1].set_ylabel('Spectral shape change\nJensen–Shannon divergence')
    axes[2].set_ylabel('Raw zero-crossing\nfrequency proxy (Hz)')
    axes[3].set_ylabel('Hilbert IF (Hz)\nsigned 1-s median')
    axes[0].set_title('Frequency cross-check: trailing windows; no trigger-time inference')
    axes[0].legend(fontsize=8,ncol=4,frameon=False)
    for ax in axes:
        ax.axvspan(0,settings.minimum_noise_s,color='grey',alpha=.1)
        for value in (5,10):ax.axvline(value,color='black',ls='--',lw=.7)
        ax.set_xlim(0,settings.plot_seconds);ax.grid(alpha=.15)
    axes[3].set_xlabel('Seconds since file reference; summary times mark window ends')
    fig.text(.02,.012,'Dotted: sustained centroid + spectral-shape candidates. Grey: assumed minimum noise interval (offsets may vary by trace).\nZero crossings use raw detrended samples and can be dominated by acquisition noise. Hilbert is raw broadband phase-derived IF; weak-envelope/edge values are excluded from summaries.',fontsize=8)
    fig.tight_layout(rect=(0,.065,1,1));path=Path(outfile)
    path.parent.mkdir(parents=True,exist_ok=True)
    for ext in ['png','pdf','svg']:fig.savefig(path.with_suffix('.'+ext),dpi=180)
    return fig


def plot_hilbert_trace(record,settings,outfile):
    """Show signed raw IF, screened median-smoothed IF and analytic envelope."""
    import matplotlib.pyplot as plt
    samples=record['hilbert'];offset=record['file_offset_s']
    if not len(samples):return None
    t=samples.time_trace_s+offset
    fig,axes=plt.subplots(3,1,figsize=(12,8),sharex=True)
    axes[0].plot(t,samples.envelope_counts,lw=.6,color='#4477AA',label='analytic envelope')
    axes[0].plot(t,samples.envelope_floor_counts,ls=':',color='black',lw=.8,label='low-envelope screen')
    axes[0].set_ylabel('Envelope (counts)');axes[0].legend(frameon=False,fontsize=8)
    axes[1].plot(t,samples.instantaneous_hz_raw,lw=.4,color='grey')
    axes[1].axhline(0,color='black',lw=.5);axes[1].set_ylabel('Signed raw IF (Hz)')
    axes[2].plot(t,samples.instantaneous_hz_smoothed,lw=.7,color='#228833')
    axes[2].set(ylabel='Signed, median-\nsmoothed IF (Hz)',xlabel='Seconds after file reference; phase differences located at sample midpoints')
    for ax in axes:
        for v in [5,10]:ax.axvline(v,ls='--',color='black',lw=.7)
        ax.grid(alpha=.15);ax.set_xlim(0,settings.plot_seconds)
    axes[0].set_title(record['qc']['trace_id']+' — Hilbert phase derivative (no bandpass)')
    fig.text(.02,.012,'Raw = unwrap(angle(hilbert(linearly detrended contiguous data))) first difference × fs / (2π).\nSigned raw values are retained. Screening removes low-envelope and edge samples; signed estimates are retained. Offline/nonlocal estimate; no onset is inferred.',fontsize=8)
    fig.tight_layout(rect=(0,.065,1,1));path=Path(outfile);path.parent.mkdir(parents=True,exist_ok=True)
    for ext in ['png','pdf','svg']:fig.savefig(path.with_suffix('.'+ext),dpi=180)
    return fig
