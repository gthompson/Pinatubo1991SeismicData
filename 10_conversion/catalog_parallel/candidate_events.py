"""Conservative candidate grouping of existing picks, with waveform support diagnostics.

No new picks, relocations, or calibrated detection probabilities are produced.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass
from pathlib import Path
import hashlib
import json
import math

import numpy as np
import pandas as pd
from obspy import read, UTCDateTime
from scipy.ndimage import uniform_filter1d

from catalog_workflow import INPUT_KEYS, digest, prepare_output


@dataclass(frozen=True)
class Settings:
    gap_s: float = 2.0
    max_span_s: float = 6.0
    sensitivity_gaps_s: tuple = (1.5, 2.0, 3.0)
    repeat_station_tolerance_s: float = 0.5
    min_stations: int = 2
    max_sp_s: float = 15.0
    min_origin_to_p_s: float = -0.5
    max_origin_to_p_s: float = 10.0
    envelope_s: float = 0.15
    baseline_start_s: float = 3.0
    baseline_end_s: float = 0.5
    signal_before_s: float = 0.2
    signal_after_s: float = 0.8
    min_energy_ratio: float = 3.0
    clip_plateau_s: float = 0.05
    max_waveforms: int | None = 150  # None checks all picked windows; sampling is deterministic.
    max_plots: int = 30
    sample_seed: int = 1991

    def validate(self):
        positive = ('gap_s','max_span_s','repeat_station_tolerance_s','max_sp_s','envelope_s',
                    'baseline_start_s','baseline_end_s','signal_before_s','signal_after_s',
                    'min_energy_ratio','clip_plateau_s')
        if any(not math.isfinite(getattr(self,k)) or getattr(self,k) <= 0 for k in positive):
            raise ValueError('Timing and energy parameters must be finite and positive')
        if self.baseline_start_s <= self.baseline_end_s or self.min_stations < 2:
            raise ValueError('Invalid baseline or minimum station count')
        if not self.sensitivity_gaps_s or any(not math.isfinite(x) or x<=0 for x in self.sensitivity_gaps_s):
            raise ValueError('Sensitivity gaps must be positive')
        if not all(math.isfinite(x) for x in (self.min_origin_to_p_s,self.max_origin_to_p_s)) or self.min_origin_to_p_s > self.max_origin_to_p_s:
            raise ValueError('Invalid origin-to-P interval')
        if self.max_waveforms is not None and self.max_waveforms < 0 or self.max_plots < 0:
            raise ValueError('Negative processing limit')


def cid(window, picks):
    return 'candidate_' + hashlib.sha256((str(window)+'|'+'|'.join(sorted(map(str,picks)))).encode()).hexdigest()[:24]


def station(seed):
    parts=str(seed).split('.')
    return '.'.join(parts[:2]) if len(parts)>=2 else str(seed)


def partition(picks, gap_s, span_s):
    """Bounded-gap clusters: the span cap prevents chains joining many arrivals."""
    groups=[]
    for row in picks.sort_values(['_t','pick_id']).to_dict('records'):
        if not groups or row['_t']-groups[-1][-1]['_t'] > gap_s or row['_t']-groups[-1][0]['_t'] > span_s:
            groups.append([])
        groups[-1].append(row)
    return groups


def group_window(window, picks, settings, *, start=None, end=None):
    """Return candidates, pick assignments and sensitivity diagnostics for one window."""
    settings.validate()
    p=picks.copy()
    p['_t']=pd.to_datetime(p.pick_time,format='mixed',utc=True,errors='raise').astype('datetime64[ns, UTC]').astype('int64') / 1e9
    p['_station']=p.seed_id.map(station)
    phase=p.phase.fillna('').str.upper()
    within=pd.Series(True,index=p.index)
    if start is not None and end is not None:
        within=(p._t>=start)&(p._t<=end)
    pp=p[phase.str.startswith('P') & within]
    clusters=partition(pp,settings.gap_s,settings.max_span_s)
    alternatives={g:{frozenset(r['pick_id'] for r in x) for x in partition(pp,g,settings.max_span_s)}
                  for g in settings.sensitivity_gaps_s}
    candidates=[]; membership={}; sensitivity=[]
    for g,sets in alternatives.items():
        sensitivity.append(dict(window_id=window,gap_s=g,n_candidates=len(sets)))
    for cluster in clusters:
        df=pd.DataFrame(cluster); ids=frozenset(df.pick_id); key=cid(window,ids)
        flags=[]
        if any((g._t.max()-g._t.min())>settings.repeat_station_tolerance_s for _,g in df.groupby('_station')):
            flags.append('repeated_station_P')
        stable=all(ids in sets for sets in alternatives.values())
        if not stable: flags.append('gap_sensitive')
        historical=sorted(set(df.pick_group_id.dropna().astype(str))) if 'pick_group_id' in df else []
        candidates.append(dict(candidate_id=key,window_id=window,start_epoch=float(df._t.min()),
            end_epoch=float(df._t.max()),n_P_picks=len(df),n_stations=df._station.nunique(),
            historical_groups=json.dumps(historical),stable=stable,flags=';'.join(flags)))
        for pid in ids: membership[pid]=(key,'P_time_cluster')
    # Historical groups can support S assignment only when they identify exactly one cluster,
    # and the same station has a preceding P within the explicit S-P allowance.
    for row in p.to_dict('records'):
        pid=row['pick_id']
        if pid in membership: continue
        if not within.loc[p.index[p.pick_id.eq(pid)][0]]:
            membership[pid]=(None,'outside_waveform_window');continue
        if not str(row.get('phase','')).upper().startswith('S'):
            membership[pid]=(None,'unresolved_phase');continue
        group=row.get('pick_group_id')
        group_clusters={membership[r['pick_id']][0] for r in pp.to_dict('records')
                        if pd.notna(group) and r.get('pick_group_id')==group}
        possible=set()
        for r in pp.to_dict('records'):
            if r['_station']==row['_station'] and 0<=row['_t']-r['_t']<=settings.max_sp_s:
                possible.add(membership[r['pick_id']][0])
        possible &= group_clusters
        if len(possible)==1 and len(group_clusters)==1:
            membership[pid]=(possible.pop(),'historical_group_and_station_SP')
        else: membership[pid]=(None,'ambiguous_or_unmatched_S')
    # A historical group split across temporal candidates is a review issue, not an automatic fix.
    if 'pick_group_id' in pp:
        for _,g in pp.groupby('pick_group_id'):
            keys={membership[x][0] for x in g.pick_id}
            if len(keys)>1:
                for c in candidates:
                    if c['candidate_id'] in keys:
                        c['flags']=';'.join(filter(None,[c['flags'],'historical_group_split']))
    return candidates,[dict(pick_id=k,candidate_id=v[0],membership_reason=v[1],window_id=window)
                       for k,v in membership.items()],sensitivity


def trace_envelope(trace, settings):
    data=np.asanyarray(trace.data)
    if np.ma.isMaskedArray(data) and np.ma.getmaskarray(data).any():
        return None,'gapped_trace'
    data=np.asarray(data,dtype=float)
    if len(data)<3 or not np.isfinite(data).all(): return None,'invalid_trace'
    data=data-np.median(data)
    if np.ptp(data)==0: return None,'flat_trace'
    scale=np.max(np.abs(data)); data=data/scale
    width=max(1,int(round(settings.envelope_s*trace.stats.sampling_rate)))
    env=np.sqrt(np.maximum(0,uniform_filter1d(data*data,size=width)))
    return (data,env),'ok'


def measure_pick(stream, row, settings, cache):
    """RMS-envelope signal/baseline ratio around the existing pick, not an autopicker."""
    seed=row.get('waveform_seed_id')
    if pd.isna(seed) or not seed: seed=row['seed_id']
    streams=[tr for tr in stream if tr.id==seed]
    if not streams:
        return dict(energy_ratio=None,support=False,quality='channel_not_found',waveform_seed_id=seed)
    t=UTCDateTime(row['pick_time']); measures=[]
    for tr in streams:
        if id(tr) not in cache:
            cache[id(tr)] = trace_envelope(tr,settings)
        pair,quality=cache[id(tr)]
        if pair is None:
            measures.append((None,quality));continue
        x,env=pair; fs=tr.stats.sampling_rate; at=float(t-tr.stats.starttime)
        def segment(lo,hi,values):
            a=int(np.floor((at+lo)*fs));b=int(np.ceil((at+hi)*fs))
            if a<0 or b>len(values) or b<=a: return None
            return values[a:b]
        base=segment(-settings.baseline_start_s,-settings.baseline_end_s,env)
        signal=segment(-settings.signal_before_s,settings.signal_after_s,env)
        raw=segment(-settings.signal_before_s,settings.signal_after_s,x)
        if base is None or signal is None:
            measures.append((None,'window_not_covered'));continue
        # Flat-topped extrema are a warning only; no inference about instrument calibration.
        extrema=(np.abs(raw)>=np.max(np.abs(x))*(1-1e-10))
        longest=0; run=0
        for v in extrema:
            run=run+1 if v else 0;longest=max(longest,run)
        clipped=longest>=max(3,int(np.ceil(settings.clip_plateau_s*fs)))
        noise=float(np.median(base))
        if noise<1e-12:
            measures.append((None,'zero_baseline'));continue
        ratio=float(np.quantile(signal,.9)/noise)
        measures.append((ratio,'possible_clipping' if clipped else 'ok'))
    usable=[(r,q) for r,q in measures if q=='ok' and r is not None]
    if len(usable)!=1:
        return dict(energy_ratio=None,support=False,quality='overlapping_trace_segments' if len(usable)>1 else measures[0][1],waveform_seed_id=seed)
    ratio,quality=usable[0]
    return dict(energy_ratio=ratio,support=ratio>=settings.min_energy_ratio,quality=quality,waveform_seed_id=seed)


def propose_associations(candidates, origins, settings):
    """Keep every plausible temporal edge; automatic proposals must be mutually unique."""
    edges=[]
    if not candidates.empty:
        c=candidates.sort_values('start_epoch'); times=c.start_epoch.to_numpy()
        for row in origins.to_dict('records'):
            t=UTCDateTime(row['preferred_origin_time']).timestamp
            lo=np.searchsorted(times,t+settings.min_origin_to_p_s,side='left')
            hi=np.searchsorted(times,t+settings.max_origin_to_p_s,side='right')
            for cr in c.iloc[lo:hi].to_dict('records'):
                edges.append(dict(hypocenter_group_id=str(row['event_id']),candidate_id=cr['candidate_id'],
                                  first_P_minus_origin_s=cr['start_epoch']-t))
    edges=pd.DataFrame(edges,columns=['hypocenter_group_id','candidate_id','first_P_minus_origin_s'])
    degrees_h=edges.groupby('hypocenter_group_id').size();degrees_c=edges.groupby('candidate_id').size()
    status=candidates.set_index('candidate_id').status.to_dict() if len(candidates) else {}
    rows=[]
    by_h={h:g for h,g in edges.groupby('hypocenter_group_id')}
    for h in origins.event_id.astype(str):
        group=by_h.get(h)
        options=[] if group is None else group.candidate_id.tolist()
        accepted=None;reason='no_temporal_candidate'
        if len(options)>1: reason='multiple_candidate_clusters'
        elif len(options)==1:
            key=options[0]
            if degrees_c[key]>1: reason='multiple_hypocenter_groups'
            elif status[key]!='supported_candidate': reason='candidate_requires_review'
            else: accepted=key;reason='supported_unique_proposal'
        rows.append(dict(hypocenter_group_id=h,candidate_id=accepted,status=reason,
                         candidate_options=json.dumps(options)))
    return pd.DataFrame(rows),edges


def plot_window(stream, window, picks, candidates, origin_rows, path, settings):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    count=len(stream)
    fig,axes=plt.subplots(count+1,1,figsize=(14,max(4,1.6*count+2)),sharex=True,squeeze=False)
    axes=axes[:,0]; zero=min(tr.stats.starttime for tr in stream)
    for ax,tr in zip(axes,stream):
        data=np.asarray(tr.data,dtype=float); step=max(1,len(data)//6000)
        # Min/max blocks retain short transients when reducing plot size.
        n=len(data)//step
        if n:
            blocks=data[:n*step].reshape(n,step); t=np.arange(n)*step/tr.stats.sampling_rate+float(tr.stats.starttime-zero)
            ax.fill_between(t,blocks.min(axis=1),blocks.max(axis=1),color='0.45',lw=.2)
        ax.set_ylabel(tr.id,fontsize=7)
    for row in picks.to_dict('records'):
        t=float(UTCDateTime(row['pick_time'])-zero)
        seed=row.get('waveform_seed_id')
        if pd.isna(seed) or not seed: seed=row['seed_id']
        for ax,tr in zip(axes,stream):
            if tr.id==seed:
                ax.axvline(t,color='tab:blue' if str(row.get('phase')).upper().startswith('P') else 'tab:orange',lw=.7)
    for tr in stream:
        pair,quality=trace_envelope(tr,settings)
        if pair is not None:
            env=pair[1]; scale=max(float(np.max(env)),1e-12)
            step=max(1,len(env)//4000)
            axes[-1].plot(np.arange(0,len(env),step)/tr.stats.sampling_rate+float(tr.stats.starttime-zero),
                          env[::step]/scale,alpha=.35,lw=.6)
    for c in candidates.to_dict('records'):
        axes[-1].axvspan(c['start_epoch']-zero.timestamp,c['end_epoch']-zero.timestamp,
                        color='green' if c['status']=='supported_candidate' else 'orange',alpha=.35)
        axes[-1].text(c['start_epoch']-zero.timestamp,.8,c['candidate_id'][-6:],fontsize=7,rotation=45)
    for r in origin_rows.to_dict('records'):
        axes[-1].axvline(float(UTCDateTime(r['preferred_origin_time'])-zero),color='purple',lw=.8)
    axes[-1].set_ylim(0,1);axes[-1].set_yticks([])
    axes[-1].set_xlabel(f'Seconds after {zero} (UTC); blue=P, orange=S, purple=origin')
    fig.suptitle(window+' — candidate P clusters: green=supported, orange=review')
    fig.tight_layout();fig.savefig(path,dpi=120);plt.close(fig)


def run(paths, output_dir, settings=Settings()):
    settings.validate();out=prepare_output(output_dir)
    tables={k:pd.read_csv(paths[k],dtype={'event_id':str,'pick_id':str,'origin_id':str},low_memory=False)
            for k in INPUT_KEYS}
    picks=tables['MERGED_PICK_INDEX'];mapping=tables['PICK_MAP_CSV'];spine=tables['WAVEFORM_PICK_EVENT_INDEX'];origins=tables['HYPO71_EVENT_INDEX']
    if picks.pick_id.duplicated().any() or mapping.pick_id.duplicated().any() or spine.event_id.duplicated().any():
        raise ValueError('Duplicate pick/spine identifiers; resolve before clustering')
    if not set(mapping.pick_id)<=set(picks.pick_id) or not set(mapping.event_id)<=set(spine.event_id):
        raise ValueError('Map references missing picks or windows')
    # Canonical pick times and phases are authoritative; mapping supplies group and waveform identity.
    cols=[x for x in ['pick_id','event_id','pick_group_id','waveform_seed_id'] if x in mapping]
    mapped=mapping[cols].merge(picks.drop(columns=[k for k in cols if k!='pick_id'],errors='ignore'),on='pick_id',validate='one_to_one')
    candidates=[];members=[];sensitivity=[]
    spine_index=spine.set_index('event_id'); window_picks={w:g for w,g in mapped.groupby('event_id')}
    for w,g in window_picks.items():
        row=spine_index.loc[w];has_wave=pd.notna(row.get('wav_file'))
        cs,ms,ss=group_window(w,g,settings,start=UTCDateTime(row.starttime).timestamp if has_wave else None,
                             end=UTCDateTime(row.endtime).timestamp if has_wave else None)
        candidates.extend(cs);members.extend(ms);sensitivity.extend(ss)
    unmapped=set(picks.pick_id)-set(mapping.pick_id)
    for row in picks[picks.pick_id.isin(unmapped)].to_dict('records'):
        group=str(row.get('pha_file',''))+'|'+str(row.get('event_id',row['pick_id']))
        window='unmapped_'+hashlib.sha256(group.encode()).hexdigest()[:24]
        members.append(dict(pick_id=row['pick_id'],candidate_id=None,membership_reason='unmapped_upstream',window_id=window))
    cc=['candidate_id','window_id','start_epoch','end_epoch','n_P_picks','n_stations','historical_groups','stable','flags']
    c=pd.DataFrame(candidates,columns=cc);m=pd.DataFrame(members,columns=['pick_id','candidate_id','membership_reason','window_id'])
    if len(m)!=len(picks) or m.pick_id.duplicated().any() or set(m.pick_id)!=set(picks.pick_id):
        raise AssertionError('Pick conservation failure')
    waveform_windows=[w for w in window_picks if pd.notna(spine_index.loc[w].get('wav_file'))]
    rank=lambda w:hashlib.sha256(f'{settings.sample_seed}|{w}'.encode()).hexdigest()
    waveform_windows=sorted(waveform_windows,key=rank)
    selected=waveform_windows if settings.max_waveforms is None else waveform_windows[:settings.max_waveforms]
    # Review figures use a bounded, reproducible mixture of potentially difficult and ordinary windows.
    flagged=set(c.loc[c['flags'].ne('') | c.n_stations.lt(settings.min_stations),'window_id'])
    multi=set(c.window_id.value_counts()[lambda x:x>1].index)
    difficult=[w for w in selected if w in flagged|multi]
    ordinary=[w for w in selected if w not in flagged|multi]
    plot_selection=set((difficult[:(settings.max_plots+1)//2]+ordinary[:settings.max_plots//2]))
    measures=[];wave_audit=[];plot_cache={}
    for i,w in enumerate(selected):
        row=spine_index.loc[w];path=Path(row.wav_file)
        try:
            stream=read(str(path)); cache={}
            if not stream: raise ValueError('Empty waveform')
            group=window_picks[w];pg=group[group.phase.fillna('').str.upper().str.startswith('P')]
            for r in pg.to_dict('records'):
                measurement=measure_pick(stream,r,settings,cache)
                measures.append(dict(pick_id=r['pick_id'],window_id=w,station=station(measurement['waveform_seed_id']),
                                     **measurement))
            wave_audit.append(dict(window_id=w,path=str(path),status='read',sha256=digest(path),error=None))
            if w in plot_selection: plot_cache[w]=stream
        except Exception as exc:
            wave_audit.append(dict(window_id=w,path=str(path),status='read_error',sha256=None,error=str(exc)))
        if (i+1)%50==0: print(f'Waveforms checked: {i+1}/{len(selected)}',flush=True)
    e=pd.DataFrame(measures,columns=['pick_id','window_id','station','energy_ratio','support','quality','waveform_seed_id'])
    em=e.set_index('pick_id').to_dict('index')
    support=[];qualities=[];statuses=[]
    p_members={key:g.pick_id.tolist() for key,g in m[m.membership_reason.eq('P_time_cluster')].groupby('candidate_id')}
    for cr in c.to_dict('records'):
        ids=p_members.get(cr['candidate_id'],[])
        rows=[em[x] for x in ids if x in em]
        stations={x['station'] for x in rows if x['support']}
        quality=sorted({x['quality'] for x in rows if x['quality']!='ok'})
        if cr['n_stations']<settings.min_stations: status='insufficient_stations'
        elif cr['flags']: status='clustering_requires_review'
        elif len(rows)!=len(ids): status='waveform_unchecked_or_unavailable'
        elif quality: status='waveform_quality_review'
        elif len(stations)<settings.min_stations: status='weak_waveform_support'
        else: status='supported_candidate'
        support.append(len(stations));qualities.append(';'.join(quality));statuses.append(status)
    c['supporting_stations']=support;c['waveform_quality_flags']=qualities;c['status']=statuses
    associations,edges=propose_associations(c,origins,settings)
    queue=c[c.status.ne('supported_candidate')].copy()
    queue['review_decision']='';queue['review_notes']=''
    # Include temporally ambiguous associations even when the individual clusters have good signals.
    ambiguous=set(edges.loc[edges.hypocenter_group_id.isin(associations.loc[associations.status.ne('supported_unique_proposal'),'hypocenter_group_id']),'candidate_id'])
    queue=pd.concat([queue,c[c.candidate_id.isin(ambiguous) & c.status.eq('supported_candidate')]]).drop_duplicates('candidate_id')
    for col in ['review_decision','review_notes']: queue[col]=queue[col].fillna('')
    (out/'plots').mkdir()
    for w,stream in plot_cache.items():
        relevant=edges.loc[edges.candidate_id.isin(c.loc[c.window_id.eq(w),'candidate_id']),'hypocenter_group_id']
        plot_window(stream,w,window_picks[w],c[c.window_id.eq(w)],origins[origins.event_id.isin(relevant)],out/'plots'/f'{w}.png',settings)
    tables_out={'candidates.csv':c,'pick_membership.csv':m,'origin_associations.csv':associations,
                'origin_candidate_edges.csv':edges,'envelope_support.csv':e,
                'sensitivity.csv':pd.DataFrame(sensitivity),'review_queue.csv':queue,
                'waveform_audit.csv':pd.DataFrame(wave_audit,columns=['window_id','path','status','sha256','error'])}
    for name,df in tables_out.items(): df.to_csv(out/name,index=False)
    summary={'schema_version':1,'settings':asdict(settings),'inputs':{k:{'path':str(paths[k]),'sha256':digest(paths[k])} for k in INPUT_KEYS},
             'software_sha256':digest(__file__),'products':{n:digest(out/n) for n in tables_out},
             'pick_count':len(picks),'unmapped_upstream_picks':len(unmapped),'candidate_count':len(c),
             'candidate_status_counts':c.status.value_counts().to_dict(),
             'association_status_counts':associations.status.value_counts().to_dict(),
             'origin_decisions':associations.astype(object).where(pd.notna(associations),None).to_dict('records'),
             'picked_waveform_windows':len(waveform_windows),'waveforms_read':sum(r['status']=='read' for r in wave_audit),
             'waveform_errors':sum(r['status']=='read_error' for r in wave_audit),
             'plots':len(plot_cache),'mode':'diagnostic_proposals_no_catalog_changes'}
    (out/'manifest.json').write_text(json.dumps(summary,indent=2)+'\n')
    return summary


def integration_tables(paths, bundle):
    """Explicit opt-in: apply supported, mutually unique proposals; keep all other objects."""
    bundle=Path(bundle);manifest=json.loads((bundle/'manifest.json').read_text())
    for key,item in manifest['inputs'].items():
        if digest(paths[key])!=item['sha256']: raise ValueError(f'Stale candidate inputs: {key}')
    for name,sha in manifest['products'].items():
        if digest(bundle/name)!=sha: raise ValueError(f'Candidate diagnostic changed: {name}')
    c=pd.read_csv(bundle/'candidates.csv');m=pd.read_csv(bundle/'pick_membership.csv',dtype={'pick_id':str})
    a=pd.read_csv(bundle/'origin_associations.csv',dtype={'hypocenter_group_id':str})
    spine=pd.read_csv(paths['WAVEFORM_PICK_EVENT_INDEX'],dtype={'event_id':str})
    picks=pd.read_csv(paths['MERGED_PICK_INDEX'],dtype={'pick_id':str},low_memory=False)
    originals=spine.set_index('event_id').to_dict('index')
    rows=[];newmap=[]
    # All clusters are candidates; only supported mutual matches attach historical origins.
    for cr in c.to_dict('records'):
        row=dict(originals[cr['window_id']],event_id=cr['candidate_id'],candidate_id=cr['candidate_id'],
                 original_window_id=cr['window_id'],candidate_status=cr['status'])
        rows.append(row)
    for mr in m.to_dict('records'):
        candidate=mr['candidate_id']
        if pd.isna(candidate): candidate='unresolved_'+mr['window_id']
        newmap.append(dict(event_id=candidate,pick_id=mr['pick_id'],membership_reason=mr['membership_reason']))
    known={r['event_id'] for r in rows}
    unresolved=m[m.candidate_id.isna()]
    for w in unresolved.window_id.unique():
        row=originals.get(w,dict(event_class='PICKS_ONLY',wav_file=None,starttime=None,endtime=None))
        rows.append(dict(row,event_id='unresolved_'+w,original_window_id=w,
                         candidate_status='unresolved_pick_container'))
    handled=set(m.window_id)|set(c.window_id)
    for w,row in originals.items():
        if w not in handled: rows.append(dict(row,event_id=w,original_window_id=w,candidate_status='no_existing_picks'))
    s=pd.DataFrame(rows);mp=pd.DataFrame(newmap)
    if s.event_id.duplicated().any() or mp.pick_id.duplicated().any() or set(mp.pick_id)!=set(picks.pick_id):
        raise AssertionError('Candidate integration lost or duplicated picks/containers')
    supported=a[a.status.eq('supported_unique_proposal')]
    matches=pd.DataFrame({'event_id_h':supported.hypocenter_group_id,'event_id_w':supported.candidate_id,
                          'method':'supported_unique_candidate'})
    if matches.event_id_h.duplicated().any() or matches.event_id_w.duplicated().any():
        raise ValueError('Supported proposals are not one-to-one')
    return s,mp,matches,manifest
