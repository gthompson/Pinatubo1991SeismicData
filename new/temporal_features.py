"""Reusable temporal/spectral features for volcanic seismic event traces.

Uses uncalibrated amplitudes; all times relative to event-window start.
No event-type labels are inferred. Undefined metrics are NaN.
"""
from __future__ import annotations
import numpy as np
from scipy.ndimage import uniform_filter1d
from scipy.signal import hilbert, find_peaks


def _q(x, p):
    return float(np.percentile(x, p)) if len(x) else np.nan


def _safe_div(a, b):
    return float(a / b) if np.isfinite(a) and np.isfinite(b) and b > 0 else np.nan


def _shape(y, fs, prefix, out, noise_rms=np.nan, threshold=3.0, smooth_s=.25):
    """Envelope shape, energy duration, peak/onset, and temporal concentration."""
    y = np.asarray(y, float)
    if y.size < 4 or not np.all(np.isfinite(y)):
        return None
    dt = 1. / fs
    e = np.abs(hilbert(y))
    env = uniform_filter1d(e, size=max(1, round(smooth_s*fs)), mode='nearest')
    power = env**2
    total = float(np.sum(power))
    if total <= 0:
        return None
    t = np.arange(len(y)) * dt
    cumulative = np.cumsum(power) / total
    def etime(frac):
        return float(t[min(np.searchsorted(cumulative, frac), len(t)-1)])
    t05, t10, t50, t90, t95 = [etime(q) for q in (.05,.10,.50,.90,.95)]
    peak = int(np.argmax(env))
    p = prefix
    out.update({
        p+'peak_time_s':float(t[peak]), p+'peak_envelope':float(env[peak]),
        p+'envelope_mean':float(np.mean(env)), p+'envelope_std':float(np.std(env)),
        p+'envelope_peak_to_mean':_safe_div(env[peak],float(np.mean(env))),
        p+'rms':float(np.sqrt(np.mean(y*y))), p+'energy_counts2_s':float(np.sum(y*y)*dt),
        p+'energy_t05_s':t05, p+'energy_t10_s':t10,
        p+'energy_t50_s':t50, p+'energy_t90_s':t90, p+'energy_t95_s':t95,
        p+'duration_t90_t10_s':t90-t10, p+'duration_t95_t05_s':t95-t05,
        p+'energy_first_quarter_fraction':float(cumulative[max(0,int(len(y)*.25)-1)]),
        p+'energy_first_half_fraction':float(cumulative[max(0,int(len(y)*.50)-1)]),
        p+'energy_last_half_fraction':float(1-cumulative[max(0,int(len(y)*.50)-1)]),
        p+'energy_last_quarter_fraction':float(1-cumulative[max(0,int(len(y)*.75)-1)]),
        p+'energy_time_centroid_s':float(np.sum(t*power)/total),
        p+'energy_time_spread_s':float(np.sqrt(np.sum((t-np.sum(t*power)/total)**2*power)/total)),
        p+'envelope_slope_first_half':float(np.polyfit(t[:max(3,len(t)//2)],env[:max(3,len(t)//2)],1)[0]),
        p+'envelope_slope_last_half':float(np.polyfit(t[len(t)//2:],env[len(t)//2:],1)[0]),
        p+'peak_count':int(len(find_peaks(env, prominence=.15*env[peak],distance=max(1,round(smooth_s*fs)))[0])),
    })
    if np.isfinite(noise_rms) and noise_rms > 0:
        out[p+'rms_snr']=_safe_div(out[p+'rms'],noise_rms)
        out[p+'peak_envelope_snr']=_safe_div(env[peak],noise_rms)
        above=np.flatnonzero(env >= threshold*noise_rms)
        out[p+'threshold_onset_s']=float(t[above[0]]) if len(above) else np.nan
        out[p+'threshold_offset_s']=float(t[above[-1]]) if len(above) else np.nan
        out[p+'threshold_duration_s']=float(t[above[-1]]-t[above[0]]) if len(above) else np.nan
        out[p+'above_threshold_fraction']=float(len(above)/len(t))
    else:
        out.update({p+k:np.nan for k in ('rms_snr','peak_envelope_snr','threshold_onset_s','threshold_offset_s','threshold_duration_s','above_threshold_fraction')})
    return env


def extract_temporal_features(trace, noise_trace=None, *, bands=None, smoothing_seconds=.25,
                              snr_threshold=3., filter_corners=4, early_seconds=2.,
                              transition_min_seconds=.5):
    """Compute full-band and per-band features, HF-LF timing and spectral evolution.

    `trace` and `noise_trace` are ObsPy Trace objects. Filter a COPY with ObsPy.
    Returned flat dict is directly usable as CSV columns. Event window starts t=0.
    """
    bands = bands or {'lf':(.8,4.), 'hf':(4.,20.)}
    fs=float(trace.stats.sampling_rate)
    y=np.asarray(trace.data,dtype=float)
    out={'temporal_sampling_rate_hz':fs,'temporal_window_duration_s':len(y)/fs,
         'temporal_smoothing_seconds':smoothing_seconds,'temporal_snr_threshold':snr_threshold}
    if len(y)<max(16,int(fs*.5)) or not np.all(np.isfinite(y)):
        return out
    nyquist=fs/2
    noise=np.asarray(noise_trace.data,float) if noise_trace is not None else None
    full_noise=float(np.sqrt(np.mean(noise**2))) if noise is not None and len(noise) else np.nan
    _shape(y,fs,'full_',out,full_noise,snr_threshold,smoothing_seconds)
    envs={}; energies={}
    for name,(lo,hi) in bands.items():
        prefix=name+'_'
        if not (0 < lo < hi < nyquist):
            out[prefix+'status']='invalid_band_nyquist'
            continue
        try:
            filtered=trace.copy().filter('bandpass',freqmin=lo,freqmax=hi,corners=filter_corners,zerophase=True)
            yn=np.asarray(filtered.data,float)
            nr=np.nan
            if noise_trace is not None and len(noise_trace.data)>=16:
                nf=noise_trace.copy().filter('bandpass',freqmin=lo,freqmax=hi,corners=filter_corners,zerophase=True)
                nr=float(np.sqrt(np.mean(np.asarray(nf.data,float)**2)))
            env=_shape(yn,fs,prefix,out,nr,snr_threshold,smoothing_seconds)
            if env is not None:
                envs[name]=env
                energies[name]=float(np.sum(yn**2)/fs)
                out[prefix+'status']='ok'
        except Exception as exc:
            out[prefix+'status']='filter_error:'+type(exc).__name__
    if 'hf' in envs and 'lf' in envs:
        h=envs['hf']; l=envs['lf']; eps=np.finfo(float).tiny
        out['hf_lf_peak_delay_s']=out['lf_peak_time_s']-out['hf_peak_time_s']
        for suffix in ('threshold_onset_s','threshold_offset_s','energy_t10_s','energy_t50_s','energy_t90_s','energy_time_centroid_s'):
            a=out.get('lf_'+suffix,np.nan);b=out.get('hf_'+suffix,np.nan)
            out['hf_lf_'+suffix.replace('_s','')+'_delay_s']=float(a-b) if np.isfinite(a) and np.isfinite(b) else np.nan
        out['hf_lf_total_energy_ratio']=_safe_div(energies['hf'],energies['lf'])
        out['hf_lf_log2_total_energy_ratio']=float(np.log2(out['hf_lf_total_energy_ratio'])) if out['hf_lf_total_energy_ratio']>0 else np.nan
        # Smooth instantaneous band energies. A transition requires a sustained
        # LF-dominant run AFTER a preceding HF-dominant interval.
        smooth_n=max(1,round(smoothing_seconds*fs))
        hp=uniform_filter1d(h*h,size=smooth_n,mode='nearest')
        lp=uniform_filter1d(l*l,size=smooth_n,mode='nearest')
        logratio=np.log2((np.maximum(hp,0)+eps)/(np.maximum(lp,0)+eps))
        out['hf_lf_ratio_early_median']=float(np.median(logratio[:max(1,min(len(y),round(early_seconds*fs)))]))
        out['hf_lf_ratio_late_median']=float(np.median(logratio[len(y)//2:]))
        out['hf_lf_ratio_change']=out['hf_lf_ratio_late_median']-out['hf_lf_ratio_early_median']
        out['hf_lf_ratio_positive_fraction']=float(np.mean(logratio>0))
        out['hf_lf_ratio_negative_fraction']=float(np.mean(logratio<0))
        n=max(1,round(transition_min_seconds*fs))
        positive=np.flatnonzero(logratio>0)
        trans=np.nan
        if len(positive):
            start=int(positive[0])+1
            for i in range(start,max(start,len(logratio)-n+1)):
                if np.all(logratio[i:i+n]<0) and np.any(logratio[:i]>0):
                    trans=i/fs;break
        out['hf_lf_transition_time_s']=trans
        out['hf_lf_envelope_correlation']=float(np.corrcoef(h,l)[0,1]) if np.std(h)>0 and np.std(l)>0 else np.nan
        out['hf_lf_peak_envelope_ratio']=_safe_div(out['hf_peak_envelope'],out['lf_peak_envelope'])
        # Cross-correlation lag of smoothed envelope shapes; positive means LF later.
        hc=h-h.mean();lc=l-l.mean()
        from scipy.signal import correlate, correlation_lags
        corr=correlate(lc,hc,mode='full',method='fft')
        lags=correlation_lags(len(lc),len(hc),mode='full')
        k=int(np.argmax(corr))
        out['hf_lf_envelope_xcorr_lag_s']=float(lags[k]/fs)
        out['hf_lf_envelope_xcorr_normalized_peak']=_safe_div(float(corr[k]),float(np.linalg.norm(hc)*np.linalg.norm(lc)))
        # Energy fractions in a fixed early window, and after the HF peak.
        split=max(1,min(len(y),round(early_seconds*fs)))
        for name in ('hf','lf'):
            e=envs[name]**2
            out[name+'_energy_early_fixed_fraction']=float(np.sum(e[:split])/np.sum(e))
            out[name+'_energy_late_fixed_fraction']=float(np.sum(e[split:])/np.sum(e))
        split=max(1,min(len(y),round(out['hf_peak_time_s']*fs)))
        out['lf_energy_after_hf_peak_fraction']=float(np.sum(l[split:]**2)/np.sum(l**2))
    return out
