#!/usr/bin/env python3
"""Plot station-specific event/noise metrics and pre/post artifact audit from CSVs.

Usage: python summary_plots.py event_metrics_output [--dpi 140]
Plots are regenerated without waveform reprocessing. Times are UTC.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def num(frame, name):
    return pd.to_numeric(frame[name], errors='coerce') if name in frame else pd.Series(np.nan, index=frame.index)


def save(fig, path, dpi):
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def scatter(ax, frame, field, **kw):
    ok = frame['time'].notna() & np.isfinite(frame[field])
    if ok.any():
        ax.scatter(frame.loc[ok, 'time'], frame.loc[ok, field], s=9, alpha=.45, **kw)


def plot_daily_amplitude_and_coverage(metrics, directory, dest, dpi, cfg):
    """Summarize observed event traces, not actual station uptime/detection thresholds.

    Each station-day minimum is the minimum *event-window RMS* of recorded
    events on that channel. It is not a calibrated detection threshold.
    """
    event = metrics.loc[metrics['window'].eq('event')].copy()
    event['date'] = event['time'].dt.floor('D')
    event['station'] = event['trace_id'].astype(str).str.split('.').str[1]
    event['event_rms'] = num(event, 'sample_rms')
    event = event.loc[event['date'].notna() & event['trace_id'].notna()]
    if event.empty:
        return
    # Explicitly include calendar days with no observed events; zeros indicate
    # zero coverage in *these files*, not proof of a non-operational station.
    days = pd.date_range(event['date'].min(), event['date'].max(), freq='D', tz='UTC')
    coverage = event.groupby('date').agg(
        event_files=('file', 'nunique'),
        stations=('station', 'nunique'),
        channels=('trace_id', 'nunique'),
        trace_observations=('trace_id', 'size'),
    ).reindex(days, fill_value=0)
    coverage.index.name = 'date_utc'
    coverage.to_csv(directory / 'daily_station_counts.csv')

    fig, axes = plt.subplots(2, 1, figsize=(14, 7), sharex=True)
    axes[0].step(days, coverage['stations'], where='mid', label='Distinct stations')
    axes[0].step(days, coverage['channels'], where='mid', label='Distinct channels', alpha=.7)
    axes[0].set(ylabel='Number represented', title='Daily station/channel representation in event WAV files')
    axes[0].legend()
    axes[1].bar(days, coverage['event_files'], width=.85)
    axes[1].set(ylabel='Unique event WAV files', xlabel='UTC date')
    for ax in axes: ax.grid(alpha=.2)
    save(fig, dest / 'stations_per_day.png', dpi)

    valid = event.loc[np.isfinite(event['event_rms']) & (event['event_rms'] > 0)].copy()
    if valid.empty:
        return
    summary = valid.groupby(['date', 'trace_id']).agg(
        minimum_rms=('event_rms', 'min'),
        p05_rms=('event_rms', lambda x: x.quantile(.05)),
        p10_rms=('event_rms', lambda x: x.quantile(.10)),
        median_rms=('event_rms', 'median'),
        n_events=('file', 'nunique'),
    ).reset_index()
    summary['stations_per_day'] = summary['date'].map(coverage['stations'])
    summary['channels_per_day'] = summary['date'].map(coverage['channels'])
    summary['event_files_per_day'] = summary['date'].map(coverage['event_files'])
    summary.to_csv(directory / 'daily_station_amplitude.csv', index=False)

    for sid, group in summary.groupby('trace_id', sort=True):
        safe = re.sub(r'[^A-Za-z0-9_.-]', '_', str(sid))
        group = group.sort_values('date')
        fig, axes = plt.subplots(3, 1, figsize=(13, 9), sharex=True,
                                 gridspec_kw={'height_ratios':[2, 1, 1]})
        axes[0].scatter(group['date'], group['minimum_rms'], s=28, label='Observed daily minimum RMS')
        axes[0].scatter(group['date'], group['p05_rms'], s=22, marker='^', label='Daily 5th percentile')
        axes[0].plot(group['date'], group['median_rms'], 'o', markersize=3, alpha=.65,
                     label='Daily median')
        axes[0].set(yscale='log', ylabel='Event-window RMS (counts)',
                    title=f'{sid}: observed lower amplitude envelope and network representation')
        axes[0].legend(fontsize=8)
        axes[1].step(days, coverage['stations'], where='mid')
        axes[1].set(ylabel='Stations/day')
        axes[2].bar(group['date'], group['n_events'], width=.85)
        axes[2].set(ylabel='Events/channel/day', xlabel='UTC date')
        for ax in axes: ax.grid(alpha=.2)
        save(fig, dest / f'{safe}_daily_min_amplitude.png', dpi)

    # Channel-by-day matrix of observed daily minima; missing cells stay blank.
    matrix = summary.pivot(index='trace_id', columns='date', values='minimum_rms')
    from matplotlib.colors import LogNorm
    values = matrix.to_numpy(dtype=float)
    masked = np.ma.masked_invalid(values)
    masked = np.ma.masked_less_equal(masked, 0)
    if masked.count():
        fig, ax = plt.subplots(figsize=(14, max(5, .43*len(matrix)+2)))
        image = ax.imshow(masked, aspect='auto', interpolation='nearest',
                          norm=LogNorm(vmin=float(masked.min()), vmax=float(masked.max())))
        ax.set_yticks(range(len(matrix.index)), labels=matrix.index)
        ax.set_xticks(range(len(matrix.columns)), labels=[d.strftime('%b %d') for d in matrix.columns], rotation=90)
        ax.set(xlabel='UTC date', title='Minimum observed event-window RMS by channel and day (counts)')
        fig.colorbar(image, ax=ax, label='Minimum observed RMS (counts)')
        save(fig, dest / 'daily_min_amplitude_heatmap.png', dpi)

def plot_daily_noise_and_detection(metrics, directory, dest, dpi, cfg):
    """Daily observed initial-window noise and event amplitude, by SEED ID.

    This is *event-file sampled* initial-window noise, not continuous noise
    monitoring or a verified pre-trigger window. A daily minimum of recorded
    event RMS is NOT an estimate of a detection threshold. All times UTC.
    """
    data = metrics.loc[metrics['window'].isin(['noise', 'event']),
                       ['file', 'trace_id', 'time', 'window', 'sample_rms']].copy()
    data['rms'] = pd.to_numeric(data['sample_rms'], errors='coerce')
    data = data.loc[data['time'].notna() & data['trace_id'].notna() &
                    np.isfinite(data['rms']) & (data['rms'] > 0)]
    if data.empty:
        return
    data['date'] = data['time'].dt.floor('D')
    # Pair noise and event from the same file and channel, not a day-level
    # quotient of unrelated samples. Duplicate rows are aggregated defensively.
    pairs = data.pivot_table(index=['file', 'trace_id'], columns='window',
                             values='rms', aggfunc='median').reset_index()
    pairs['event_to_initial_rms'] = pairs.get('event', np.nan) / pairs.get('noise', np.nan)
    pairs.to_csv(directory / 'event_initial_window_pairs.csv', index=False)

    noise = data.loc[data['window'].eq('noise')]
    event = data.loc[data['window'].eq('event')]
    ns = noise.groupby(['date', 'trace_id'])['rms'].agg(
        noise_n='count', noise_min_rms='min', noise_median_rms='median',
        noise_mean_rms='mean',
        noise_p10_rms=lambda x: x.quantile(.10),
        noise_p90_rms=lambda x: x.quantile(.90),
    ).reset_index()
    es = event.groupby(['date', 'trace_id'])['rms'].agg(
        event_n='count', event_min_rms='min', event_median_rms='median',
        event_p05_rms=lambda x: x.quantile(.05),
    ).reset_index()
    daily = ns.merge(es, on=['date', 'trace_id'], how='outer')
    # Derive dates from actual paired files for the ratio distribution.
    time_lookup = data.groupby(['file', 'trace_id'])['date'].first().rename('date').reset_index()
    ratios = pairs.merge(time_lookup, on=['file', 'trace_id'], how='left')
    rr = ratios.loc[np.isfinite(ratios['event_to_initial_rms']) &
                    (ratios['event_to_initial_rms'] > 0)]
    if not rr.empty:
        rs = rr.groupby(['date', 'trace_id'])['event_to_initial_rms'].agg(
            ratio_n='count', ratio_median='median',
            ratio_p10=lambda x: x.quantile(.10),
        ).reset_index()
        daily = daily.merge(rs, on=['date', 'trace_id'], how='left')
    daily.sort_values(['trace_id', 'date']).to_csv(directory / 'daily_station_noise.csv', index=False)

    coverage_path = directory / 'daily_station_counts.csv'
    if coverage_path.exists():
        coverage = pd.read_csv(coverage_path, parse_dates=['date_utc'])
        coverage['date_utc'] = pd.to_datetime(coverage['date_utc'], utc=True)
        coverage = coverage.set_index('date_utc')
    else:
        coverage = event.groupby('date').agg(
            stations=('trace_id', lambda x: x.str.split('.').str[1].nunique()),
            event_files=('file', 'nunique'))
    all_days = pd.date_range(data['date'].min(), data['date'].max(), freq='D', tz='UTC')
    coverage = coverage.reindex(all_days, fill_value=0)

    for sid, g in daily.groupby('trace_id', sort=True):
        g = g.sort_values('date')
        safe = re.sub(r'[^A-Za-z0-9_.-]', '_', str(sid))
        fig, axes = plt.subplots(4, 1, figsize=(14, 12), sharex=True,
                                 gridspec_kw={'height_ratios': [2, 1.2, 1, 1]})
        # Noise percentiles show variability and the evolving noise floor.
        axes[0].fill_between(g['date'], g['noise_p10_rms'].astype(float).to_numpy(),
                             g['noise_p90_rms'].astype(float).to_numpy(),
                             alpha=.20, label='Initial-window noise P10–P90')
        axes[0].plot(g['date'], g['noise_median_rms'], 'o-', ms=3,
                     label='Median initial-window noise')
        axes[0].plot(g['date'], g['event_min_rms'], 'v', ms=5,
                     label='Minimum recorded event RMS')
        axes[0].plot(g['date'], g['event_p05_rms'], '^', ms=4,
                     label='Event RMS P05')
        axes[0].set(yscale='log', ylabel='Filtered RMS (counts)',
                    title=f'{sid}: daily initial-window noise and observed event amplitudes')
        axes[0].legend(fontsize=8, ncol=2)
        axes[1].plot(g['date'], g.get('ratio_median', np.nan), 'o-', ms=3,
                     label='Median event / initial-window RMS')
        axes[1].plot(g['date'], g.get('ratio_p10', np.nan), 'v', ms=4,
                     label='10th percentile of event / initial-window RMS')
        axes[1].set(yscale='log', ylabel='RMS ratio (unitless)')
        axes[1].legend(fontsize=8)
        axes[2].step(all_days, coverage['stations'], where='mid')
        axes[2].set(ylabel='Stations in event files/day')
        axes[3].bar(g['date'], g['noise_n'], width=.85)
        axes[3].set(ylabel='Initial windows/day', xlabel='Date (UTC)')
        for ax in axes:
            ax.grid(alpha=.22)
        fig.text(.5, .005,
                 'Initial 5 s are not independently verified pre-trigger noise; counts are uncalibrated. '
                 'Minimum observed event amplitude is not a detection threshold.',
                 ha='center', fontsize=8)
        fig.subplots_adjust(bottom=.06)
        save(fig, dest / f'{safe}_daily_noise.png', dpi)

    # Daily noise heatmap by channel, missing station-days intentionally blank.
    matrix = daily.pivot(index='trace_id', columns='date', values='noise_median_rms')
    from matplotlib.colors import LogNorm
    vals = np.ma.masked_less_equal(np.ma.masked_invalid(matrix.to_numpy(dtype=float)), 0)
    if vals.count():
        fig, ax = plt.subplots(figsize=(14, max(5, .43 * len(matrix) + 2)))
        im = ax.imshow(vals, aspect='auto', interpolation='nearest',
                       norm=LogNorm(vmin=float(vals.min()), vmax=float(vals.max())))
        ax.set_yticks(range(len(matrix.index)), labels=matrix.index)
        ax.set_xticks(range(len(matrix.columns)),
                      labels=[d.strftime('%b %d') for d in matrix.columns], rotation=90)
        ax.set(xlabel='UTC date', title='Daily median initial 5-second RMS by channel (counts)')
        fig.colorbar(im, ax=ax, label='Median initial-window RMS (counts)')
        save(fig, dest / 'daily_noise_heatmap.png', dpi)

    # Network daily noise median is descriptive only: station population changes.
    network = daily.groupby('date').agg(
        channels=('trace_id', 'nunique'),
        median_channel_noise_rms=('noise_median_rms', 'median'),
        median_channel_event_min_rms=('event_min_rms', 'median'),
    ).reindex(all_days)
    network.index.name = 'date_utc'
    network.to_csv(directory / 'daily_network_noise_summary.csv')
    fig, axes = plt.subplots(2, 1, figsize=(14, 7), sharex=True)
    axes[0].plot(network.index, network['median_channel_noise_rms'], 'o-', ms=4,
                 label='Median of channel daily noise medians')
    axes[0].plot(network.index, network['median_channel_event_min_rms'], 'v-', ms=4,
                 label='Median of channel daily event minima')
    axes[0].set(yscale='log', ylabel='Filtered RMS (counts)',
                title='Observed noise and lower event envelope (changing station mix)')
    axes[0].legend(fontsize=9)
    axes[1].step(all_days, coverage['stations'], where='mid')
    axes[1].set(ylabel='Stations represented/day', xlabel='UTC date')
    for ax in axes:
        ax.grid(alpha=.2)
    save(fig, dest / 'daily_network_noise_and_coverage.png', dpi)


def generate_plots(directory, cfg=None):
    cfg = cfg or {}
    directory = Path(directory)
    dest = directory/'plots'
    dest.mkdir(parents=True, exist_ok=True)
    dpi = int(cfg.get('dpi', 140))
    audit = pd.read_csv(directory/'artifact_audit.csv', low_memory=False)
    metrics = pd.read_csv(directory/'trace_window_metrics.csv', low_memory=False)
    for df in (audit, metrics):
        df['time'] = pd.to_datetime(df['starttime'], utc=True, errors='coerce')
    amp_col = cfg.get('amplitude_metric', 'sample_rms')
    ratio_col = cfg.get('bandratio_metric', 'spectral.bandratio.ratio')
    metrics['amplitude'] = num(metrics, amp_col)
    metrics['ratio'] = num(metrics, ratio_col)
    metrics['meanf'] = num(metrics, 'spectral.meanf')
    plot_daily_amplitude_and_coverage(metrics, directory, dest, dpi, cfg)
    plot_daily_noise_and_detection(metrics, directory, dest, dpi, cfg)
    audit['peak'] = num(audit, 'peak_hz')
    audit['strength'] = num(audit, 'psd_ratio')
    audit['attenuation'] = num(audit, 'attenuation_db_at_pre_peak')
    audit['fixed_psd'] = num(audit, 'fixed_psd')
    audit['fixed_background'] = num(audit, 'fixed_background_psd')
    for col in ('noise_before_fixed_psd', 'event_before_fixed_psd',
                'noise_before_fixed_background_psd', 'event_before_fixed_background_psd'):
        audit[col] = num(audit, col)

    # Never connect separate event measurements across data gaps.
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    detected = audit['narrowband_detected'] if 'narrowband_detected' in audit else audit.get('detected', pd.Series(False,index=audit.index))
    detected = detected.astype(str).str.lower().eq('true')
    for sid, group in audit.groupby('trace_id'):
        scatter(axes[0], group[detected.loc[group.index]], 'peak', label=sid)
        scatter(axes[1], group, 'strength')
        scatter(axes[2], group, 'attenuation')
    axes[0].set(ylabel='Detected peak (Hz)', title='Narrowband artifact detections (38–43 Hz)')
    axes[1].set(ylabel='Peak/background PSD', yscale='log')
    axes[2].set(ylabel='Attenuation (dB)', xlabel='UTC')
    for ax in axes: ax.grid(alpha=.25)
    axes[0].legend(fontsize=7, ncol=4)
    save(fig, dest/'artifact_overview.png', dpi)

    # Coverage is a key confounder: record unique stations and event files per day.
    m_event = metrics[metrics['window'] == 'event'].copy()
    m_event['date'] = m_event['time'].dt.floor('D')
    if len(m_event):
        coverage = m_event.groupby('date').agg(events=('file','nunique'),
                                               stations=('trace_id',lambda x: x.str.split('.').str[1].nunique()),
                                               channels=('trace_id','nunique'))
        coverage.to_csv(directory/'daily_coverage.csv')
        fig, ax = plt.subplots(figsize=(13, 4))
        ax.plot(coverage.index, coverage['stations'], 'o-', label='Unique stations')
        ax.plot(coverage.index, coverage['channels'], '.-', label='Unique channels')
        ax.set(ylabel='Count', xlabel='UTC', title='Recording coverage (not operational uptime)')
        ax.legend(); ax.grid(alpha=.25)
        save(fig, dest/'station_coverage.png', dpi)

    # Separate plots per channel to avoid mixing sensitivities and instrument epochs.
    for sid in sorted(metrics['trace_id'].dropna().unique())[:int(cfg.get('max_station_plots',100))]:
        safe = re.sub(r'[^A-Za-z0-9_.-]', '_', str(sid))
        mm = metrics[metrics['trace_id'] == sid]
        event = mm[mm['window'] == 'event'].sort_values('time')
        noise = mm[mm['window'] == 'noise'].sort_values('time')
        aa = audit[audit['trace_id'] == sid].sort_values('time')
        if len(event):
            fig, axes = plt.subplots(4,1,figsize=(13,11),sharex=True)
            a = event['amplitude'].clip(lower=1e-30)
            valid = np.isfinite(a) & (a > 0)
            if valid.any():
                la = np.log10(a)
                lo, hi = la[valid].quantile([.05,.95]); hi = max(hi, lo+.01)
                sizes = np.clip(20+160*(la-lo)/(hi-lo), 10, 210)
            else:
                sizes = np.full(len(event), 20)
            axes[0].scatter(event['time'],event['ratio'],s=sizes,alpha=.5)
            axes[0].set_ylabel('Spectral band ratio')
            scatter(axes[1],event,'meanf')
            axes[1].set_ylabel('Mean frequency (Hz)')
            scatter(axes[2],event,'amplitude',label='Event')
            scatter(axes[2],noise,'amplitude',label='Initial 5-s window')
            axes[2].set(ylabel=f'RMS ({amp_col}; counts)',yscale='log')
            axes[2].legend()
            if len(aa):
                scatter(axes[3],aa,'fixed_psd',label='Fixed-frequency PSD')
                scatter(axes[3],aa,'fixed_background',label='Local background PSD')
            axes[3].set(ylabel='Original PSD (counts²/Hz)',yscale='log',xlabel='UTC')
            axes[3].legend(fontsize=8)
            axes[0].set_title(f'{sid}: event spectrum and amplitude; initial-window noise')
            for ax in axes: ax.grid(alpha=.25)
            save(fig,dest/f'{safe}_metrics.png',dpi)

        # Dedicated noise plot with before-filter fixed-frequency PSD and
        # filtered initial-window RMS; avoid interpreting initial window as verified pre-event noise.
        fig, axes = plt.subplots(3,1,figsize=(13,9),sharex=True)
        scatter(axes[0],noise,'amplitude')
        axes[0].set(ylabel='Initial-window RMS (counts)',yscale='log')
        if len(aa):
            scatter(axes[1],aa,'noise_before_fixed_psd')
            scatter(axes[2],aa,'noise_before_fixed_background_psd')
        axes[1].set(ylabel='40.45-Hz PSD (counts²/Hz)',yscale='log')
        axes[2].set(ylabel='Nearby PSD (counts²/Hz)',yscale='log',xlabel='UTC')
        axes[0].set_title(f'{sid}: initial 5-s window noise and unfiltered 40-Hz audit')
        for ax in axes: ax.grid(alpha=.25)
        save(fig,dest/f'{safe}_noise.png',dpi)

    # Per-station comparisons of absolute artifact PSD vs local broadband PSD.
    for sid in sorted(audit['trace_id'].dropna().unique())[:int(cfg.get('max_station_plots',100))]:
        aa = audit[audit['trace_id'] == sid]
        safe = re.sub(r'[^A-Za-z0-9_.-]', '_', str(sid))
        fig, axes = plt.subplots(2,1,figsize=(12,7),sharex=True)
        for field,label in [('fixed_psd','40.45 Hz PSD'),('fixed_background','Nearby PSD')]:
            scatter(axes[0],aa,field,label=label)
        for field,label in [('noise_before_fixed_psd','Initial window'),('event_before_fixed_psd','Event window')]:
            scatter(axes[1],aa,field,label=label)
        axes[0].set(ylabel='Full trace PSD (counts²/Hz)',yscale='log',title=f'{sid}: artifact versus broadband energy')
        axes[1].set(ylabel='40.45 Hz PSD (counts²/Hz)',yscale='log',xlabel='UTC')
        for ax in axes: ax.legend(fontsize=8);ax.grid(alpha=.25)
        save(fig,dest/f'{safe}_artifact.png',dpi)
    return dest


if __name__ == '__main__':
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('output_directory',type=Path)
    ap.add_argument('--dpi',type=int,default=140)
    ap.add_argument('--daily-noise-only',action='store_true', help='Generate daily noise summaries and plots only')
    args=ap.parse_args()
    if args.daily_noise_only:
        out = args.output_directory
        metrics = pd.read_csv(out / 'trace_window_metrics.csv', low_memory=False)
        metrics['time'] = pd.to_datetime(metrics['starttime'], utc=True, errors='coerce')
        dest = out / 'plots'
        dest.mkdir(parents=True, exist_ok=True)
        plot_daily_noise_and_detection(metrics, out, dest, args.dpi, {})
        print(dest)
    else:
        print(generate_plots(args.output_directory,{'dpi':args.dpi}))
