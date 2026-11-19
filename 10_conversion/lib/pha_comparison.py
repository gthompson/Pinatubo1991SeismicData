"""Read-only cross-source PHA validation; no event association or pick suppression.

Rows match on full normalized SEED ID and phase, then inclusive time tolerance.
All candidates are retained. A unique candidate is not necessarily a unique
pair: the counterpart may be reused by other rows. Source tables are copied.
"""
from pathlib import Path
import re
import math
import numpy as np
import pandas as pd
from .shared import normalize_seed_id_df, to_dt
from flovopy.core.event_utils import parse_seed_id


def _day(value):
    text = str(value)
    if re.fullmatch(r'\d{6}', text):
        return pd.to_datetime(text, format='%y%m%d', utc=True).strftime('%Y-%m-%d')
    return pd.Timestamp(value).strftime('%Y-%m-%d')


def _prepare(frame, source):
    if 'pick_time' not in frame:
        raise ValueError(f'{source}: missing pick_time column')
    if 'phase' not in frame:
        raise ValueError(f'{source}: missing phase column')
    if 'seed_id' not in frame and not {'station','channel'} <= set(frame):
        raise ValueError(f'{source}: need seed_id or station/channel columns')
    table = frame.copy(deep=True).reset_index(drop=True)
    # Reserve a namespace rather than overwriting any original provenance.
    if any(str(c).startswith('comparison_') for c in table.columns):
        raise ValueError('Input columns starting comparison_ are reserved')
    table['comparison_row_id'] = [f'{source}:{i}' for i in range(len(table))]
    table['comparison_source'] = source
    table['comparison_time'] = to_dt(table.pick_time)
    table['comparison_day'] = table.comparison_time.dt.strftime('%Y-%m-%d')
    table['comparison_seed_id'] = normalize_seed_id_df(table).str.upper()
    table['comparison_phase'] = table.phase.astype('string').fillna('').str.strip().str.upper()
    reasons = []
    for _, row in table.iterrows():
        parts = parse_seed_id(row.comparison_seed_id)
        invalid = []
        if pd.isna(row.comparison_time): invalid.append('invalid_pick_time')
        if parts[0] is None or not parts[1] or not parts[3]: invalid.append('missing_or_invalid_identity')
        if not row.comparison_phase: invalid.append('missing_phase')
        reasons.append(';'.join(invalid))
    table['comparison_invalid_reason'] = reasons
    table['comparison_valid'] = table.comparison_invalid_reason.eq('')
    return table


def _date_audit(table):
    rows = []
    for _, row in table.iterrows():
        path = next((str(row[key]) for key in ('pha_path','source_file','pha_file','raw_file')
                     if key in row and pd.notna(row[key]) and str(row[key]).strip()), '')
        parts = path.replace('\\','/').split('/')
        folder = next((p for p in reversed(parts[:-1]) if re.fullmatch(r'\d{6}',p)), None)
        filename = re.match(r'^(\d{6})', parts[-1]) if parts else None
        if filename is None and row.comparison_source == 'individual':
            filename = re.match(r'^(\d{6})', str(row.get('event_id','')))
        def parse(value):
            try: return _day(value) if value else None
            except (ValueError,TypeError): return None
        fday = parse(folder); nday = parse(filename[1] if filename else None)
        tday = row.comparison_day if pd.notna(row.comparison_day) else None
        flags = [label for label,a,b in [('folder_vs_filename',fday,nday),
                 ('folder_vs_pick',fday,tday),('filename_vs_pick',nday,tday)] if a and b and a!=b]
        known = sum(value is not None for value in (fday,nday,tday))
        rows.append(dict(comparison_row_id=row.comparison_row_id,source=row.comparison_source,
            pick_id=row.get('pick_id'),path=path,folder_day=fday,filename_day=nday,pick_day=tday,
            date_mismatch=bool(flags),issues=';'.join(flags),
            audit_status='mismatch' if flags else 'consistent' if known>=2 else 'insufficient_metadata'))
    return pd.DataFrame(rows,columns=['comparison_row_id','source','pick_id','path','folder_day',
        'filename_day','pick_day','date_mismatch','issues','audit_status'])


def compare_pick_sources(individual, monthly, *, tolerances=(0.05,1.0), dates=None):
    """Compare independent pick tables at each tolerance; return audit DataFrames.

    dates optionally selects reporting dates (YYMMDD or ISO dates). Matching
    still uses the complete inputs, so neighboring-day candidates are retained
    across midnight. Dates are UTC. Invalid rows are never silently discarded;
    unknown-time rows cannot be attributed to a selected reporting date.

    Summary rates use valid picks as denominators; empty denominators give NaN.
    Matches are existence/overlap checks, not one-to-one assignments. Pairs have
    signed delta = monthly time minus individual time. Mutual-unique pairs are
    also identified for less ambiguous timing statistics. An unmatched pick is
    not evidence of a bad parser or missing earthquake.
    """
    tolerances = sorted(set(float(t) for t in tolerances))
    if not tolerances or any(not math.isfinite(t) or t<0 for t in tolerances):
        raise ValueError('tolerances must contain finite, nonnegative seconds')
    selected = None if dates is None else sorted(set(_day(d) for d in dates))
    left=_prepare(individual,'individual');right=_prepare(monthly,'monthly')
    for table in (left,right):
        table['comparison_in_scope'] = True if selected is None else table.comparison_day.isin(selected)
    report_days = selected if selected is not None else sorted(set(left.comparison_day.dropna())|set(right.comparison_day.dropna()))
    valid_left=left[left.comparison_valid];valid_right=right[right.comparison_valid]
    keys=['comparison_seed_id','comparison_phase']
    right_groups={key:group.sort_values('comparison_time') for key,group in valid_right.groupby(keys)}
    max_ns=pd.Timedelta(seconds=max(tolerances)).value
    edges=[]
    for key,group in valid_left.groupby(keys):
        other=right_groups.get(key)
        if other is None:continue
        # Explicit ns resolution handles pandas versions with us timestamps.
        times=other.comparison_time.astype('datetime64[ns, UTC]').astype('int64').to_numpy()
        for row in group.itertuples(index=False):
            t=row.comparison_time.value
            start=np.searchsorted(times,t-max_ns,side='left');stop=np.searchsorted(times,t+max_ns,side='right')
            for counterpart in other.iloc[start:stop].itertuples(index=False):
                delta=counterpart.comparison_time.value-t
                edges.append(dict(individual_row_id=row.comparison_row_id,monthly_row_id=counterpart.comparison_row_id,
                    seed_id=key[0],phase=key[1],individual_time=row.comparison_time,monthly_time=counterpart.comparison_time,
                    delta_s=delta/1e9,abs_delta_s=abs(delta)/1e9,abs_delta_ns=abs(delta)))
    edgecols=['individual_row_id','monthly_row_id','seed_id','phase','individual_time','monthly_time','delta_s','abs_delta_s','abs_delta_ns']
    base=pd.DataFrame(edges,columns=edgecols)
    combined=pd.concat([left,right],ignore_index=True)
    lookup=combined.set_index('comparison_row_id')
    summaries=[];pair_tables=[];row_tables=[];timing=[]
    for tolerance in tolerances:
        pairs=base[base.abs_delta_ns <= pd.Timedelta(seconds=tolerance).value].copy()
        lc=pairs.individual_row_id.value_counts();rc=pairs.monthly_row_id.value_counts()
        pairs['individual_candidates']=pairs.individual_row_id.map(lc)
        pairs['monthly_candidates']=pairs.monthly_row_id.map(rc)
        pairs['mutual_unique']=(pairs.individual_candidates==1)&(pairs.monthly_candidates==1)
        pairs['tolerance_s']=tolerance
        rows=combined.copy()
        counts=pd.concat([lc,rc])
        rows['comparison_tolerance_s']=tolerance
        rows['comparison_candidates']=rows.comparison_row_id.map(counts).fillna(0).astype(int)
        mutual=set(pairs.loc[pairs.mutual_unique,'individual_row_id'])|set(pairs.loc[pairs.mutual_unique,'monthly_row_id'])
        reused=set(pairs.loc[pairs.monthly_candidates>1,'individual_row_id'])|set(pairs.loc[pairs.individual_candidates>1,'monthly_row_id'])
        rows['comparison_mutual_unique']=rows.comparison_row_id.isin(mutual)
        rows['comparison_reused_counterpart']=rows.comparison_row_id.isin(reused)
        rows['comparison_status']=np.select([~rows.comparison_valid,rows.comparison_candidates.eq(0),rows.comparison_candidates.gt(1)],
            ['invalid','unmatched','multiple_candidates'],default='single_candidate')
        row_tables.append(rows[rows.comparison_in_scope])
        for source in ('individual','monthly'):
            for day in ['ALL',*report_days]:
                subset=rows[(rows.comparison_source==source)&rows.comparison_in_scope]
                if day!='ALL':subset=subset[subset.comparison_day.eq(day)]
                valid=subset[subset.comparison_valid];n=len(valid);matched=int(valid.comparison_candidates.gt(0).sum())
                summaries.append(dict(source=source,date=day,tolerance_s=tolerance,total_picks=len(subset),
                    valid_picks=n,invalid_picks=len(subset)-n,matched=matched,unmatched=n-matched,
                    mutual_unique=int(valid.comparison_mutual_unique.sum()),multiple_candidates=int(valid.comparison_candidates.gt(1).sum()),
                    reused_counterpart=int(valid.comparison_reused_counterpart.sum()),match_rate=matched/n if n else np.nan,
                    coverage_status='observed' if n else 'invalid_only' if len(subset) else 'no_records'))
        visible=(pairs.individual_row_id.map(lookup.comparison_in_scope).fillna(False).astype(bool)|pairs.monthly_row_id.map(lookup.comparison_in_scope).fillna(False).astype(bool))
        pairs=pairs[visible].copy()
        for side in ('individual','monthly'):
            ids=pairs[side+'_row_id']
            for field in ('pick_id','event_id','pha_file','pha_path','source_file','raw_file','raw_line','raw_lineno','comparison_in_scope'):
                if field in lookup:pairs[side+'_'+field.removeprefix('comparison_')]=ids.map(lookup[field])
        pair_tables.append(pairs.drop(columns='abs_delta_ns'))
        for label,subset in [('all_candidates',pairs),('mutual_unique',pairs[pairs.mutual_unique])]:
            timing.append(dict(tolerance_s=tolerance,pair_set=label,pairs=len(subset),
                median_delta_s=subset.delta_s.median() if len(subset) else np.nan,
                mean_delta_s=subset.delta_s.mean() if len(subset) else np.nan,
                p95_abs_delta_s=subset.abs_delta_s.quantile(.95) if len(subset) else np.nan,
                max_abs_delta_s=subset.abs_delta_s.max() if len(subset) else np.nan))
    # Nearest same-identity rows are diagnostic candidates, never assignments.
    nearest=[]
    for source, own, others in [('individual',valid_left,valid_right),('monthly',valid_right,valid_left)]:
        groups={}
        for key, group in others.groupby(keys):
            group=group.sort_values('comparison_time')
            stamps=group.comparison_time.astype('datetime64[ns, UTC]').astype('int64').to_numpy()
            groups[key]=(group,stamps)
        for key,group in own.groupby(keys):
            item=groups.get(key)
            other,stamps=item if item is not None else (None,None)
            for row in group.itertuples(index=False):
                if not row.comparison_in_scope:continue
                if other is None:
                    nearest.append(dict(source=source,comparison_row_id=row.comparison_row_id,
                        counterpart_row_id=None,delta_s=np.nan,abs_delta_s=np.nan,
                        reason='no_same_identity',nearest_tie_count=0))
                    continue
                position=np.searchsorted(stamps,row.comparison_time.value)
                distance=min(abs(int(stamps[i])-row.comparison_time.value)
                             for i in (position-1,position) if 0<=i<len(stamps))
                targets={row.comparison_time.value-distance,row.comparison_time.value+distance}
                indices=[i for target in sorted(targets)
                         for i in range(np.searchsorted(stamps,target,side='left'),
                                        np.searchsorted(stamps,target,side='right'))]
                candidates=other.iloc[indices]
                for counterpart in candidates.itertuples(index=False):
                    delta=(counterpart.comparison_time.value-row.comparison_time.value)/1e9
                    if source=='monthly':delta=-delta
                    nearest.append(dict(source=source,comparison_row_id=row.comparison_row_id,
                        counterpart_row_id=counterpart.comparison_row_id,delta_s=delta,abs_delta_s=abs(delta),
                        reason='outside_largest_tolerance' if distance>max_ns else 'within_largest_tolerance',
                        nearest_tie_count=len(candidates)))
    nearest_table=pd.DataFrame(nearest,columns=['source','comparison_row_id','counterpart_row_id',
        'delta_s','abs_delta_s','reason','nearest_tie_count'])
    for field in ('pick_id','event_id','pha_file','pha_path','source_file','raw_file','raw_line','raw_lineno','pick_time'):
        if field in lookup:
            nearest_table[field]=nearest_table.comparison_row_id.map(lookup[field])
            nearest_table['counterpart_'+field]=nearest_table.counterpart_row_id.map(lookup[field])
    comparisons=pd.concat(row_tables,ignore_index=True)
    pairs=pd.concat(pair_tables,ignore_index=True)
    audit=_date_audit(combined)
    audit['comparison_in_scope']=audit.comparison_row_id.map(lookup.comparison_in_scope)
    invalid=combined[~combined.comparison_valid].copy()
    return dict(summary=pd.DataFrame(summaries),pairs=pairs,row_matches=comparisons,
        unmatched=comparisons[comparisons.comparison_status.eq('unmatched')].copy(),
        ambiguous_pairs=pairs[~pairs.mutual_unique].copy(),invalid=invalid,
        date_audit=audit,timing_summary=pd.DataFrame(timing),nearest_candidates=nearest_table)


def compare_pick_csvs(individual_csv, monthly_csv, *, outdir, tolerances=(.05,1.0),dates=None):
    """Read existing Step-20/21 products and write separate validation CSVs.

    Missing files raise FileNotFoundError; they are never treated as empty
    coverage. Inputs are not rewritten. Report filenames have a 23_pha_ prefix.
    """
    paths=[Path(individual_csv).expanduser(),Path(monthly_csv).expanduser()]
    for path in paths:
        if not path.is_file():raise FileNotFoundError(path)
    outputs=Path(outdir).expanduser()
    tables=[pd.read_csv(path,dtype={'pick_id':str,'event_id':str,'station':str,'channel':str,'seed_id':str,'phase':str}) for path in paths]
    result=compare_pick_sources(*tables,tolerances=tolerances,dates=dates)
    targets=[outputs/f'23_pha_{name}.csv' for name in result]
    if any(target.resolve() in {p.resolve() for p in paths} for target in targets):
        raise ValueError('Report output would overwrite an input')
    outputs.mkdir(parents=True,exist_ok=True)
    for (name,table),target in zip(result.items(),targets):table.to_csv(target,index=False)
    return result
