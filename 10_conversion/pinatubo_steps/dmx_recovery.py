"""Conservative diagnostic recovery: only complete structures before first defect.

No resynchronization, byte edits, clock correction, ID mapping or count offset.
Candidates are never merged into the production waveform/event archive.
"""
from pathlib import Path
import csv,hashlib,json,re,struct,tempfile
import numpy as np
from obspy import read,UTCDateTime
from obspy.io.dmx.core import descript_trace_dtypes,types


from flovopy.core.dmx_recovery import inspect_prefix,compare_ascii


def recover_file(source,output,*,ascii_file=None):
    source=Path(source);output=Path(output);blob=source.read_bytes();stop,descriptors,reason=inspect_prefix(blob)
    result={'source':str(source),'source_sha256':hashlib.sha256(blob).hexdigest(),'source_bytes':len(blob),'valid_prefix_bytes':stop,'stop_reason':reason,'descriptors':descriptors,'status':'no_complete_traces','time_policy':'Reported timestamps only; ObsPy epoch representation is not independent UTC validation.','data_policy':'Original samples and IDs; no 2048 subtraction, resampling, merging, zero-fill or clock correction.'}
    if not descriptors:return result
    with tempfile.TemporaryDirectory() as tmp:
        prefix=Path(tmp)/'prefix.DMX';prefix.write_bytes(blob[:stop]);stream=read(str(prefix),format='DMX')
    if len(stream)!=len(descriptors):raise ValueError('Descriptor/readback trace count differs')
    for tr,d in zip(stream,descriptors):
        # Independent metadata checks; raw samples were read only within validated boundaries.
        if tr.stats.npts!=d['npts'] or tr.stats.station!=d['station'] or tr.stats.sampling_rate!=d['rate']:raise ValueError('Readback metadata mismatch')
    check=compare_ascii(ascii_file,stream)
    mismatches=[r['trace_index'] for r in check.get('traces',[]) if r['status']=='mismatch']
    if mismatches:
        cutoff=min(mismatches)
        result['ascii_rejected_from_trace']=cutoff
        result['ascii_comparison_before_truncation']=check
        if cutoff==0:
            result.update(status='no_crossvalidated_prefix',ascii_comparison=check)
            return result
        stop=descriptors[cutoff]['offset'];descriptors=descriptors[:cutoff];stream=stream[:cutoff]
        result.update(valid_prefix_bytes=stop,descriptors=descriptors,stop_reason='ASCII disagreement: prefix stopped before first mismatching trace')
        check=compare_ascii(ascii_file,stream)
        check['traces']=[r for r in check.get('traces',[]) if r['trace_index']<cutoff]

    output.mkdir(parents=True,exist_ok=True)
    candidate=output/(source.stem+'.prefix.npz');prefix=output/(source.stem+'.prefix.DMX')
    for path in [candidate,prefix]:
        if path.exists():raise FileExistsError(path)
    np.savez_compressed(candidate,**{f'trace_{i:03d}':tr.data for i,tr in enumerate(stream)})
    with np.load(candidate,allow_pickle=False) as restored:
        if any(not np.array_equal(tr.data,restored[f'trace_{i:03d}']) for i,tr in enumerate(stream)):raise ValueError('Array round-trip differs')
    prefix.write_bytes(blob[:stop])
    result.update(status='candidate_partial' if stop<len(blob) else 'candidate_complete',traces=len(stream),nonconstant_seismic_traces=sum(not d['constant'] and not d['irig'] for d in descriptors),ascii_comparison=check,candidate_arrays=str(candidate),candidate_prefix_dmx=str(prefix))
    return result


def run(failures_csv,output,*,ascii_dir=None):
    output=Path(output);results=[]
    for row in csv.DictReader(Path(failures_csv).open()):
        source=Path(row['dmx_file']);ascii_file=Path(ascii_dir)/(source.stem+'.ASC') if ascii_dir else None
        try:r=recover_file(source,output,ascii_file=ascii_file)
        except Exception as exc:r={'source':str(source),'status':'failed','error':str(exc)}
        results.append(r)
    output.mkdir(parents=True,exist_ok=True)
    (output/'recovery_manifest.json').write_text(json.dumps(results,indent=2)+'\n')
    return results
