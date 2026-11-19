"""Convert the supplied COSPEC, COUNTS and RAD_FREQ records using shared paths."""
from pathlib import Path
import hashlib,json
from pinatubo_steps.config import pipeline_paths,load_conversion_config
from . import cospec,counts,radio,rain,tilt

FILES={'cospec':('COSPEC',['COS_PEC.DAT','BOB_QPLO.TXT']),
       'counts':('COUNTS',['CAB_CNT.DAT','CAB_HR.DAT','PIE_CNT.DAT','BOB_QPLO.TXT','BOB_SEIS.TXT']),
       'tilt':('TILT',[]),
       'rain':('RAIN',[]),
       'radio':('RAD_FREQ',['MAP.TXT','SEIS_MAP.OLD','SEIS_MAP.CUR'])}

def run(config_file=None, *, products=None):
    paths=pipeline_paths(config_file=config_file)
    cfg=load_conversion_config(config_file)
    selected=list(products) if products is not None else cfg.get('legacy_bob',{}).get('products',list(FILES))
    if not selected or set(selected)-set(FILES):raise ValueError('Select cospec, counts, radio and/or rain')
    source=Path(paths['LEGACY_TOP'])/'BOB'
    output=Path(paths['OUTPUT_ROOT'])/'legacy_bob'
    # Preflight every selected input before producing anything.
    inputs=[source/FILES[k][0]/n for k in selected for n in FILES[k][1]]
    rain_source=source/'RAIN'
    if 'rain' in selected:
        from pinatubo_config import resolve_path
        value=cfg.get('legacy_bob',{}).get('rain',{}).get('source')
        if value:rain_source=resolve_path(value,relative_to=Path(cfg['_config_file']).parent)
        if rain_source.is_file():inputs.append(rain_source)
        else:
            daily=[p for p in rain_source.iterdir() if p.is_file() and p.suffix.upper()=='.DAT']
            if not daily:raise ValueError('No daily RAIN files found')
            inputs.extend(daily)
            inputs.extend(p for p in rain_source.iterdir() if p.suffix.upper() in ('.TXT','.FOR'))
    if 'tilt' in selected:
        tilt_inputs=[p for p in (source/'TILT').rglob('*') if p.is_file() and p.suffix.upper() in ('.DAT','.TXT')]
        if not tilt_inputs:raise ValueError('No TILT inputs found')
        inputs.extend(tilt_inputs)
    for path in inputs:
        if path.resolve()==output.resolve() or output.resolve() in path.resolve().parents:
            raise ValueError('Output must not contain original inputs')
        if not path.is_file():raise FileNotFoundError(path)
    hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    result={}
    for product in selected:
        folder=source/FILES[product][0];dest=output/product
        dest.mkdir(parents=True,exist_ok=True)
        if product=='cospec':
            frame=cospec.read_cospec(folder/'COS_PEC.DAT')
            frame.to_csv(dest/'cospec_records.csv',index=False)
            cospec.read_bob_plot_definitions(folder/'BOB_QPLO.TXT').to_csv(dest/'bob_plot_definitions.csv',index=False)
            settings=cfg.get('legacy_bob',{}).get('cospec',{})
            units=settings.get('units','legacy units')
            basis=settings.get('units_basis','Not specified in source file')
            cospec.plot_cospec(frame,dest,units=units,units_basis=basis)
            metadata={**frame.attrs,'records':len(frame),'unique_dates':int(frame.recorded_time_gmt.dt.date.nunique()),'units':units,'units_basis':basis,'units_reference':settings.get('units_reference'),'aggregated':False,'interpolated':False}
            (dest/'cospec_metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
        elif product=='tilt':tilt.run(folder,dest)
        elif product=='rain':rain.run(rain_source,dest)
        elif product=='counts':counts.run(folder,dest)
        else:radio.run(folder,dest)
        result[product]=str(dest)
    if any(hashlib.sha256(Path(p).read_bytes()).hexdigest()!=digest for p,digest in hashes.items()):
        raise RuntimeError('An input changed during conversion')
    manifest={'conversion_config':cfg,'outputs':result,'input_sha256':hashes,
              'code_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('*.py')},
              'policy':'Original files and labels preserved; no clock repairs, interpolation or cross-version summing.'}
    (output/'conversion_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return result
