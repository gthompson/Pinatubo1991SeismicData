"""Shared input/output configuration for the Pinatubo conversion/analysis project.

Configuration is source; products, runtime caches and test artifacts live under
results_root. No bulk data moves, deletions, or silent local-output fallback.
"""
from pathlib import Path
import json
import os

PROJECT_ROOT=Path(__file__).resolve().parent


def _configuration():
    from pinatubo_config import read_config,resolve_path
    config_file=Path(os.environ.get('PINATUBO_PROJECT_CONFIG',PROJECT_ROOT/'project_config.json')).expanduser().resolve()
    cfg=read_config(config_file) if config_file.exists() else {}
    conversion=Path(os.environ.get('PINATUBO_CONVERSION_CONFIG',PROJECT_ROOT/'10_conversion/conversion_config.json')).expanduser().resolve()
    return cfg,config_file,read_config(conversion),conversion


def settings():
    from pinatubo_config import resolve_path,external_output_root
    cfg,path,conversion,cpath=_configuration()
    default=resolve_path(conversion['output_root'],relative_to=cpath.parent).parent
    value=os.environ.get('PINATUBO_RESULTS_ROOT') or cfg.get('results_root') or default
    root=external_output_root(value,relative_to=path.parent)
    if root.parts[:2]==('/','Volumes') and not Path(*root.parts[:3]).is_mount():
        raise FileNotFoundError(f'Results volume is unavailable: {root}')
    return {**cfg,'results_root':root,'config_file':path}


def output_path(stage,*parts,create=False):
    from pinatubo_config import read_config,analysis_products_root
    root=settings()['results_root']
    if os.environ.get('PINATUBO_RESULTS_ROOT') or os.environ.get('PINATUBO_PROJECT_CONFIG'):
        folders={'10_conversion':root/'10_conversion/products','20_analysis':root/'20_analysis/products','testing':root/'testing','runtime':root/'_runtime'}
    else:
        import sys
        if str(PROJECT_ROOT/'10_conversion') not in sys.path:sys.path.insert(0,str(PROJECT_ROOT/'10_conversion'))
        from pinatubo_steps.config import pipeline_paths
        folders={'10_conversion':Path(pipeline_paths()['OUTPUT_ROOT']),
                 '20_analysis':analysis_products_root(read_config(PROJECT_ROOT/'20_analysis/analysis_workflow.json'),PROJECT_ROOT/'20_analysis'),
                 'testing':root/'testing','runtime':root/'_runtime'}
    if stage not in folders:raise ValueError('Unknown results stage: '+stage)
    path=folders[stage].joinpath(*parts).resolve()
    if not path.is_relative_to(folders[stage].resolve()):raise ValueError('Output path escapes stage root')
    if create:path.mkdir(parents=True,exist_ok=True)
    return path


def input_path(name):
    from pinatubo_config import read_config,resolve_path
    cfg,path,conversion,cpath=_configuration()
    value=cfg.get('inputs',{}).get(name)
    if value is not None:return resolve_path(value,relative_to=path.parent)
    if name=='data_top':return resolve_path(conversion['data_top'],relative_to=cpath.parent)
    if name=='waveform_root':
        afile=PROJECT_ROOT/'20_analysis/analysis_workflow.json';a=read_config(afile)
        if a.get('wav_root'):return resolve_path(a['wav_root'],relative_to=afile.parent)
        return input_path('data_top')/'FAIR/SEISAN/WAV'/conversion.get('database','PNTBO')
    raise KeyError(name)


def runtime_environment():
    """Put generated plotting/GMT/Jupyter caches on the configured results disk."""
    root=output_path('runtime',create=True)
    for key,suffix in [('MPLCONFIGDIR','matplotlib'),('GMT_USERDIR','gmt'),
                       ('GMT_SESSIONDIR','gmt/sessions'),('IPYTHONDIR','ipython'),('PYTHONPYCACHEPREFIX','python')]:
        path=root/suffix;path.mkdir(parents=True,exist_ok=True)
        existing=os.environ.get(key)
        if not existing or not Path(existing).is_relative_to(settings()['results_root']):os.environ[key]=str(path)
    import sys
    sys.pycache_prefix=str(root/'python')
    return root


def resolve_artifact(path):
    """Resolve moved output paths in existing manifests/indexes without rewriting them.

    This function is for OUTPUT references only, never input waveform paths.
    Existing configured paths win. Legacy repository/FAIR output prefixes are
    mapped to the new root. An unmoved legacy file may still be read in place.
    """
    path=Path(path).expanduser()
    mappings=[(PROJECT_ROOT/'20_analysis/products',output_path('20_analysis')),
              (PROJECT_ROOT/'10_conversion/network_history/products',network_products()),
              (input_path('data_top')/'pipeline_tmp',_conversion_artifact('TMP_DIR')),
              (input_path('data_top')/'FAIR/metadata',_conversion_artifact('FAIR_META_DIR'))]
    for old,new in mappings:
        try:relative=path.relative_to(old)
        except ValueError:continue
        relocated=new/relative
        return relocated if relocated.exists() or not path.exists() else path
    return path


def analysis_products(config=None, *, override=None):
    candidate=override or (config or {}).get('products_directory')
    if not candidate and not (os.environ.get('PINATUBO_RESULTS_ROOT') or os.environ.get('PINATUBO_PROJECT_CONFIG')):
        from pinatubo_config import analysis_products_root
        return analysis_products_root(config or {},PROJECT_ROOT/'20_analysis')
    
    from pinatubo_config import external_output_root
    return external_output_root(candidate,relative_to=PROJECT_ROOT/'20_analysis') if candidate else output_path('20_analysis')


def network_products():
    return _conversion_artifact('NETWORK_HISTORY_OUTPUT')


def network_input(name, source_data):
    # Historical evidence remains in source; header refreshes are generated data.
    for generated in [network_products()/name,network_products()/'data'/name]:
        if generated.exists():return generated
    reference = _conversion_artifact('NETWORK_HISTORY_INPUT') / name
    return reference if reference.exists() else network_products()/name


def test_directory(*parts, create=True):
    return output_path('testing',*parts,create=create)


def _conversion_artifact(key):
    import sys
    if str(PROJECT_ROOT/'10_conversion') not in sys.path:sys.path.insert(0,str(PROJECT_ROOT/'10_conversion'))
    from pinatubo_steps.config import pipeline_paths
    return Path(pipeline_paths()[key])
