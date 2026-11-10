"""Paths shared by the Pinatubo notebooks; no directories created on import."""
from pathlib import Path
import os
import sys
_PROJECT = Path(__file__).resolve().parents[2]
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))
from pinatubo_config import read_config, resolve_path, external_output_root


def load_conversion_config(config_file=None):
    path = Path(config_file or os.environ.get('PINATUBO_CONVERSION_CONFIG') or
                Path(__file__).resolve().parents[1] / 'conversion_config.json').expanduser().resolve()
    config = read_config(path)
    config['_config_file'] = str(path)
    return config


def pipeline_paths(data_top=None, *, output_root=None, config_file=None):
    config = load_conversion_config(config_file)
    base = Path(config['_config_file']).parent
    source = resolve_path(data_top if data_top is not None else config['data_top'], relative_to=base)
    destination = external_output_root(output_root or os.environ.get('PINATUBO_CONVERSION_OUTPUT') or config['output_root'], relative_to=base)
    legacy = resolve_path(config['legacy_root'], relative_to=base) if config.get('legacy_root') else source / 'LEGACY'
    if destination == legacy or legacy in destination.parents or destination in legacy.parents:
        raise ValueError('Conversion output must not overlap the original LEGACY inputs')
    if os.environ.get('PINATUBO_RESULTS_ROOT') or os.environ.get('PINATUBO_PROJECT_CONFIG'):
        from pinatubo_project import settings,input_path
        destination=settings()['results_root']/'10_conversion/products'
        source=input_path('data_top')
        legacy=source/'LEGACY'
        if destination == legacy or legacy in destination.parents or destination in legacy.parents:
            raise ValueError('Conversion output must not overlap LEGACY inputs')
    p = {'HOME': str(Path.home()), 'DATA_TOP': str(source), 'OUTPUT_ROOT': str(destination),
         'CONFIG_FILE': config['_config_file'], 'NETWORK_HISTORY_OUTPUT': str(destination / 'network_history')}

    p['LEGACY_TOP'] = str(legacy)
    p['SUDS_TOP'] = f"{p['LEGACY_TOP']}/WAVEFORM_DATA/SUDS"
    p['FIX_FS'] = str(config.get('waveform', {}).get('sampling_rate', 100.0))
    p['DMX_GLOB'] = config.get('waveform', {}).get('pattern', '**/*.DMX')
    p['LEGACY_EVENTMETA_DIR'] = f"{p['LEGACY_TOP']}/EVENT_METADATA"
    p['LEGACY_PHA_MONTHLY_DIR'] = f"{p['LEGACY_EVENTMETA_DIR']}/MONTHLY_PHA"
    p['LEGACY_PHA_INDIVIDUAL_DIR'] = f"{p['LEGACY_EVENTMETA_DIR']}/PHA"
    p['LEGACY_HYPO_DIR'] = f"{p['LEGACY_EVENTMETA_DIR']}/HYPOCENTERS"
    p['FAIR_TOP'] = f"{p['OUTPUT_ROOT']}/FAIR"
    p['DB'] = config.get('database', 'PNTBO')
    p['NET'] = config.get('network', 'XB')
    p['SEISAN_TOP'] = f"{p['FAIR_TOP']}/SEISAN"
    p['SEISAN_WAV_DB'] = f"{p['SEISAN_TOP']}/WAV/{p['DB']}"
    p['FAIR_META_DIR'] = f"{p['FAIR_TOP']}/metadata"
    p['TMP_DIR'] = f"{p['OUTPUT_ROOT']}/pipeline_tmp"
    p['QC_DIR'] = f"{p['TMP_DIR']}/qc"
    p['WAVEFORM_INDEX'] = f"{p['TMP_DIR']}/10_waveform_index.csv"
    p['TRANSLATION_CSV'] = f"{p['FAIR_META_DIR']}/10_trace_id_mapping.csv"
    p['TRANSLATION_TEX'] = f"{p['FAIR_META_DIR']}/10_trace_id_mapping.tex"
    p['INDIV_PICK_INDEX'] = f"{p['TMP_DIR']}/20_individual_pick_index.csv"
    p['INDIV_LOGFILE'] = f"{p['TMP_DIR']}/20_individual_pick_parse_errors.log"
    p['MONTHLY_PICK_INDEX'] = f"{p['TMP_DIR']}/21_monthly_pick_index.csv"
    p['MONTHLY_PICK_ERR'] = f"{p['TMP_DIR']}/21_monthly_pick_parse_errors.log"
    p['MERGED_PICK_INDEX'] = f"{p['TMP_DIR']}/22_merged_pick_index.csv"
    p['MERGED_PICKS_SUPPRESSED'] = f"{p['TMP_DIR']}/22_suppressed_picks.csv"
    p['PICK_TIME_TOL'] = '0.5'
    p['STEP23_DIR'] = f"{p['QC_DIR']}/step23_pick_event_diagnostics"
    p['MAX_PS_DELAY'] = '15.0'
    p['MAX_STATION_COUNT'] = '20'
    p['INDIV_WAVEFORM_EVENT_CSV'] = f"{p['TMP_DIR']}/30_individual_waveform_event_index.csv"
    p['INDIV_PICK_WAVEFORM_MAP'] = f"{p['TMP_DIR']}/30_individual_pick_waveform_map.csv"
    p['INDIV_WAVEFORM_QC'] = f"{p['QC_DIR']}/30_individual_waveform_qc.csv"
    p['WAVEFORM_PICK_EVENT_INDEX'] = f"{p['TMP_DIR']}/32_waveform_pick_event_index.csv"
    p['PICK_MAP_CSV'] = f"{p['TMP_DIR']}/32_waveform_pick_event_map.csv"
    p['QC_CSV'] = f"{p['QC_DIR']}/32_event_catalog_qc.csv"
    p['STEP33_DIR'] = f"{p['QC_DIR']}/step33_event_association"
    p['STEP33_PLOTS'] = f"{p['STEP33_DIR']}/plots"
    p['SUMMARY_FILE_40'] = f"{p['LEGACY_HYPO_DIR']}/Pinatubo_all.sum"
    p['SUMMARY_FILE_40_INDEX'] = f"{p['TMP_DIR']}/40_summary_file_index.csv"
    p['SUMMARY_FILE_40_ERR'] = f"{p['TMP_DIR']}/40_summary_file_unparsed_lines.txt"
    p['SUMMARY_FILE_41'] = f"{p['LEGACY_HYPO_DIR']}/PINAALL.DAT"
    p['SUMMARY_FILE_41_INDEX'] = f"{p['TMP_DIR']}/41_summary_file_index.csv"
    p['SUMMARY_FILE_41_ERR'] = f"{p['TMP_DIR']}/41_summary_file_unparsed_lines.txt"
    p['COMPARE_PREFIX'] = f"{p['TMP_DIR']}/42_pinaalldat_vs_pinatuboallsum"
    p['HYPO71_EVENT_INDEX'] = f"{p['TMP_DIR']}/43_hypo71_event_index.csv"
    p['HYPO71_ORIGIN_INDEX'] = f"{p['TMP_DIR']}/43_hypo71_origin_index.csv"
    p['TIME_TOL_S'] = '5.0'
    p['DIST_TOL_KM'] = '15.0'
    p['PREFERRED_SOURCE'] = 'hypo40'
    p['HYPO_QC_DIR'] = f"{p['QC_DIR']}/step44_hypo71_diagnostics"
    p['CATALOG_PREFIX'] = f"{p['FAIR_META_DIR']}/50_pin_catalog"
    p['QUAKEML_ALL'] = f"{p['CATALOG_PREFIX']}.xml"
    p['QUAKEML_WAVEFORM'] = f"{p['CATALOG_PREFIX']}_waveform.xml"
    p['ORIGIN_TIME_TOL_S'] = '10.0'
    p['FAIR_REA_DIR'] = f"{p['FAIR_TOP']}/SEISAN/REA"
    p['DEFAULT_AUTHOR'] = 'GT__'
    p['DEFAULT_EVTYPE'] = 'L'
    p['SEISAN_DIAG_DIR'] = f"{p['QC_DIR']}/step53_seisan_rea_diagnostics"
    return p
