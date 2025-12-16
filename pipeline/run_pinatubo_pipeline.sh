#!/bin/bash
set -euo pipefail

###############################################################################
# CONFIGURATION
###############################################################################

DATA_TOP="$HOME/Dropbox/PROFESSIONAL/DATA/Pinatubo/ASSEMBLED-25-028-THOMPSON-PINATUBO1991"
CODE_TOP="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

# Legacy data 
LEGACY_TOP="$DATA_TOP/LEGACY"
SUDS_TOP="$LEGACY_TOP/WAVEFORM_DATA/SUDS"
FIX_FS=100.0
DMX_GLOB="**/*.DMX"
LEGACY_EVENTMETA_DIR="${LEGACY_TOP}/EVENT_METADATA"
LEGACY_PHA_MONTHLY_DIR="${LEGACY_EVENTMETA_DIR}/MONTHLY_PHA"
LEGACY_PHA_INDIVIDUAL_DIR="${LEGACY_EVENTMETA_DIR}/PHA"
LEGACY_HYPO_DIR="${LEGACY_EVENTMETA_DIR}/HYPOCENTERS"

# FAIR data
FAIR_TOP="$DATA_TOP/FAIR"
DB="PNTBO"
NET="XB"
SEISAN_TOP="${FAIR_TOP}/SEISAN"
SEISAN_WAV_DB="${SEISAN_TOP}/WAV/${DB}"
FAIR_META_DIR="${FAIR_TOP}/metadata"
mkdir -p "${FAIR_META_DIR}" "${SEISAN_WAV_DB}"

# Intermediate processing paths
TMP_DIR="${DATA_TOP}/pipeline_tmp"
QC_DIR="${TMP_DIR}/qc"
mkdir -p "${QC_DIR}"

###############################################################################
# STEP SWITCHES (match script numbering)
###############################################################################
ENABLE_STEP_10=false   # DMX → SEISAN WAV (+ index)
ENABLE_STEP_11=false   # Waveform archive diagnostics
ENABLE_STEP_20=false   # Individual PHA → CSV
ENABLE_STEP_21=false   # Monthly PHA → CSV
ENABLE_STEP_22=false   # Merge picks
ENABLE_STEP_23=false   # Plot pick/event diagnostics for Step 20/21/22
ENABLE_STEP_30=false   # Associate individual picks with waveforms
ENABLE_STEP_32=false   # Build waveform-centered event catalog (waveforms ↔ pick events)
ENABLE_STEP_33=false   # Plot waveform ↔ pick association diagnostics
ENABLE_STEP_40=false   # HYPO71 summary (Pinatubo_all.sum) → hypocenter index
ENABLE_STEP_41=false   # PINAALL.DAT → hypocenter index
ENABLE_STEP_42=false   # Compare hypocenter indexes
ENABLE_STEP_43=false   # Associate hypocenters into unified events
ENABLE_STEP_44=false   # Plot hypocenter diagnostics
ENABLE_STEP_50=true    # Build ObsPy Catalog (QuakeML)
ENABLE_STEP_52=true    # Build SEISAN REA catalog
ENABLE_STEP_53=true

###############################################################################
# STEP 10 — DMX → SEISAN WAV (+ index)
# This is the first step in the pipeline. It converts legacy DMX files to SEISAN WAV files
# and builds an index of the waveforms that we can use to associate picks and hypocenters with waveforms.
###############################################################################
WAVEFORM_INDEX="${TMP_DIR}/10_waveform_index.csv" 
TRANSLATION_CSV="${FAIR_META_DIR}/10_trace_id_mapping.csv"
TRANSLATION_TEX="${FAIR_META_DIR}/10_trace_id_mapping.tex"

if [ "${ENABLE_STEP_10}" = true ]; then
    echo "=== STEP 10: DMX → SEISAN WAV ==="
    python "${CODE_TOP}/10_dmx_to_seisanWAV.py" \
        --rawtop "${SUDS_TOP}" \
        --seisan-wav-db "${SEISAN_WAV_DB}" \
        --db "${DB}" \
        --net "${NET}" \
        --fix-fs "${FIX_FS}" \
        --glob "${DMX_GLOB}" \
        --out-waveform-index "${WAVEFORM_INDEX}" \
        --out-trace-id-map "${TRANSLATION_CSV}" \
        --out-trace-id-map-tex "${TRANSLATION_TEX}" \
        --verbose
else
    echo "=== STEP 10: SKIPPED ==="
fi
###############################################################################
# STEP 11 — Waveform archive diagnostics
###############################################################################
if [ "${ENABLE_STEP_11}" = true ]; then
    echo "=== STEP 11: Waveform time-series diagnostics ==="
    python "${CODE_TOP}/11_waveform_timeseries_diagnostics.py" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --outdir "${QC_DIR}" \
        --net "${NET}"
else
    echo "=== STEP 11: SKIPPED ==="
fi
###############################################################################
# STEP 20 — Individual PHA files → pick event index
# We only have individual PHA files for the two days: June 3 and 10, 1991
# Ideally we would have them for every waveform file, making association easier.
# Instead we use the monthly PHA files to fill in gaps, but these are more 
# challenging to parse and associate.
###############################################################################
INDIV_PICK_INDEX="${TMP_DIR}/20_individual_pick_index.csv"
INDIV_LOGFILE="${TMP_DIR}/20_individual_pick_parse_errors.log"

if [ "${ENABLE_STEP_20}" = true ]; then
    echo "=== STEP 20: Parsing individual PHA files ==="
    python "${CODE_TOP}/20_parse_individual_phase_files.py" \
        --pha-root "${LEGACY_PHA_INDIVIDUAL_DIR}" \
        --out-csv "${INDIV_PICK_INDEX}" \
        --error-log "${INDIV_LOGFILE}"
else
    echo "=== STEP 20: SKIPPED ==="
fi

###############################################################################
# STEP 21 — Monthly PHA files → pick event index
# We parse the monthly PHA files to get picks for May-Aug 1991, but these are
# secondary to the individual PHA files and are more challenging to parse.
###############################################################################
MONTHLY_PICK_INDEX="${TMP_DIR}/21_monthly_pick_index.csv"
MONTHLY_PICK_ERR="${TMP_DIR}/21_monthly_pick_parse_errors.log"

if [ "${ENABLE_STEP_21}" = true ]; then
    echo "=== STEP 21: Parsing monthly PHA files ==="
    python "${CODE_TOP}/21_parse_monthly_phase_files.py" \
        --pha-dir "${LEGACY_PHA_MONTHLY_DIR}" \
        --out-csv "${MONTHLY_PICK_INDEX}" \
        --error-log "${MONTHLY_PICK_ERR}"
else
    echo "=== STEP 21: SKIPPED ==="
fi

###############################################################################
# STEP 22 — Merge individual + monthly pick event indexes
# We treat the monthly picks as secondary to the individual picks, so we use a
# time tolerance to avoid double-counting picks that are close in time.
# Then we add unassociated monthly picks to the merged index.
###############################################################################
MERGED_PICK_INDEX="${TMP_DIR}/22_merged_pick_index.csv"
MERGED_PICKS_SUPPRESSED="${TMP_DIR}/22_suppressed_picks.csv"
PICK_TIME_TOL=0.5

if [ "${ENABLE_STEP_22}" = true ]; then
    echo "=== STEP 22: Merging phase picks ==="
    python "${CODE_TOP}/22_merge_picks.py" \
        --primary "${INDIV_PICK_INDEX}" \
        --secondary "${MONTHLY_PICK_INDEX}" \
        --out "${MERGED_PICK_INDEX}" \
        --time-tolerance "${PICK_TIME_TOL}" \
        --report "${MERGED_PICKS_SUPPRESSED}"
else
    echo "=== STEP 22: SKIPPED ==="
fi

###############################################################################
# STEP 23 — Plot pick/event diagnostics for Step 20/21/22
###############################################################################
STEP23_DIR="${QC_DIR}/step23_pick_event_diagnostics"
mkdir -p "${STEP23_DIR}"
MAX_PS_DELAY=15.0
MAX_STATION_COUNT=20

if [ "${ENABLE_STEP_23}" = true ]; then
    echo "=== STEP 23: Plotting pick/event diagnostics ==="
    python "${CODE_TOP}/23_plot_pick_event_diagnostics.py" \
        --individual "${INDIV_PICK_INDEX}" \
        --monthly "${MONTHLY_PICK_INDEX}" \
        --merged "${MERGED_PICK_INDEX}" \
        --outdir "${STEP23_DIR}" \
        --top-stations "${MAX_STATION_COUNT}" \
        --ps-delay-max "${MAX_PS_DELAY}" \
        --emit-csv \
        --emit-qc
else
    echo "=== STEP 23: SKIPPED ==="
fi

###############################################################################
# STEP 30 — Associate authoritative individual pick events with waveform events
# These are related by use of common YYMMDDNN SUDS identifiers (with extensions DMX or PHA)
###############################################################################
INDIV_WAVEFORM_EVENT_CSV="${TMP_DIR}/30_individual_waveform_event_index.csv"
INDIV_PICK_WAVEFORM_MAP="${TMP_DIR}/30_individual_pick_waveform_map.csv"
INDIV_WAVEFORM_QC="${QC_DIR}/30_individual_waveform_qc.csv"

if [ "${ENABLE_STEP_30}" = true ]; then
    echo "=== STEP 30: Associating individual picks with waveform files ==="
    python "${CODE_TOP}/30_associate_individual_picks_with_waveforms.py" \
        --merged-picks "${MERGED_PICK_INDEX}" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --out-event-csv "${INDIV_WAVEFORM_EVENT_CSV}" \
        --out-pick-map-csv "${INDIV_PICK_WAVEFORM_MAP}" \
        --out-qc-csv "${INDIV_WAVEFORM_QC}"
else
    echo "=== STEP 30: SKIPPED ==="
fi

###############################################################################
# STEP 32 — Associate all pick events to waveform events (and keep unassociated pick events)
# This effectively adds monthly pick events to the waveform-pick event index from step 30
###############################################################################
WAVEFORM_PICK_EVENT_INDEX="${TMP_DIR}/32_waveform_pick_event_index.csv"
PICK_MAP_CSV="${TMP_DIR}/32_waveform_pick_event_map.csv"
QC_CSV="${QC_DIR}/32_event_catalog_qc.csv"

if [ "${ENABLE_STEP_32}" = true ]; then
    echo "=== STEP 32: Building authoritative event catalog ==="
    python "${CODE_TOP}/32_build_waveform_pick_events.py" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --merged-picks "${MERGED_PICK_INDEX}" \
        --individual-pick-waveform-map "${INDIV_PICK_WAVEFORM_MAP}" \
        --out-event-csv "${WAVEFORM_PICK_EVENT_INDEX}" \
        --out-pick-map-csv "${PICK_MAP_CSV}" \
        --out-qc-csv "${QC_CSV}" \
        --time-tolerance 0.5
else
    echo "=== STEP 32: SKIPPED ==="
fi

###############################################################################
# STEP 33 — Plot waveform-pick association diagnostics
###############################################################################
STEP33_DIR="${QC_DIR}/step33_event_association"
STEP33_PLOTS="${STEP33_DIR}/plots"
mkdir -p "${STEP33_DIR}"

if [ "${ENABLE_STEP_33}" = true ]; then
    echo "=== STEP 33: Plotting event association diagnostics ==="
    python "${CODE_TOP}/33_plot_event_association_diagnostics.py" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --step30-event-csv "${INDIV_WAVEFORM_EVENT_CSV}" \
        --event-catalog "${WAVEFORM_PICK_EVENT_INDEX}" \
        --pick-map "${PICK_MAP_CSV}" \
        --outdir "${STEP33_DIR}"
else
    echo "=== STEP 33: SKIPPED ==="
fi

###############################################################################
# STEP 40 — HYPO71 summary file "Pinatubo_all.sum" → hypocenter index
# This file contains hypocenters from HYPO71, and it seems to be the most complete
# version in the legacy archive, so we use it as our initial reference.
###############################################################################
SUMMARY_FILE_40="${LEGACY_HYPO_DIR}/Pinatubo_all.sum"
SUMMARY_FILE_40_INDEX="${TMP_DIR}/40_summary_file_index.csv"
SUMMARY_FILE_40_ERR="${TMP_DIR}/40_summary_file_unparsed_lines.txt"

if [ "${ENABLE_STEP_40}" = true ]; then
    echo "=== STEP 40: Building hypocenter index from HYPO71 summary ==="
    python "${CODE_TOP}/40_build_hypocenter_index_pinatubo_all.py" \
        --summary-file "${SUMMARY_FILE_40}" \
        --out-csv "${SUMMARY_FILE_40_INDEX}" \
        --error-log "${SUMMARY_FILE_40_ERR}"
else
    echo "=== STEP 40: SKIPPED ==="
fi

###############################################################################
# STEP 41 — HYPO71 summary file "PINAALL.DAT" → hypocenter index
# This file also contains hypocenters from HYPO71, but it seems to be less complete than the
# Pinatubo_all.sum file, but it seems to have some additional hypocenters that are not in the Pinatubo_all.sum file.
###############################################################################
SUMMARY_FILE_41="${LEGACY_HYPO_DIR}/PINAALL.DAT"
SUMMARY_FILE_41_INDEX="${TMP_DIR}/41_summary_file_index.csv"
SUMMARY_FILE_41_ERR="${TMP_DIR}/41_summary_file_unparsed_lines.txt"

if [ "${ENABLE_STEP_41}" = true ]; then
    echo "=== STEP 41: Parsing PINAALL.DAT hypocenter file ==="
    python "${CODE_TOP}/41_build_hypocenter_index_pinaall.py" \
        --pinaall-file "${SUMMARY_FILE_41}" \
        --out-csv "${SUMMARY_FILE_41_INDEX}" \
        --error-log "${SUMMARY_FILE_41_ERR}"
else
    echo "=== STEP 41: SKIPPED ==="
fi

###############################################################################
# STEP 42 — Compare hypocenter indexes (exact match test)
# This step compares the hypocenter indexes from the two summary files and outputs a CSV file with the results.
###############################################################################
COMPARE_PREFIX="${TMP_DIR}/42_pinaalldat_vs_pinatuboallsum"

if [ "${ENABLE_STEP_42}" = true ]; then
    echo "=== STEP 42: Comparing PINAALL vs HYPO71 hypocenter indexes ==="
    python "${CODE_TOP}/42_compare_hypocenter_indexes.py" \
        --hypo40 "${SUMMARY_FILE_40_INDEX}" \
        --hypo41 "${SUMMARY_FILE_41_INDEX}" \
        --out-prefix "${COMPARE_PREFIX}"
else
    echo "=== STEP 42: SKIPPED ==="
fi

###############################################################################
# STEP 43 — Associate hypocenters into unified events
# This step associates hypocenters from the two summary files into unified events.
# We use a time and distance tolerance to associate hypocenters that are close in time and space.
###############################################################################
HYPO71_EVENT_INDEX="${TMP_DIR}/43_hypo71_event_index.csv"
HYPO71_ORIGIN_INDEX="${TMP_DIR}/43_hypo71_origin_index.csv"
TIME_TOL_S=5.0
DIST_TOL_KM=15.0
PREFERRED_SOURCE="hypo40"  # options: hypo40 | pinaall

if [ "${ENABLE_STEP_43}" = true ]; then
    echo "=== STEP 43: Associating hypocenters into events ==="
    python "${CODE_TOP}/43_associate_hypocenters.py" \
        --hypo40 "${SUMMARY_FILE_40_INDEX}" \
        --hypo41 "${SUMMARY_FILE_41_INDEX}" \
        --time-tol "${TIME_TOL_S}" \
        --dist-tol "${DIST_TOL_KM}" \
        --preferred-source "${PREFERRED_SOURCE}" \
        --emit-diagnostics \
        --diagnostics-dir "${QC_DIR}/step43_hypo_associate_diagnostics" \
        --out-event-csv "${HYPO71_EVENT_INDEX}" \
        --out-origin-csv "${HYPO71_ORIGIN_INDEX}"
else
    echo "=== STEP 43: SKIPPED ==="
fi

#################################################################################
# STEP 44 — Plot hypocenter diagnostics
#################################################################################
HYPO_QC_DIR="${QC_DIR}/step44_hypo71_diagnostics"
mkdir -p "${HYPO_QC_DIR}"

if [ "${ENABLE_STEP_44}" = true ]; then
    echo "=== STEP 44: Plot hypocenters per day ==="
    python "${CODE_TOP}/44_plot_hypocenters_per_day.py" \
        --hypo40 "${SUMMARY_FILE_40_INDEX}" \
        --hypo41 "${SUMMARY_FILE_41_INDEX}" \
        --out "${HYPO_QC_DIR}/44_hypocenters_per_day.png"
else
    echo "=== STEP 44: SKIPPED ==="
fi

###############################################################################
# STEP 50 — Build ObsPy Catalog (QuakeML)
# This step builds an ObsPy catalog from the waveform-pick event index and the hypocenter index.
# The catalog is written to a QuakeML file.
# The origin time tolerance is used to associate picks with hypocenters that are close in time,
# but this matching is not perfect.
###############################################################################
QUAKEML_OUT="${FAIR_META_DIR}/50_pin_catalog.xml"
ORIGIN_TIME_TOL_S=10.0

if [ "${ENABLE_STEP_50}" = true ]; then
    echo "=== STEP 50: Building ObsPy Catalog ==="
    python "${CODE_TOP}/50_build_obspy_catalog.py" \
        --waveform-event-index "${WAVEFORM_PICK_EVENT_INDEX}" \
        --waveform-pick-map "${PICK_MAP_CSV}" \
        --pick-index "${MERGED_PICK_INDEX}" \
        --hypo-event-index "${HYPO71_EVENT_INDEX}" \
        --hypo-origin-index "${HYPO71_ORIGIN_INDEX}" \
        --origin-time-tol "${ORIGIN_TIME_TOL_S}" \
        --out-quakeml "${QUAKEML_OUT}"
else
    echo "=== STEP 50: SKIPPED ==="
fi

###############################################################################
# STEP 52 — Build SEISAN REA catalog
# This step builds a SEISAN REA catalog from the QuakeML catalog we just wrote.
###############################################################################
FAIR_REA_DIR="${FAIR_TOP}/SEISAN/REA"
DEFAULT_AUTHOR="GT__"
DEFAULT_EVTYPE="L"

if [ "${ENABLE_STEP_52}" = true ]; then
    echo "=== STEP 52: Building SEISAN REA catalog ==="
    python "${CODE_TOP}/52_build_seisan_rea_catalog.py" \
        --quakeml "${QUAKEML_OUT}" \
        --rea-dir "${FAIR_REA_DIR}" \
        --author "${DEFAULT_AUTHOR}" \
        --evtype "${DEFAULT_EVTYPE}"
else
    echo "=== STEP 52: SKIPPED ==="
fi

###############################################################################
# STEP 53 — SEISAN REA sanity checks & diagnostics
###############################################################################
SEISAN_DIAG_DIR="${QC_DIR}/step53_seisan_rea_diagnostics"

mkdir -p "${SEISAN_DIAG_DIR}"

if [ "${ENABLE_STEP_53}" = true ]; then
    echo "=== STEP 53: SEISAN REA diagnostics ==="

    python "${CODE_TOP}/53_seisan_rea_diagnostics.py" \
        --rea-dir "${FAIR_REA_DIR}" \
        --db-name "PNTBO" \
        --out-dir "$SEISAN_DIAG_DIR" \
        --wavefile-regex "$WAVE_RE"
else
    echo "=== STEP 53: SKIPPED ==="
fi
