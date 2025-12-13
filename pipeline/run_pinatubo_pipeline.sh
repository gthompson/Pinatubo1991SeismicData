#!/bin/bash
set -e

###############################################################################
# CONFIGURATION
###############################################################################

DATA_TOP="$HOME/Dropbox/PROFESSIONAL/DATA/Pinatubo/ASSEMBLED-25-028-THOMPSON-PINATUBO1991"
CODE_TOP="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

LEGACY_TOP="$DATA_TOP/LEGACY"
SUDS_TOP="$LEGACY_TOP/WAVEFORM_DATA/SUDS"

FAIR_TOP="$DATA_TOP/FAIR"

DB="PNTBO"
NET="XB"

SEISAN_TOP="${FAIR_TOP}/SEISAN"
SEISAN_WAV_DB="${SEISAN_TOP}/WAV/${DB}"

FIX_FS=100.0
DMX_GLOB="**/*.DMX"

LEGACY_EVENTMETA_DIR="${LEGACY_TOP}/EVENT_METADATA"
LEGACY_PHA_MONTHLY_DIR="${LEGACY_EVENTMETA_DIR}/MONTHLY_PHA"
LEGACY_PHA_INDIVIDUAL_DIR="${LEGACY_EVENTMETA_DIR}/PHA"
LEGACY_HYPO_DIR="${LEGACY_EVENTMETA_DIR}/HYPOCENTERS"

SUMMARY_FILE_05="${LEGACY_HYPO_DIR}/Pinatubo_all.sum"
PINAALL_DAT_FILE_06="${LEGACY_HYPO_DIR}/PINAALL.DAT"

FAIR_META_DIR="${FAIR_TOP}/metadata"
FAIR_PHA_DIR="${FAIR_META_DIR}/pha"
FAIR_HYPO_DIR="${FAIR_META_DIR}/hypo71"
FAIR_ASSOC_DIR="${FAIR_META_DIR}/association"
WAVEFORM_INDEX="${SEISAN_WAV_DB}/01_waveform_index.csv"
QC_DIR="${FAIR_META_DIR}/qc"

mkdir -p "${FAIR_PHA_DIR}" "${FAIR_HYPO_DIR}" "${FAIR_ASSOC_DIR}" "${QC_DIR}"

###############################################################################
# STEP SWITCHES
###############################################################################
ENABLE_STEP_01=false   # DMX → MiniSEED
ENABLE_STEP_02=true    # Individual PHA → CSV
ENABLE_STEP_02b=true   # Individual PHA CSV & waveform event index association
ENABLE_STEP_03=true   # Monthly PHA → CSV
ENABLE_STEP_04=true    # Merge picks
ENABLE_STEP_05=false   # HYPO71 summary
ENABLE_STEP_05b=true   # Waveforms ↔ pick events
ENABLE_STEP_06=false
ENABLE_STEP_07=false
ENABLE_STEP_08=false
ENABLE_STEP_09=true

###############################################################################
# STEP 01 — DMX → SEISAN WAV (+ index)
###############################################################################
if [ "$ENABLE_STEP_01" = true ]; then
    echo "=== STEP 01: DMX → MiniSEED ==="
    python "${CODE_TOP}/01_dmx_to_seisanWAV.py" \
        --rawtop "${SUDS_TOP}" \
        --seisan-wav-db "${SEISAN_WAV_DB}" \
        --db "${DB}" \
        --net "${NET}" \
        --fix-fs "${FIX_FS}" \
        --glob "${DMX_GLOB}" \
        --verbose
else
    echo "=== STEP 01: SKIPPED ==="
fi

###############################################################################
# STEP 02 — Individual PHA files → pick index
###############################################################################
INDIV_PHA_CSV="${FAIR_PHA_DIR}/02_individual_pha_picks.csv"
INDIV_LOGFILE="${FAIR_PHA_DIR}/02_individual_pha_parse_errors.log"

if [ "$ENABLE_STEP_02" = true ]; then
    echo "=== STEP 02: Parsing individual PHA files ==="
    python "${CODE_TOP}/02_parse_individual_phase_files.py" \
        --pha-root "${LEGACY_PHA_INDIVIDUAL_DIR}" \
        --out-csv "${INDIV_PHA_CSV}" \
        --error-log "${INDIV_LOGFILE}"
else
    echo "=== STEP 02: SKIPPED ==="
fi

###############################################################################
# STEP 02b — Associate individual PHA events with waveform files
###############################################################################

INDIV_WAVEFORM_EVENT_CSV="${FAIR_ASSOC_DIR}/02b_individual_waveform_event_index.csv"
INDIV_PICK_WAVEFORM_MAP="${FAIR_ASSOC_DIR}/02b_individual_pick_waveform_map.csv"
INDIV_WAVEFORM_QC="${QC_DIR}/02b_individual_waveform_qc.csv"

mkdir -p "${FAIR_ASSOC_DIR}" "${QC_DIR}"

if [ "$ENABLE_STEP_02b" = true ]; then
    echo "=== STEP 02b: Associating individual PHA events with waveform files ==="

    python "${CODE_TOP}/02b_associate_individual_picks_with_waveforms.py" \
        --individual-picks "${INDIV_PHA_CSV}" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --out-event-csv "${INDIV_WAVEFORM_EVENT_CSV}" \
        --out-pick-map-csv "${INDIV_PICK_WAVEFORM_MAP}" \
        --out-qc-csv "${INDIV_WAVEFORM_QC}"

else
    echo "=== STEP 02b: SKIPPED ==="
fi

###############################################################################
# STEP 03 — Monthly PHA files → pick index
###############################################################################
MONTHLY_PHA_CSV="${FAIR_PHA_DIR}/03_monthly_pha_picks.csv"
MONTHLY_PHA_ERR="${FAIR_PHA_DIR}/03_monthly_pha_parse_errors.log"

if [ "$ENABLE_STEP_03" = true ]; then
    echo "=== STEP 03: Parsing monthly PHA files ==="
    python "${CODE_TOP}/03_parse_monthly_phase_files.py" \
        --pha-dir "${LEGACY_PHA_MONTHLY_DIR}" \
        --out-csv "${MONTHLY_PHA_CSV}" \
        --error-log "${MONTHLY_PHA_ERR}"
else
    echo "=== STEP 03: SKIPPED ==="
fi

###############################################################################
# STEP 04 — Merge individual + monthly picks
###############################################################################
MERGED_PHA_CSV="${FAIR_PHA_DIR}/04_merged_pha_picks.csv"

if [ "$ENABLE_STEP_04" = true ]; then
    echo "=== STEP 04: Merging phase picks ==="
    python "${CODE_TOP}/04_merge_picks_alt.py" \
        --primary "${INDIV_PHA_CSV}" \
        --secondary "${MONTHLY_PHA_CSV}" \
        --out "${MERGED_PHA_CSV}" \
        --time-tolerance 0.5
else
    echo "=== STEP 04: SKIPPED ==="
fi

###############################################################################
# STEP 05 — HYPO71 summary → hypocenter index
###############################################################################
HYPO_CSV="${FAIR_HYPO_DIR}/05_hypocenter_index.csv"
HYPO_ERR="${FAIR_HYPO_DIR}/05_hypocenter_unparsed_lines.txt"

if [ "$ENABLE_STEP_05" = true ]; then
    echo "=== STEP 05: Building hypocenter index from HYPO71 summary ==="
    python "${CODE_TOP}/05_build_hypocenter_index.py" \
        --summary-file "${SUMMARY_FILE_05}" \
        --out-csv "${HYPO_CSV}" \
        --error-log "${HYPO_ERR}"
else
    echo "=== STEP 05: SKIPPED ==="
fi

###############################################################################
# STEP 05b — Associate waveform files with pick events
###############################################################################


PICK_INDEX="${MERGED_PHA_CSV}"

WFP_EVENT_DIR="${FAIR_ASSOC_DIR}/waveform_pick_events"
WFP_EVENT_CSV="${WFP_EVENT_DIR}/05b_waveform_pick_event_index.csv"
WFP_PICK_MAP_CSV="${WFP_EVENT_DIR}/05b_waveform_pick_map.csv"
WFP_UNMATCHED_PICKS="${WFP_EVENT_DIR}/05b_unmatched_picks.csv"
WFP_UNMATCHED_WAVES="${WFP_EVENT_DIR}/05b_unmatched_waveforms.csv"

mkdir -p "${WFP_EVENT_DIR}"

if [ "$ENABLE_STEP_05b" = true ]; then
    echo "=== STEP 05b: Building waveform↔pick-event association ==="

    python "${CODE_TOP}/05b_build_waveform_pick_events.py" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --pick-index "${PICK_INDEX}" \
        --out-event-csv "${WFP_EVENT_CSV}" \
        --out-pick-map-csv "${WFP_PICK_MAP_CSV}" \
        --out-unmatched-picks "${WFP_UNMATCHED_PICKS}" \
        --out-unmatched-waveforms "${WFP_UNMATCHED_WAVES}"

else
    echo "=== STEP 05b: SKIPPED ==="
fi

###############################################################################
# STEP 06 — PINAALL.DAT → hypocenter index
###############################################################################
PINAALL_CSV="${FAIR_HYPO_DIR}/06_pinaall_hypocenter_index.csv"
PINAALL_ERR="${FAIR_HYPO_DIR}/06_pinaall_unparsed_lines.txt"

if [ "$ENABLE_STEP_06" = true ]; then
    echo "=== STEP 06: Parsing PINAALL.DAT hypocenter file ==="
    python "${CODE_TOP}/06_build_hypocenter_index_pinaall.py" \
        --pinaall-file "${PINAALL_DAT_FILE_06}" \
        --out-csv "${PINAALL_CSV}" \
        --error-log "${PINAALL_ERR}"
else
    echo "=== STEP 06: SKIPPED ==="
fi

###############################################################################
# STEP 07 — Compare hypocenter indexes (exact match test)
###############################################################################
COMPARE_DIR="${FAIR_HYPO_DIR}/comparisons"
COMPARE_PREFIX="${COMPARE_DIR}/07_pinaall_vs_hypo71"

mkdir -p "${COMPARE_DIR}"

if [ "$ENABLE_STEP_07" = true ]; then
    echo "=== STEP 07: Comparing PINAALL vs HYPO71 hypocenter indexes ==="
    python "${CODE_TOP}/07_compare_hypocenter_indexes.py" \
        --hypo05 "${HYPO_CSV}" \
        --hypo06 "${PINAALL_CSV}" \
        --out-prefix "${COMPARE_PREFIX}"
else
    echo "=== STEP 07: SKIPPED ==="
fi

###############################################################################
# STEP 08 — Associate hypocenters into unified events
###############################################################################

EVENT_DIR="${FAIR_HYPO_DIR}/events"
EVENT_CSV="${EVENT_DIR}/08_event_index.csv"
ORIGIN_CSV="${EVENT_DIR}/08_event_origins.csv"

TIME_TOL_S=5.0        # seconds
DIST_TOL_KM=2.0       # kilometers

# Preferred source for primary origin
PREFERRED_SOURCE="hypo05"  # options: hypo05 | pinaall

if [ "$ENABLE_STEP_08" = true ]; then
    echo "=== STEP 08: Associating hypocenters into events ==="

    python "${CODE_TOP}/08_associate_hypocenters.py" \
        --hypo05 "${HYPO_CSV}" \
        --hypo06 "${PINAALL_CSV}" \
        --time-tol "${TIME_TOL_S}" \
        --dist-tol "${DIST_TOL_KM}" \
        --preferred-source "${PREFERRED_SOURCE}" \
        --out-event-csv "${EVENT_CSV}" \
        --out-origin-csv "${ORIGIN_CSV}"

else
    echo "=== STEP 08: SKIPPED ==="
fi

###############################################################################
# STEP 09 — Build ObsPy Catalog (QuakeML)
###############################################################################

QUAKEML_DIR="${FAIR_TOP}/quakeml"
QUAKEML_OUT="${QUAKEML_DIR}/09_pin_catalog.xml"

ORIGIN_TIME_TOL_S=10.0

mkdir -p "${QUAKEML_DIR}"

if [ "$ENABLE_STEP_09" = true ]; then
    echo "=== STEP 09: Building ObsPy Catalog ==="

    python "${CODE_TOP}/09_build_obspy_catalog.py" \
        --waveform-event-index "${WFP_EVENT_CSV}" \
        --waveform-pick-map "${WFP_PICK_MAP_CSV}" \
        --pick-index "${MERGED_PHA_CSV}" \
        --hypo-event-index "${EVENT_CSV}" \
        --hypo-origin-index "${ORIGIN_CSV}" \
        --origin-time-tol "${ORIGIN_TIME_TOL_S}" \
        --out-quakeml "${QUAKEML_OUT}"

else
    echo "=== STEP 09: SKIPPED ==="
fi

