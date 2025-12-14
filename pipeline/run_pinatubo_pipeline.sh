#!/bin/bash
set -euo pipefail

#Step 10 ─┐
#         ├─ Step 30 ─┐
#Step 20 ─┘           │
#                     ├─ Step 32 → Final Event Catalog
#Step 21 ── Step 22 ──┘

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

SUMMARY_FILE_40="${LEGACY_HYPO_DIR}/Pinatubo_all.sum"
PINAALL_DAT_FILE_41="${LEGACY_HYPO_DIR}/PINAALL.DAT"

FAIR_META_DIR="${FAIR_TOP}/metadata"
FAIR_PHA_DIR="${FAIR_META_DIR}/pha"
FAIR_HYPO_DIR="${FAIR_META_DIR}/hypo71"
FAIR_ASSOC_DIR="${FAIR_META_DIR}/association"
QC_DIR="${FAIR_META_DIR}/qc"

WAVEFORM_INDEX="${SEISAN_WAV_DB}/01_waveform_index.csv"

mkdir -p "${FAIR_PHA_DIR}" "${FAIR_HYPO_DIR}" "${FAIR_ASSOC_DIR}" "${QC_DIR}"

###############################################################################
# STEP SWITCHES (match script numbering)
###############################################################################
ENABLE_STEP_10=false   # DMX → SEISAN WAV (+ index)
ENABLE_STEP_11=false   # Waveform archive diagnostics
ENABLE_STEP_20=true   # Individual PHA → CSV
ENABLE_STEP_21=true   # Monthly PHA → CSV
ENABLE_STEP_22=true   # Merge picks
ENABLE_STEP_23=true   # Plot pick/event diagnostics for Step 20/21/22
ENABLE_STEP_30=true   # Associate individual picks with waveforms
ENABLE_STEP_32=true   # Build waveform-centered event catalog (waveforms ↔ pick events)
ENABLE_STEP_33=true   # Plot waveform ↔ pick association diagnostics
ENABLE_STEP_40=false   # HYPO71 summary (Pinatubo_all.sum) → hypocenter index
ENABLE_STEP_41=false   # PINAALL.DAT → hypocenter index
ENABLE_STEP_42=false   # Compare hypocenter indexes
ENABLE_STEP_43=false   # Associate hypocenters into unified events
ENABLE_STEP_50=false    # Build ObsPy Catalog (QuakeML)

###############################################################################
# STEP 10 — DMX → SEISAN WAV (+ index)
###############################################################################

if [ "${ENABLE_STEP_10}" = true ]; then
    echo "=== STEP 10: DMX → SEISAN WAV ==="
    python "${CODE_TOP}/10_dmx_to_seisanWAV.py" \
        --rawtop "${SUDS_TOP}" \
        --seisan-wav-db "${SEISAN_WAV_DB}" \
        --db "${DB}" \
        --net "${NET}" \
        --fix-fs "${FIX_FS}" \
        --glob "${DMX_GLOB}" \
        --out-waveform-index "${SEISAN_WAV_DB}/10_waveform_index.csv" \
        --out-trace-id-map "${FAIR_META_DIR}/10_trace_id_mapping.csv" \
        --out-trace-id-map-tex "${FAIR_META_DIR}/10_trace_id_mapping.tex" \
        --verbose
else
    echo "=== STEP 10: SKIPPED ==="
fi
###############################################################################
# STEP 11 — Waveform archive diagnostics
###############################################################################
ENABLE_STEP_11=true

WAVEFORM_QC_DIR="${FAIR_META_DIR}/waveform_qc"

if [ "${ENABLE_STEP_11}" = true ]; then
    echo "=== STEP 11: Waveform time-series diagnostics ==="
    python "${CODE_TOP}/11_waveform_timeseries_diagnostics.py" \
        --waveform-index "${SEISAN_WAV_DB}/10_waveform_index.csv" \
        --outdir "${WAVEFORM_QC_DIR}" \
        --net "${NET}"
else
    echo "=== STEP 11: SKIPPED ==="
fi
###############################################################################
# STEP 20 — Individual PHA files → pick index
###############################################################################
INDIV_PHA_CSV="${FAIR_PHA_DIR}/20_individual_pha_picks.csv"
INDIV_LOGFILE="${FAIR_PHA_DIR}/20_individual_pha_parse_errors.log"

if [ "${ENABLE_STEP_20}" = true ]; then
    echo "=== STEP 20: Parsing individual PHA files ==="
    python "${CODE_TOP}/20_parse_individual_phase_files.py" \
        --pha-root "${LEGACY_PHA_INDIVIDUAL_DIR}" \
        --out-csv "${INDIV_PHA_CSV}" \
        --error-log "${INDIV_LOGFILE}"
else
    echo "=== STEP 20: SKIPPED ==="
fi

###############################################################################
# STEP 21 — Monthly PHA files → pick index
###############################################################################
MONTHLY_PHA_CSV="${FAIR_PHA_DIR}/21_monthly_pha_picks.csv"
MONTHLY_PHA_ERR="${FAIR_PHA_DIR}/21_monthly_pha_parse_errors.log"

if [ "${ENABLE_STEP_21}" = true ]; then
    echo "=== STEP 21: Parsing monthly PHA files ==="
    python "${CODE_TOP}/21_parse_monthly_phase_files.py" \
        --pha-dir "${LEGACY_PHA_MONTHLY_DIR}" \
        --out-csv "${MONTHLY_PHA_CSV}" \
        --error-log "${MONTHLY_PHA_ERR}"
else
    echo "=== STEP 21: SKIPPED ==="
fi

###############################################################################
# STEP 22 — Merge individual + monthly picks
###############################################################################
MERGED_PHA_CSV="${FAIR_PHA_DIR}/22_merged_pha_picks.csv"
MERGED_PHA_SUPPRESSED="${FAIR_PHA_DIR}/22_suppressed_pha_picks.csv"

if [ "${ENABLE_STEP_22}" = true ]; then
    echo "=== STEP 22: Merging phase picks ==="
    python "${CODE_TOP}/22_merge_picks.py" \
        --primary "${INDIV_PHA_CSV}" \
        --secondary "${MONTHLY_PHA_CSV}" \
        --out "${MERGED_PHA_CSV}" \
        --time-tolerance 0.5\
        --report "${MERGED_PHA_SUPPRESSED}"
else
    echo "=== STEP 22: SKIPPED ==="
fi

###############################################################################
# STEP 23 — Plot pick/event diagnostics for Step 20/21/22
###############################################################################
STEP23_DIR="${QC_DIR}/step23_pick_event_diagnostics"
STEP23_PLOTS="${STEP23_DIR}/plots"
STEP23_CSV="${STEP23_DIR}/csv"
STEP23_QC_JSON="${STEP23_DIR}/23_qc_flags.json"

mkdir -p "${STEP23_DIR}"

if [ "${ENABLE_STEP_23}" = true ]; then
    echo "=== STEP 23: Plotting pick/event diagnostics ==="
    python "${CODE_TOP}/23_plot_pick_event_diagnostics.py" \
        --individual "${INDIV_PHA_CSV}" \
        --monthly "${MONTHLY_PHA_CSV}" \
        --merged "${MERGED_PICKS_CSV}" \
        --outdir "${STEP23_DIR}" \
        --top-stations 10 \
        --ps-delay-max 60 \
        --emit-csv \
        --emit-qc
else
    echo "=== STEP 23: SKIPPED ==="
fi

###############################################################################
# STEP 30 — Associate individual PHA events with waveform files
###############################################################################
INDIV_WAVEFORM_EVENT_CSV="${FAIR_ASSOC_DIR}/30_individual_waveform_event_index.csv"
INDIV_PICK_WAVEFORM_MAP="${FAIR_ASSOC_DIR}/30_individual_pick_waveform_map.csv"
INDIV_WAVEFORM_QC="${QC_DIR}/30_individual_waveform_qc.csv"


if [ "${ENABLE_STEP_30}" = true ]; then
    echo "=== STEP 30: Associating individual PHA events with waveform files ==="
    python "${CODE_TOP}/30_associate_individual_picks_with_waveforms.py" \
        --individual-picks "${INDIV_PHA_CSV}" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --out-event-csv "${INDIV_WAVEFORM_EVENT_CSV}" \
        --out-pick-map-csv "${INDIV_PICK_WAVEFORM_MAP}" \
        --out-qc-csv "${INDIV_WAVEFORM_QC}"
else
    echo "=== STEP 30: SKIPPED ==="
fi


###############################################################################
# STEP 32 — Build authoritative event catalog (waveforms + picks)
###############################################################################

EVENT_DIR="${FAIR_ASSOC_DIR}/event_catalog"
EVENT_CSV="${EVENT_DIR}/32_event_catalog.csv"
PICK_MAP_CSV="${EVENT_DIR}/32_event_pick_map.csv"
QC_CSV="${QC_DIR}/32_event_catalog_qc.csv"

mkdir -p "${EVENT_DIR}" "${QC_DIR}"

if [ "${ENABLE_STEP_32}" = true ]; then
    echo "=== STEP 32: Building authoritative event catalog ==="
    python "${CODE_TOP}/32_build_event_catalog.py" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --merged-picks "${MERGED_PICKS_CSV}" \
        --individual-pick-waveform-map "${INDIV_PICK_WAVEFORM_MAP}" \
        --out-event-csv "${EVENT_CSV}" \
        --out-pick-map-csv "${PICK_MAP_CSV}" \
        --out-qc-csv "${QC_CSV}" \
        --time-tolerance 0.5
else
    echo "=== STEP 32: SKIPPED ==="
fi

###############################################################################
# STEP 33 — Plot waveform ↔ pick association diagnostics
###############################################################################

STEP33_DIR="${QC_DIR}/step33_event_association"
STEP33_PLOTS="${STEP33_DIR}/plots"

mkdir -p "${STEP33_DIR}"

if [ "${ENABLE_STEP_33}" = true ]; then
    echo "=== STEP 33: Plotting event association diagnostics ==="
    python "${CODE_TOP}/33_plot_event_association_diagnostics.py" \
        --waveform-index "${WAVEFORM_INDEX}" \
        --step30-event-csv "${INDIV_WAVEFORM_EVENT_CSV}" \
        --event-catalog "${EVENT_CSV}" \
        --pick-map "${PICK_MAP_CSV}" \
        --outdir "${STEP33_DIR}"
else
    echo "=== STEP 33: SKIPPED ==="
fi

###############################################################################
# STEP 40 — HYPO71 summary → hypocenter index
###############################################################################
HYPO_CSV="${FAIR_HYPO_DIR}/40_hypocenter_index_pinatubo_all.csv"
HYPO_ERR="${FAIR_HYPO_DIR}/40_hypocenter_unparsed_lines.txt"

if [ "${ENABLE_STEP_40}" = true ]; then
    echo "=== STEP 40: Building hypocenter index from HYPO71 summary ==="
    python "${CODE_TOP}/40_build_hypocenter_index_pinatubo_all.py" \
        --summary-file "${SUMMARY_FILE_40}" \
        --out-csv "${HYPO_CSV}" \
        --error-log "${HYPO_ERR}"
else
    echo "=== STEP 40: SKIPPED ==="
fi

###############################################################################
# STEP 41 — PINAALL.DAT → hypocenter index
###############################################################################
PINAALL_CSV="${FAIR_HYPO_DIR}/41_pinaall_hypocenter_index.csv"
PINAALL_ERR="${FAIR_HYPO_DIR}/41_pinaall_unparsed_lines.txt"

if [ "${ENABLE_STEP_41}" = true ]; then
    echo "=== STEP 41: Parsing PINAALL.DAT hypocenter file ==="
    python "${CODE_TOP}/41_build_hypocenter_index_pinaall.py" \
        --pinaall-file "${PINAALL_DAT_FILE_41}" \
        --out-csv "${PINAALL_CSV}" \
        --error-log "${PINAALL_ERR}"
else
    echo "=== STEP 41: SKIPPED ==="
fi

###############################################################################
# STEP 42 — Compare hypocenter indexes (exact match test)
###############################################################################
COMPARE_DIR="${FAIR_HYPO_DIR}/comparisons"
COMPARE_PREFIX="${COMPARE_DIR}/42_pinaall_vs_hypo71"
mkdir -p "${COMPARE_DIR}"

if [ "${ENABLE_STEP_42}" = true ]; then
    echo "=== STEP 42: Comparing PINAALL vs HYPO71 hypocenter indexes ==="
    python "${CODE_TOP}/42_compare_hypocenter_indexes.py" \
        --hypo05 "${HYPO_CSV}" \
        --hypo06 "${PINAALL_CSV}" \
        --out-prefix "${COMPARE_PREFIX}"
else
    echo "=== STEP 42: SKIPPED ==="
fi

###############################################################################
# STEP 43 — Associate hypocenters into unified events
###############################################################################
HYPO_EVENT_DIR="${FAIR_HYPO_DIR}/events"
HYPO_EVENT_CSV="${HYPO_EVENT_DIR}/43_event_index.csv"
HYPO_ORIGIN_CSV="${HYPO_EVENT_DIR}/43_event_origins.csv"
mkdir -p "${HYPO_EVENT_DIR}"

TIME_TOL_S=5.0
DIST_TOL_KM=2.0
PREFERRED_SOURCE="hypo05"  # options: hypo05 | pinaall

if [ "${ENABLE_STEP_43}" = true ]; then
    echo "=== STEP 43: Associating hypocenters into events ==="
    python "${CODE_TOP}/43_associate_hypocenters.py" \
        --hypo05 "${HYPO_CSV}" \
        --hypo06 "${PINAALL_CSV}" \
        --time-tol "${TIME_TOL_S}" \
        --dist-tol "${DIST_TOL_KM}" \
        --preferred-source "${PREFERRED_SOURCE}" \
        --out-event-csv "${HYPO_EVENT_CSV}" \
        --out-origin-csv "${HYPO_ORIGIN_CSV}"
else
    echo "=== STEP 43: SKIPPED ==="
fi

###############################################################################
# STEP 50 — Build ObsPy Catalog (QuakeML)
###############################################################################
QUAKEML_DIR="${FAIR_TOP}/quakeml"
QUAKEML_OUT="${QUAKEML_DIR}/50_pin_catalog.xml"
mkdir -p "${QUAKEML_DIR}"

ORIGIN_TIME_TOL_S=10.0

if [ "${ENABLE_STEP_50}" = true ]; then
    echo "=== STEP 50: Building ObsPy Catalog ==="
    python "${CODE_TOP}/50_build_obspy_catalog.py" \
        --waveform-event-index "${EVENT_CSV}" \
        --waveform-pick-map "${PICK_MAP_CSV}" \
        --pick-index "${MERGED_PHA_CSV}" \
        --hypo-event-index "${HYPO_EVENT_CSV}" \
        --hypo-origin-index "${HYPO_ORIGIN_CSV}" \
        --origin-time-tol "${ORIGIN_TIME_TOL_S}" \
        --out-quakeml "${QUAKEML_OUT}"
else
    echo "=== STEP 50: SKIPPED ==="
fi