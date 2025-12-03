#!/bin/bash
set -e

###############################################################################
# CONFIGURATION (edit these paths as needed)
###############################################################################

# Legacy DMX input
RAW_TOP="/Users/glennthompson/Dropbox/PROFESSIONAL/DATA/Pinatubo/ASSEMBLED-25-028-THOMPSON-PINATUBO1991/LEGACY/WAVEFORM_DATA/SUDS"

# FAIR output root
FAIR_TOP="/Users/glennthompson/Dropbox/PROFESSIONAL/DATA/Pinatubo/ASSEMBLED-25-028-THOMPSON-PINATUBO1991/FAIR"

# SEISAN database output
SEISAN_TOP="${FAIR_TOP}/SEISAN"
SEISAN_WAV="${SEISAN_TOP}/WAV"
DB="PNTBO"
NET="XB"

# Sampling rate fix (metadata only)
FIX_FS=100.0

# DMX filename pattern
DMX_GLOB="**/*.DMX"

# Metadata paths
META_DIR="${FAIR_TOP}/metadata"
PHA_DIR="${META_DIR}/pha"
HYPO_DIR="${META_DIR}/hypo71"
ASSOC_DIR="${META_DIR}/association"
QC_DIR="${META_DIR}/qc"

mkdir -p "${META_DIR}" "${PHA_DIR}" "${HYPO_DIR}" "${ASSOC_DIR}" "${QC_DIR}"

###############################################################################
# STEP SWITCHES — enable/disable each stage easily
###############################################################################
ENABLE_STEP_01=false      # Convert DMX→MiniSEED (disable if already done)
ENABLE_STEP_02=true       # Index MiniSEED into wfdisc-like catalog
ENABLE_STEP_03=true       # Parse PHA monthly pick files
ENABLE_STEP_04=true       # Parse HYPO71 / SUM files
ENABLE_STEP_05=true       # Associate PHA + HYPO71 + WAV
ENABLE_STEP_06=true       # Build unified event catalog (QuakeML + optional SEISAN)
ENABLE_STEP_07=true       # QC and FAIR exports

###############################################################################
# STEP 01 — Convert legacy DMX → SEISAN-format MiniSEED
###############################################################################
if [ "$ENABLE_STEP_01" = true ]; then
    echo "=== STEP 01: Converting DMX → SEISAN/WAV MiniSEED ==="
    python 01_dmx_to_seisanWAV.py \
        --rawtop "${RAW_TOP}" \
        --seisan-top "${SEISAN_WAV}/${DB}" \
        --db "${DB}" \
        --net "${NET}" \
        --fix-fs "${FIX_FS}" \
        --glob "${DMX_GLOB}" \
        --verbose
else
    echo "=== STEP 01: SKIPPED ==="
fi

###############################################################################
# STEP 02 — Build wfdisc-like index from SEISAN WAV MiniSEED
###############################################################################
WF_DISC_CSV="${META_DIR}/wfdisc_catalog.csv"
WF_DISC_QML="${META_DIR}/wfdisc_catalog.xml"

if [ "$ENABLE_STEP_02" = true ]; then
    echo "=== STEP 02: Indexing MiniSEED files into wfdisc catalog ==="
    python 02_index_waveforms.py \
        --wav-root "${SEISAN_WAV}/${DB}" \
        --out-csv "${WF_DISC_CSV}" \
        --out-xml "${WF_DISC_QML}"
else
    echo "=== STEP 02: SKIPPED ==="
fi

###############################################################################
# STEP 03 — Parse PHA monthly phase pick files
###############################################################################
PHA_XML="${PHA_DIR}/phase_catalog.xml"

if [ "$ENABLE_STEP_03" = true ]; then
    echo "=== STEP 03: Parsing PHA phase files ==="
    python 03_parse_phase.py \
        --pha-dir "/data/Pinatubo/PHASE" \
        --out-xml "${PHA_XML}"
else
    echo "=== STEP 03: SKIPPED ==="
fi

###############################################################################
# STEP 04 — Parse HYPO71/SUM catalogs
###############################################################################
HYPO_XML="${HYPO_DIR}/hypo71_catalog.xml"
HYPO_UNPARSED="${HYPO_DIR}/unparsed_lines.txt"

if [ "$ENABLE_STEP_04" = true ]; then
    echo "=== STEP 04: Parsing HYPO71 location files ==="
    python 04_parse_hypo71.py \
        --sum-dir "/data/Pinatubo/SUM" \
        --out-xml "${HYPO_XML}" \
        --unparsed "${HYPO_UNPARSED}"
else
    echo "=== STEP 04: SKIPPED ==="
fi

###############################################################################
# STEP 05 — Associate PHA + HYPO71 + WAV
###############################################################################
MASTER_TABLE_CSV="${ASSOC_DIR}/master_event_table.csv"
MASTER_TABLE_PKL="${ASSOC_DIR}/master_event_table.pkl"

if [ "$ENABLE_STEP_05" = true ]; then
    echo "=== STEP 05: Associating PHA + HYPO71 + WAV ==="
    python 05_associate_phase_hypo71_waveforms.py \
        --wfdisc "${WF_DISC_CSV}" \
        --pha-catalog "${PHA_XML}" \
        --hypo71-catalog "${HYPO_XML}" \
        --outcsv "${MASTER_TABLE_CSV}" \
        --outpkl "${MASTER_TABLE_PKL}" \
        --time-window 2.0
else
    echo "=== STEP 05: SKIPPED ==="
fi

###############################################################################
# STEP 06 — Build unified QuakeML catalog + optional SEISAN REA
###############################################################################
UNIFIED_QML="${META_DIR}/unified_catalog.xml"

if [ "$ENABLE_STEP_06" = true ]; then
    echo "=== STEP 06: Building unified QuakeML catalog ==="
    python 06_build_unified_catalog.py \
        --master "${MASTER_TABLE_PKL}" \
        --outxml "${UNIFIED_QML}" \
        --write-seisan \
        --seisan-top "${SEISAN_TOP}/REA/${DB}"
else
    echo "=== STEP 06: SKIPPED ==="
fi

###############################################################################
# STEP 07 — QC diagnostics
###############################################################################
if [ "$ENABLE_STEP_07" = true ]; then
    echo "=== STEP 07: Running QC diagnostics ==="
    python 07_quality_control_and_exports.py \
        --master "${MASTER_TABLE_PKL}" \
        --outdir "${QC_DIR}"
else
    echo "=== STEP 07: SKIPPED ==="
fi

echo "=== Pipeline complete ==="