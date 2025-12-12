#!/bin/bash
set -e

###############################################################################
# USER CONFIGURATION — EDIT IF NEEDED
###############################################################################

DATA_TOP="$HOME/Dropbox/PROFESSIONAL/DATA/Pinatubo/ASSEMBLED-25-028-THOMPSON-PINATUBO1991"
FAIR_TOP="$DATA_TOP/FAIR"

# SEISAN WAV root (with YYYY/MM/ files inside)
WAV_ROOT="$FAIR_TOP/SEISAN/WAV"

# Metadata paths
MONTHLY_CSV="$FAIR_TOP/metadata/pha/pha_events.csv"
INDIVIDUAL_CSV="$DATA_TOP/LEGACY/EVENT_METADATA/PHA/individual_pha_events.csv"
WF_DISC="$FAIR_TOP/metadata/wfdisc_catalog.csv"

# Output
OUTDIR="$DATA_TOP/plots_all_mseed"

# Python script location (assumes launcher inside pipeline/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$SCRIPT_DIR/plot_picks_all_mseed.py"


###############################################################################
# CHECK PATHS
###############################################################################

echo "=== Checking paths ==="

for P in "$WAV_ROOT" "$MONTHLY_CSV" "$INDIVIDUAL_CSV" "$WF_DISC" "$SCRIPT"; do
    if [[ ! -e "$P" ]]; then
        echo "ERROR: Cannot find $P"
        exit 1
    fi
done

echo "Paths OK."
echo ""


###############################################################################
# RUN PYTHON SCRIPT
###############################################################################

echo "=== Running plot_picks_all_mseed.py ==="

python "$SCRIPT" \
    --wav-root "$WAV_ROOT" \
    --monthly-csv "$MONTHLY_CSV" \
    --individual-csv "$INDIVIDUAL_CSV" \
    --wfdisc "$WF_DISC" \
    --outdir "$OUTDIR"

echo ""
echo "=== DONE plotting ==="
echo "Plots saved under: $OUTDIR"