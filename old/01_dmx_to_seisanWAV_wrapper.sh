#!/usr/bin/env bash

# Wrapper script for converting all 1991 Pinatubo DMX files to SEISAN WAV MiniSEED

RAW_TOP="/Users/glennthompson/Dropbox/PROFESSIONAL/DATA/Pinatubo/ASSEMBLED-25-028-THOMPSON-PINATUBO1991/LEGACY/WAVEFORM_DATA/SUDS"
FAIR_TOP="/Users/glennthompson/Dropbox/PROFESSIONAL/DATA/Pinatubo/ASSEMBLED-25-028-THOMPSON-PINATUBO1991/FAIR"

SEISAN_TOP="${FAIR_TOP}/SEISAN/WAV"
DB="PNTBO"
NET="XB"
FIX_FS=100.0   # leave as 100 Hz; does NOT resample, only metadata fix

# DMX pattern is YYYYMM/*.DMX
DMX_GLOB="**/*.DMX"

python 01_dmx_to_seisanWAV_v2.py \
    --rawtop "$RAW_TOP" \
    --seisan-top "$SEISAN_TOP" \
    --db "$DB" \
    --net "$NET" \
    --fix-fs "$FIX_FS" \
    --glob "$DMX_GLOB" \
    --verbose