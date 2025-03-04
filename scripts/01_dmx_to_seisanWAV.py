import glob
import os
#import numpy as np
import pandas as pd
#from IPython.display import display, HTML

import sys

def fix_sampling_rate(st, fs=100.0):
    for tr in st:
        tr.stats.sampling_rate=fs          

def remove_IRIG_channel(st):
    for tr in st:
        if tr.stats.station=='IRIG':
            st.remove(tr) # we do not want to keep the IRIG trace

REPO_DIR = '/home/thompsong/Developer/Pinatubo1991SeismicData'
#REA_DIR = '/data/SEISAN_DB/REA/PINAT2'
#WAV_DIR = '/data/SEISAN_DB/WAV/PINAT'
DEV_DIR = '/home/thompsong/Developer'
LIB_DIR = os.path.join(DEV_DIR, 'SoufriereHillsVolcano', 'lib')
sys.path.append(LIB_DIR)
from libseisGT import read_DMX_file, remove_empty_traces, fix_trace_id
from seisan_classes import stream2wavfile

# Main paths for SUDS DMX
TOPDIR = '/data/Pinatubo'
WAVEFORM_DIR = os.path.join(TOPDIR,'WAVEFORMS')

# paths for files to register into Seisan
os.environ['SEISAN_TOP']='/data/SEISAN_DB'
SEISAN_TOP = os.getenv('SEISAN_TOP')
seisanDBname = 'PNTBO'

# Other constants
FDSNnet = 'XB' # assigned by Gale Cox. 1R is code for KSC.

# record trace id mapping
trace_id_mapping = {}

succeeded = 0
failed = 0
list_failed = []

# Loop over all files
list_of_returnDict = []
alldirs = sorted(glob.glob(os.path.join(WAVEFORM_DIR, '1991*')))
for thisdir in alldirs:
    allDMXfiles = sorted(glob.glob(os.path.join(thisdir, '*.DMX')))
    #allDMXfiles = os.path.join(WAVEFORM_DIR, '199105', '9105011P.DMX')
    for dmxfile in allDMXfiles:
        print(f'succeeded={succeeded}, failed={failed}' + '\n')
        print(dmxfile)

        # load dmx file
        st = read_DMX_file(dmxfile, fix=True, defaultnet=FDSNnet)
        print(st)

        # remove blank traces
        remove_empty_traces(st)

        if len(st)==0:
            failed += 1
            list_failed.append(dmxfile)
            continue
        succeeded += 1

        # map/fix trace IDs
        for tr in st:
            id_before = tr.id
            fix_trace_id(tr, legacy=True, netcode=FDSNnet)
            id_after = tr.id
            if id_before not in trace_id_mapping:
                trace_id_mapping[id_before] = id_after

        # Establishing the Seisan WAV filename corresponding to this SUDS DMX event filename
        WAVfilename = stream2wavfile(st, SEISAN_TOP, seisanDBname)
        #st.write(os.path.join(SEISAN_TOP,'WAV', seisanDBname, os.path.basename(WAVfilename)),format='MSEED')
        os.makedirs(os.path.dirname(WAVfilename), exist_ok=True)
        st.write(WAVfilename, format='MSEED')
        print(f'- {WAVfilename}')

        # Convert mapping to a DataFrame
        df_trace_id_mapping = pd.DataFrame(list(trace_id_mapping.items()), columns=['Old Trace ID', 'New Trace ID'])
        print(df_trace_id_mapping)

# Save table for journal appendix
df_trace_id_mapping.to_csv(os.path.join(REPO_DIR, 'metadata', "trace_id_mapping.csv"), index=False)  # Save as CSV
df_trace_id_mapping.to_latex(os.path.join(REPO_DIR, 'metadata', "trace_id_mapping.tex"), index=False)  # Save as LaTeX for journal articles

# files not converted
file_of_bad_dmx_files = os.path.join(REPO_DIR, 'metadata', 'baddmxfiles.txt')
# Write each element as a new line
with open(file_of_bad_dmx_files, "w") as file:
    for item in list_failed:
        file.write(f"{item}\n")

print(f"List saved to {file_of_bad_dmx_files}")