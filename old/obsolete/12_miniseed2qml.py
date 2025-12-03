import os
import re, glob
from obspy import read, UTCDateTime
from obspy.io.nordic.core import blanksfile #, _write_nordic
import shutil

def process_wav_directory(wav_base, rea_base):
    """Loops over MiniSEED files in a Seisan WAV database and creates corresponding S-files in Nordic format."""

    year_pattern = re.compile(r"^(199[0-9]|20[0-2][0-9])$")
    month_pattern = re.compile(r"^([0-1][0-9])$")

    # List only directories that match the year pattern
    year_dirs = [entry.name for entry in os.scandir(wav_base) if entry.is_dir() and year_pattern.match(entry.name)]

    for year in sorted(year_dirs):
        year_path = os.path.join(wav_base, year)
        if not os.path.isdir(year_path):
            continue
        print(year_path)
        
        month_dirs = [entry.name for entry in os.scandir(year_path) if entry.is_dir() and month_pattern.match(entry.name)]
        for month in sorted(month_dirs):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue
            print(month_path)
            
            # Define corresponding REA directory
            rea_dir = os.path.join(rea_base, year, month)
            os.makedirs(rea_dir, exist_ok=True)
            
            # Process each MiniSEED file
            for mseed_file in glob.glob(os.path.join(month_path, f"{year}*")):
                process_miniseed(mseed_file, rea_dir)

def process_miniseed(mseed_file, rea_dir):
    """Processes a MiniSEED file and creates a corresponding S-file in Nordic format."""
    try:
        st = read(mseed_file)
        start_time = st[0].stats.starttime
        wavfile = os.path.join(os.path.basename(mseed_file), 'dummy')
        sfilename = blanksfile(wavfile, 'L', 'gt  ', evtime=start_time, nordic_format='OLD')

        sfile_path = os.path.join(rea_dir, sfilename)      
        shutil.move(sfilename, sfile_path)

        print(f"Created S-file: {sfile_path}")
        
    except Exception as e:
        print(f"Error processing {mseed_file}: {e}")

if __name__ == "__main__":
    SEISAN_TOP = '/data/SEISAN_DB'
    DB = 'PINAT'
    wav_base = os.path.join(SEISAN_TOP, "WAV", DB)  # Path to the WAV database
    rea_base = os.path.join(SEISAN_TOP, "REA", 'PINAT')  # Path to the REA database
    process_wav_directory(wav_base, rea_base)