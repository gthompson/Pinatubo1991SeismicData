
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import re
import os
from collections import defaultdict
from obspy import read

# === HYPO71-style datetime parser ===
def parse_hypo71_datetime_only_strict(line):
    try:
        line = line.ljust(17)
        year = int(line[0:2])
        month = int(line[2:4])
        day = int(line[4:6])
        hour = int(line[7:9])
        minute = int(line[9:11])
        seconds = float(line[12:17])
        year += 1900 if year >= 70 else 2000
        return datetime(year, month, day, hour, minute) + pd.to_timedelta(seconds, unit="s")
    except:
        return None

# === PINAALL.DAT parser ===
def parse_hypo71_datetime_only(line):
    try:
        year = int(line[0:2])
        month = int(line[2:4])
        day = int(line[4:6])
        hour = int(line[7:9]) if line[7:9].strip() else 0
        minute = int(line[9:11]) if line[9:11].strip() else 0
        seconds = float(line[12:17]) if line[12:17].strip() else 0
        year += 1900 if year >= 70 else 2000
        return datetime(year, month, day, hour, minute) + pd.to_timedelta(seconds, unit="s")
    except:
        return None

# === b-run.sum and TESTALL.SUM parser ===
def parse_yyyymmddhhmmss(line):
    try:
        ts = line[:14]
        return datetime.strptime(ts, "%Y%m%d%H%M%S")
    except:
        return None

# === File loading and datetime parsing ===
def load_and_parse(file_path, parser):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [line.strip() for line in f if line.strip()]
    datetimes = [parser(line) for line in lines]
    datetimes = [dt for dt in datetimes if dt is not None]
    df = pd.DataFrame({"datetime": pd.to_datetime(datetimes)})
    df["date"] = df["datetime"].dt.date
    return df["date"].value_counts().sort_index()




'''
def parse_dmx_filenames(directory):
    pattern = re.compile(r'^(\d{2})(\d{2})(\d{2})([0-9A-Fa-f]{2})\.DMX$')
    matched_files = []

    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            yy, mm, dd, hex_index = match.groups()
            year = 1900 + int(yy)
            date_key = f"{year:04d}-{mm}-{dd}"
            index_value = int(hex_index, 16)
            matched_files.append((date_key, index_value, filename))

    # Sort matched files by date and then by hex index
    matched_files.sort(key=lambda x: (x[0], x[1]))

    # Now group them by date
    files_per_day = defaultdict(list)
    for date_key, index_value, _ in matched_files:
        files_per_day[date_key].append(index_value)

    summary = {}
    for date, indices in files_per_day.items():
        summary[date] = {
            "file_count": len(indices),
            "max_index": max(indices)
        }

    return summary

def process_all_91_dirs(parent_dir): # process the from_JP_waveforms dir
    combined_summary = defaultdict(lambda: {'file_count': 0, 'max_index': -1})
    
    for entry in os.listdir(parent_dir):
        dir_path = os.path.join(parent_dir, entry)
        if os.path.isdir(dir_path) and entry.endswith("91"):
            print(f"Processing {dir_path}")
            daily_summary = parse_dmx_filenames(dir_path)
            for date, info in daily_summary.items():
                combined_summary[date]['file_count'] += info['file_count']
                combined_summary[date]['max_index'] = max(
                    combined_summary[date]['max_index'], info['max_index']
                )

    return combined_summary
'''

def get_dmx_daily_counts_and_max_indices(directory, base=36): # use base=16 for Montserrat, 36 for Pinatubo
    pattern = re.compile(r'^(\d{2})(\d{2})(\d{2})([0-9A-Za-z]{2})\.DMX$', re.IGNORECASE)
    daily_data = defaultdict(list)
    
    for root, _, files in os.walk(directory):
        print(f'Examining {root}')
        files = sorted(files)
        print(f'- found {len(files)} files')
        os.system(f'ls {root} | wc -l')
        for filename in sorted(files):
            match = pattern.match(filename)
            if match:
                #print(f'{filename} Matched')
                yy, mm, dd, hex_index = match.groups()
                year = 1900 + int(yy)
                date = pd.Timestamp(f"{year:04d}-{mm}-{dd}")
                index_value = int(hex_index.upper(), base)
                print(filename, yy, mm, dd, hex_index, index_value)
                daily_data[date].append(index_value)
            else:
                print(f'{filename} failed to match')

    # Convert to DataFrame
    df = pd.DataFrame.from_dict({
        date: {
            "files": len(indices),
            "counter": max(indices)+1
        }
        for date, indices in daily_data.items()
    }, orient="index").sort_index()

    return df


def plot_daily_event_counts(df_all, colors=None, tick_interval=3, output_file="hypo71_bar_chart.png"):
    """
    Plots a bar chart of daily event counts from multiple seismic catalogs.

    Parameters:
    - df_all: DataFrame with datetime index and one column per catalog
    - colors: List of colors for the bars (default: 6-category palette)
    - tick_interval: Interval for x-axis date labels to avoid overlap
    - output_file: Filename to save the figure
    """
    if colors is None:
        colors = ["#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#9467bd", "black"]

    # Sort by date
    df_sorted = df_all.sort_index()

    # Plot as bar chart
    ax = df_sorted.plot(
        kind="bar",
        stacked=False,
        width=1.0,
        figsize=(18, 7),
        color=colors
    )

    # Generate spaced tick labels
    tick_positions = list(range(0, len(df_sorted), tick_interval))
    tick_labels = [d.strftime('%Y-%m-%d') for i, d in enumerate(df_sorted.index) if i % tick_interval == 0]

    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=45, ha='right')

    # Titles and labels
    plt.title("Number of Events per Day from Seismic Catalogs")
    plt.xlabel("Date")
    plt.ylabel("Number of Events")
    plt.tight_layout()
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.legend(title="Source", loc="upper right")

    # Save to file
    plt.savefig(output_file)
    print(f"✅ Saved bar chart to {output_file}")
    plt.close()

def plot_dmx_stairs_with_cumulative(df_dmx):

    # Sort just in case
    df_sorted = df_dmx.sort_index()

    # Calculate cumulative sums
    df_sorted["Cumulative files"] = df_sorted["files"].cumsum()
    df_sorted["Cumulative counter"] = df_sorted["counter"].cumsum()

    # Create figure and left axis (primary)
    fig, ax1 = plt.subplots(figsize=(14, 6))

    ax1.step(df_sorted.index, df_sorted["files"], where="mid", label="DMX files", linewidth=2)
    ax1.step(df_sorted.index, df_sorted["counter"], where="mid", label="counter", linewidth=2)    

    ax1.set_xlabel("Date")
    ax1.set_ylabel("Number")
    #ax1.set_title("DMX Files per Day: Count, Max Index, and Cumulative Totals")
    ax1.grid(True, linestyle='--', alpha=0.5)

    # Create right axis (secondary)
    ax2 = ax1.twinx()
    ax2.plot(df_sorted.index, df_sorted["Cumulative files"], 'k--', label="Cumulative DMX files", linewidth=1.5)
    ax2.plot(df_sorted.index, df_sorted["Cumulative counter"], 'gray', linestyle='--', label="Cumulative counter", linewidth=1.5)
    ax2.set_ylabel("Cumulative Count")

    # Combine legends from both axes
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    plt.tight_layout()
    plt.savefig('DMX_stairs.png')


def plot_dmx_gaps_overlaps(df):
    df_sorted = df.sort_values("Start Time").reset_index(drop=True)

    # Compute time difference between each file's start and previous file's end
    df_sorted["Previous End Time"] = df_sorted["End Time"].shift(1)
    df_sorted["Gap (seconds)"] = (df_sorted["Start Time"] - df_sorted["Previous End Time"]).dt.total_seconds()

    # Plot
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.axhline(0, color='black', linewidth=1, linestyle='--', label='No Gap')

    ax.plot(df_sorted["File Index"], df_sorted["Gap (seconds)"], marker='o', linestyle='-', label="Gap / Overlap")

    ax.set_title("Time Gaps and Overlaps Between DMX Files")
    ax.set_xlabel("File Index")
    ax.set_ylabel("Gap in Seconds (positive = gap, negative = overlap)")
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.legend()
    plt.tight_layout()
    plt.savefig('gaps_overlaps.png')

    return df_sorted  # with gap column for inspection    

############ MAKE HYPO71 bar chart ##############

# === File paths (update these for local use) ===
file_paths = {
    "GOLD.SUM": "GOLD.SUM",
    "moriall.sum": "moriall.sum",
    "b-run.sum": "b-run.sum",
    "TESTALL.SUM": "TESTALL.SUM",
    "PINAALL.DAT": "PINAALL.DAT"
}

parsers = {
    "GOLD.SUM": parse_hypo71_datetime_only_strict,
    "moriall.sum": parse_hypo71_datetime_only_strict,
    "b-run.sum": parse_yyyymmddhhmmss,
    "TESTALL.SUM": parse_yyyymmddhhmmss,
    "PINAALL.DAT": parse_hypo71_datetime_only
}



# === Parse all and build daily count DataFrame ===
df_all = pd.DataFrame()

dropboxdir = os.path.join(os.path.expanduser('~'), 'Dropbox', 'Pinatubo')
catalogsdir = os.path.join(dropboxdir, 'hypo71')
for name, path in file_paths.items():
    fullpath = os.path.join(catalogsdir, path)
    counts = load_and_parse(fullpath, parsers[name])
    df = pd.DataFrame({name: counts})
    df_all = pd.concat([df_all, df], axis=1)

# DMX files per day
DMXdir = os.path.join(dropboxdir, 'from_JP', 'Waveforms')
# DMX files per day (using new function)
df_dmx = get_dmx_daily_counts_and_max_indices(DMXdir, base=36).sort_index()  # Use base=36 for Pinatubo
#df_all = pd.concat([df_all, df_dmx], axis=1).fillna(0)

df_all = df_all.fillna(0)
df_all.index = pd.to_datetime(df_all.index)
#print(df_all)
#cutoff_date = pd.Timestamp("1991-08-19")
df_all = df_all[df_all.index <= cutoff_date]

plot_daily_event_counts(df_all, tick_interval=3, output_file="pinatubo_catalogs.png")   

########### MAKE DMX BAR CHART

df_dmx = get_dmx_daily_counts_and_max_indices(DMXdir)
plot_dmx_stairs_with_cumulative(df_dmx)

# === Summary ===
total_actual = df_dmx["files"].sum()
total_expected = df_dmx["counter"].sum()

print(f"📦 Total DMX files found: {total_actual}")
print(f"📏 Estimated total based on Max NN + 1 per day: {total_expected}")
print(f"📉 Files missing: {total_expected - total_actual}")



def analyze_dmx_file_metadata(directory):
    pattern = re.compile(r'^(\d{2})(\d{2})(\d{2})([0-9A-Z]{2})\.DMX$', re.IGNORECASE)
    dmx_files = []

    # Gather and sort matching DMX files across subdirectories
    for root, _, files in os.walk(directory):
        files = sorted(files)
        for f in files:
            if pattern.match(f):
                fullpath = os.path.join(root, f)
                dmx_files.append(fullpath)

    dmx_files.sort()  # Sort filenames (will sort chronologically due to naming)

    previous_start = None
    summaries = []

    for file_index, filepath in enumerate(dmx_files):
        filename = os.path.basename(filepath)
        match = pattern.match(filename)
        if not match:
            continue

        _, _, _, nn = match.groups()
        try:
            sequence_number = int(nn.upper(), 36)
        except ValueError:
            continue

        try:
            st = read(filepath)
        except Exception as e:
            print(f"⚠️ Could not read {filename}: {e}")
            continue

        # Clean the stream
        st = st.select()  # remove None-type
        st = st.copy().trim()  # trim just in case
        st = st.__class__([tr for tr in st if tr.data is not None and tr.stats.npts > 0 and not (tr.data == tr.data[0]).all()])

        if len(st) == 0:
            continue  # skip empty or flat traces

        starttime = min(tr.stats.starttime for tr in st)
        endtime = max(tr.stats.endtime for tr in st)
        clock_issue = previous_start is not None and starttime < previous_start
        previous_start = starttime

        station_list = sorted(set(tr.stats.station for tr in st))

        summaries.append({
            "File Index": file_index,
            "Filename": filename,
            "Sequence Number": sequence_number,
            "Start Time": starttime.datetime,
            "End Time": endtime.datetime,
            "Clock Issue": clock_issue,
            "Stations": ",".join(station_list),
            "Num Traces": len(st)
        })

    df = pd.DataFrame(summaries)
    return df

output_csv = os.path.join(dropboxdir, 'dmx_metadata.csv') 
if not os.path.isfile(output_csv):
    raise IOError('it definitely exists')
    dmx_metadata = analyze_dmx_file_metadata(DMXdir)
    dmx_metadata.to_csv(output_csv, index=False)
    print(f"✅ Saved DMX metadata summary to {output_csv}")
else:
    dmx_metadata = pd.read_csv(output_csv)

import matplotlib.pyplot as plt
import seaborn as sns

def build_station_presence_matrix(metadata_csv, output_csv="station_presence_matrix.csv", heatmap_file="station_heatmap.png", stairs_file="station_stairs.png"):
    # Load the metadata
    df = pd.read_csv(metadata_csv, parse_dates=["Start Time", "End Time"])
    df["Date"] = df["Start Time"].dt.date
    
    # Flatten into (date, station) records
    records = []
    for _, row in df.iterrows():
        date = row["Date"]
        stations = row["Stations"].split(",") if pd.notna(row["Stations"]) else []
        for station in stations:
            station = station.strip()
            if station:
                records.append((date, station))

    df_presence = pd.DataFrame(records, columns=["Date", "Station"])
    df_presence["Present"] = True
  

    # Pivot to presence matrix
    matrix = df_presence.pivot_table(index="Date", columns="Station", values="Present", fill_value=False)
    if "IRIG" in matrix.columns:
        matrix = matrix.drop(columns="IRIG")

    # Add daily total
    matrix["Station Count"] = matrix.sum(axis=1)    

    # Ensure all dates are present
    full_date_range = pd.date_range(start=matrix.index.min(), end=matrix.index.max(), freq='D')
    matrix = matrix.reindex(full_date_range, fill_value=False)

    # Convert index to date only (to avoid 00:00:00 labels)
    matrix.index = matrix.index.date
    matrix.index.name = "Date"

    # Recompute station count
    station_columns = matrix.columns.difference(["Station Count"])
    matrix["Station Count"] = matrix[station_columns].sum(axis=1)

    # print missing dates
    original_dates = set(df_presence["Date"])
    all_dates = set(full_date_range.date)
    missing_dates = sorted(all_dates - original_dates)

    print(f"Missing days with no station data: {len(missing_dates)}")
    print(missing_dates)  # print first 10 as example  

    # Save to CSV
    matrix.to_csv(output_csv)
    print(f"✅ Saved station presence matrix to {output_csv}")

    # === Plot 1: Heatmap ===
    plt.figure(figsize=(18, 8))
    heatmap_data = matrix.drop(columns="Station Count").T.astype(int)
    sns.heatmap(heatmap_data, cmap="gray_r", cbar=False, linewidths=0.5, linecolor='lightgray')
    plt.title("Station Presence Heatmap (Black = Present)")
    plt.xlabel("Date")
    plt.ylabel("Station")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(heatmap_file)
    plt.close()
    print(f"🗺️ Saved heatmap to {heatmap_file}")

    # === Plot 2: Stairs plot of daily station count ===
    matrix_sorted = matrix.sort_index()
    plt.figure(figsize=(14, 5))
    plt.step(matrix_sorted.index, matrix_sorted["Station Count"], where='mid', linewidth=2)
    plt.title("Number of Stations with DMX Data per Day")
    plt.xlabel("Date")
    plt.ylabel("Station Count")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(stairs_file)
    plt.close()
    print(f"📈 Saved station count plot to {stairs_file}")

    return matrix

station_matrix = build_station_presence_matrix(output_csv)
