
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import re

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
for name, path in file_paths.items():
    counts = load_and_parse(path, parsers[name])
    df = pd.DataFrame({name: counts})
    df_all = pd.concat([df_all, df], axis=1)

df_all = df_all.fillna(0)

# === Plot ===
colors = ["#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#9467bd"]
df_all.sort_index().plot(
    kind="bar",
    stacked=False,
    width=1.0,
    figsize=(18, 7),
    color=colors
)
plt.title("Number of Events per Day from Five Seismic Catalogs")
plt.xlabel("Date")
plt.ylabel("Number of Events")
plt.tight_layout()
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.legend(title="Source", loc="upper right")
plt.show()
