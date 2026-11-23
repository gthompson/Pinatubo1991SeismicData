"""Readers for the later Pinatubo hypocentre solutions (step 40).

* GOLD.SUM  - HYPOELLIPSE summary records, pre-Y2K layout (2-digit year), processed 1992-93.
* b-run.sum - HYPOELLIPSE summary records, post-Y2K layout (4-digit year), processed 2001 ("BRUN").
* PinatuboRelocs.rtf - the 153 high-precision locations of 30 May-3 June 1991 published by
  Jones, Thurber and Lutter (2001), Physics of the Earth and Planetary Interiors 123, 221-232 ("PEPI").
  Josh Jones computed them; Cliff Thurber (University of Wisconsin) found the final files
  ("final.hypos.off", off-summit, then "final.hypos.summ", summit) and pasted them into an email,
  without the location uncertainties shown in the paper.

Summary-record columns follow Lahr (1999), USGS OFR 99-23, section 2.4.1. The pre-Y2K layout
is the post-Y2K one shifted two columns left. The record marker "/" is at 0-based index 80
(pre-Y2K) or 82 (post-Y2K); records without a location (blank latitude) are rejected with
reason "no_location". Seconds are written without a decimal point (F4.2), latitude and
longitude minutes likewise (F4.2), depth as F5.2 and magnitude as F2.1.

Times are returned as written (raw); lib.clock.apply_relocation_clock converts them to UTC.
Every input line is either returned or listed in the rejects with a reason.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

COLUMNS = ["origin_time", "latitude", "longitude", "depth_km", "magnitude", "reported_magnitude",
           "rms", "nass", "source_file", "source_line", "processing_date", "note", "raw_text"]

# Field slices of the post-Y2K summary record (0-based, end-exclusive); pre-Y2K is shifted by -2.
_POST_Y2K = dict(hr=(8, 10), mn=(10, 12), sec=(12, 16), lat_d=(16, 18), ns=(18, 19), lat_m=(19, 23),
                 lon_d=(23, 26), ew=(26, 27), lon_m=(27, 31), depth=(31, 36), mag=(36, 38))
_MARKER = {True: 82, False: 80}
_PROC = {True: (87, 89, 89, 91), False: (85, 87, 87, 89)}  # processing month, then 2-digit year


def _field(line, span, shift):
    return line[span[0] - shift:span[1] - shift]


def _number(text, scale=1.0):
    text = text.strip()
    return float(text) / scale if text else None


def _date(y, mo, d, hr, mn, sec):
    """Raw clock time; seconds or minutes past 59 carry over (as HYPO71/HYPOELLIPSE print them)."""
    return datetime(y, mo, d) + timedelta(hours=hr, minutes=mn, seconds=sec)


def parse_hypoellipse(path, *, post_y2k: bool):
    """Return (origins, rejects) DataFrames for a HYPOELLIPSE summary file."""
    path = Path(path)
    shift = 0 if post_y2k else 2
    rows, rejects = [], []
    for lineno, raw in enumerate(path.read_text(errors="replace").splitlines(), 1):
        line = raw.rstrip("\r\n")
        reject = lambda reason: rejects.append(dict(source_file=path.name, source_line=lineno, reason=reason, raw_text=line))
        if not line.strip():
            reject("blank")
            continue
        if len(line) <= _MARKER[post_y2k] or line[_MARKER[post_y2k]] != "/":
            reject("not_a_summary_record")
            continue
        f = {k: _field(line, s, shift) for k, s in _POST_Y2K.items()}
        if not f["lat_d"].strip() or not f["lon_d"].strip():
            reject("no_location")
            continue
        try:
            year = int(line[0:4]) if post_y2k else 1900 + int(line[0:2])
            mo, dy = (int(line[4:6]), int(line[6:8])) if post_y2k else (int(line[2:4]), int(line[4:6]))
            t = _date(year, mo, dy, int(f["hr"] or 0), int(f["mn"] or 0), _number(f["sec"], 100) or 0.0)
            lat = int(f["lat_d"]) + _number(f["lat_m"], 100) / 60.0
            lon = int(f["lon_d"]) + _number(f["lon_m"], 100) / 60.0
        except (ValueError, TypeError) as exc:
            reject(f"unparseable: {exc}")
            continue
        lat = -lat if f["ns"].upper() == "S" else lat
        lon = -lon if f["ew"].upper() == "W" else lon
        mag = _number(f["mag"], 10) if f["mag"].strip().isdigit() else None
        m0, m1, y0, y1 = _PROC[post_y2k]
        pm, py = line[m0:m1].strip(), line[y0:y1].strip()
        rows.append(dict(origin_time=t.isoformat(), latitude=round(lat, 6), longitude=round(lon, 6),
                         depth_km=_number(f["depth"], 100), magnitude=mag, reported_magnitude=mag,
                         rms=None, nass=None, source_file=path.name, source_line=lineno,
                         processing_date=(f"{int(py) + (1900 if int(py) >= 50 else 2000)}-{int(pm):02d}"
                                         if pm.isdigit() and py.isdigit() else None),
                         note=None, raw_text=line))
    return pd.DataFrame(rows, columns=COLUMNS), pd.DataFrame(rejects, columns=["source_file", "source_line", "reason", "raw_text"])


_PEPI = re.compile(r"^(\d{2})(\d{2})(\d{2}) +(\d{1,2})(\d{2}) +([\d.]+) +([\d.]+)([NS]) +([\d.]+)([EW]) +"
                   r"(-?[\d.]+) +([\d.]+) +(\d+) +([\d.]+) +(\d+)$")


def parse_pepi(path):
    """Return (origins, rejects) for PinatuboRelocs.rtf.

    Longitudes are written "W" but are east (Pinatubo is at 120.35 E); they are returned positive
    and the note says so. A magnitude of 0.00 is returned as written; step 43 treats zeros from
    every source as missing. The event number (e.g. 91530035) is kept in "note".
    """
    path = Path(path)
    rows, rejects, part = [], [], None
    for lineno, raw in enumerate(path.read_text(errors="replace").splitlines(), 1):
        line = raw.strip().rstrip("\\").strip()
        if "final.hypos." in line:
            part = line.rsplit("final.hypos.", 1)[1].strip()  # "off" (off-summit) or "summ" (summit)
            continue
        m = _PEPI.match(line)
        if not m:
            if re.match(r"^\d{6} ", line):
                rejects.append(dict(source_file=path.name, source_line=lineno, reason="unparseable", raw_text=raw))
            continue
        yy, mo, dy, hr, mn, sec, lat, ns, lon, ew, dep, mag, nobs, rms, evno = m.groups()
        t = _date(1900 + int(yy), int(mo), int(dy), int(hr), int(mn), float(sec))
        mag = float(mag)
        rows.append(dict(origin_time=t.isoformat(), latitude=float(lat) * (-1 if ns == "S" else 1),
                         longitude=float(lon), depth_km=float(dep), magnitude=mag,
                         reported_magnitude=mag, rms=float(rms), nass=int(nobs), source_file=path.name,
                         source_line=lineno, processing_date=None,
                         note=f"pepi_event:{evno}; set:{part}; longitude written {ew}, east assumed", raw_text=raw))
    return pd.DataFrame(rows, columns=COLUMNS), pd.DataFrame(rejects, columns=["source_file", "source_line", "reason", "raw_text"])
