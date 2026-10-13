#!/usr/bin/env python3
"""
41_build_hypocenter_index_pinaall.py

STEP 41 — Pinatubo FAIR pipeline

Parse Randy White's annotated PINAALL hypocenter catalog.

Designed for:
    PINAALL_annotated_RW_20261013.DAT

Features
--------
- Handles variable HYPO71-like date/time spacing.
- Parses degrees/minutes latitude and longitude.
- Handles seconds/minutes rollover.
- Preserves original source records and line numbers.
- Preserves inline and standalone annotations.
- Distinguishes annotations from malformed earthquake records.
- Preserves the original reported time.
- Optionally converts Philippine local time to UTC.
- Flags ambiguous time conventions.
- Preserves additional numeric fields without assuming their meaning.
- Produces CSV indexes and an audit report.
- Does not create QuakeML.

Time conventions
----------------
Randy's catalog states:

    LOCAL time until 91 530 00:00. Then GMT time!

However, additional annotations introduce uncertainty near
the transition.

By default, the parser:

    - treats records before 1991-05-30 as local time
    - treats records after 1991-05-30 as UTC
    - flags records on the transition date as uncertain

Use --local-utc-offset 8 to convert Philippine local time
to UTC (UTC+08:00).

The conversion is intentionally optional.

IMPORTANT:
The second numeric field after longitude is retained in the
'magnitude' column for compatibility with the existing pipeline,
but its interpretation should be independently confirmed.
"""

from __future__ import annotations

import argparse
import csv
import re

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


PARSER_ID = "pinaall_annotated_rw_v1"

DEFAULT_SOURCE = "PINAALL_annotated_RW_20261013.DAT"

# Randy's stated transition date.
TIME_TRANSITION = datetime(1991, 5, 30)

# Latitude/longitude coordinates in degrees and minutes.
#
# Examples:
#   15n 9.34 120e19.56
#   15n26.44 120e22.83
#
COORD_RE = re.compile(
    r"(?P<latdeg>\d{1,2})\s*"
    r"(?P<lathem>[NSns])\s*"
    r"(?P<latmin>\d+(?:\.\d+)?)"
    r"\s+"
    r"(?P<londeg>\d{1,3})\s*"
    r"(?P<lonhem>[EWew])\s*"
    r"(?P<lonmin>\d+(?:\.\d+)?)"
)

EVENT_PREFIX_RE = re.compile(r"^\s*91\s+")

NUMBER_RE = re.compile(
    r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$"
)


# ---------------------------------------------------------------------
# Date and time parsing
# ---------------------------------------------------------------------

def parse_month_day(tokens):
    """
    Parse either:

        510       -> May 10
        0510      -> May 10
        5 10      -> May 10

    Returns
    -------
    month, day, next_index
    """

    token = tokens[1]

    if len(token) in (3, 4) and token.isdigit():

        month = int(token[:-2])
        day = int(token[-2:])

        if not (1 <= month <= 12 and 1 <= day <= 31):
            raise ValueError("Invalid month/day")

        return month, day, 2

    month = int(tokens[1])
    day = int(tokens[2])

    if not (1 <= month <= 12 and 1 <= day <= 31):
        raise ValueError("Invalid month/day")

    return month, day, 3


def parse_time_tokens(tokens):
    """
    Parse the date/time portion preceding latitude.

    Examples
    --------
    91 510  410 52.99
    91 512  2 4 25.20
    91 521 11 7 46.96
    91 818  032 24.05

    Returns
    -------
    datetime
        Naive datetime preserving the reported clock time.
    """

    if len(tokens) < 4:
        raise ValueError("Too few date/time tokens")

    yy = int(tokens[0])

    year = 1900 + yy if yy >= 70 else 2000 + yy

    month, day, i = parse_month_day(tokens)

    remaining = tokens[i:]

    if len(remaining) == 2:

        hhmm = remaining[0]

        if not hhmm.isdigit():
            raise ValueError("Invalid HHMM")

        value = int(hhmm)

        hour = value // 100
        minute = value % 100

        seconds = float(remaining[1])

    elif len(remaining) == 3:

        hour = int(remaining[0])
        minute = int(remaining[1])
        seconds = float(remaining[2])

    else:
        raise ValueError(
            f"Unexpected time fields: {remaining}"
        )

    if not 0 <= hour <= 23:
        raise ValueError(f"Invalid hour: {hour}")

    if not 0 <= minute <= 99:
        raise ValueError(f"Invalid minute: {minute}")

    if not 0 <= seconds < 120:
        raise ValueError(f"Invalid seconds: {seconds}")

    # Timedelta handles minute/second rollover.
    base = datetime(year, month, day)

    return base + timedelta(
        hours=hour,
        minutes=minute,
        seconds=seconds
    )


# ---------------------------------------------------------------------
# Coordinate parsing
# ---------------------------------------------------------------------

def parse_coordinates(match):
    """
    Convert degrees/minutes to decimal degrees.
    """

    latdeg = float(match.group("latdeg"))
    latmin = float(match.group("latmin"))

    londeg = float(match.group("londeg"))
    lonmin = float(match.group("lonmin"))

    if not 0 <= latmin < 60:
        raise ValueError("Latitude minutes out of range")

    if not 0 <= lonmin < 60:
        raise ValueError("Longitude minutes out of range")

    lat = latdeg + latmin / 60.0
    lon = londeg + lonmin / 60.0

    if match.group("lathem").upper() == "S":
        lat = -lat

    if match.group("lonhem").upper() == "W":
        lon = -lon

    if not (-90 <= lat <= 90):
        raise ValueError("Latitude out of range")

    if not (-180 <= lon <= 180):
        raise ValueError("Longitude out of range")

    return lat, lon


# ---------------------------------------------------------------------
# Time convention
# ---------------------------------------------------------------------

def classify_time_basis(dt):
    """
    Classify Pinatubo catalog time conventions.

    Based on Randy White's email of October 2026.

    Returns
    -------
    basis : str
        LOCAL, LOCAL_PLUS_1MIN, UTC, or UNCERTAIN

    confidence : str
        CONFIRMED, INFERRED, or UNKNOWN

    note : str
        Explanation of the classification.
    """

    if dt < datetime(1991, 5, 20):

        return (
            "LOCAL",
            "INFERRED",
            "Pre-May-20 records; local time assumed."
        )

    if dt < datetime(1991, 5, 21, 2):

        return (
            "LOCAL",
            "CONFIRMED",
            "May 20 to May 21 02:00: local time."
        )

    if dt < datetime(1991, 5, 21, 11):

        return (
            "UNCERTAIN",
            "UNKNOWN",
            "May 21 02:00–11:00 transition interval."
        )

    if dt < datetime(1991, 5, 24):

        return (
            "LOCAL_PLUS_1MIN",
            "CONFIRMED",
            "May 21 11:00 through May 23."
        )

    if dt < datetime(1991, 5, 27):

        return (
            "LOCAL",
            "CONFIRMED",
            "May 24–26: local time."
        )

    if dt < datetime(1991, 5, 30):

        return (
            "UNCERTAIN",
            "UNKNOWN",
            "May 27–29: time convention unresolved."
        )

    return (
        "UTC",
        "CONFIRMED",
        "May 30 onward: GMT."
    )


def convert_origin_time(dt, basis, local_utc_offset=8):
    """
    Convert reported catalog time to UTC.

    Philippine local time = UTC + 8 hours.

    LOCAL_PLUS_1MIN means the recorded clock is
    one minute ahead of Philippine local time.
    """

    if basis == "UTC":

        utc_dt = dt

    elif basis == "LOCAL":

        utc_dt = dt - timedelta(
            hours=local_utc_offset
        )

    elif basis == "LOCAL_PLUS_1MIN":

        utc_dt = dt - timedelta(
            hours=local_utc_offset,
            minutes=1
        )

    else:

        return ""

    return (
        utc_dt.replace(tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
# ---------------------------------------------------------------------
# Numeric fields and annotations
# ---------------------------------------------------------------------

def split_numeric_and_annotations(text):
    """
    Extract leading numeric fields and remaining annotation.

    Supports:
        3.37 0.93 D
        4.20 0.38!
        3.17 0.66!merged
        1.66 0.00!p
        1.90 0.00! Pvt
        8.29 1.18 6 0.14

    Returns
    -------
    numbers : list[float]
        Numeric fields preceding the first annotation.

    annotation : str
        Annotation text, including attached punctuation.
    """

    tokens = text.strip().split()

    numbers = []
    annotation_tokens = []

    # Match a numeric prefix, optionally followed by annotation text.
    numeric_prefix = re.compile(
        r"^([+-]?(?:\d+(?:\.\d*)?|\.\d+))(.*)$"
    )

    in_annotation = False

    for token in tokens:

        if in_annotation:
            annotation_tokens.append(token)
            continue

        match = numeric_prefix.match(token)

        if match is None:
            in_annotation = True
            annotation_tokens.append(token)
            continue

        numbers.append(float(match.group(1)))

        suffix = match.group(2)

        if suffix:
            in_annotation = True
            annotation_tokens.append(suffix)

    return numbers, " ".join(annotation_tokens)

def classify_event(annotation):
    """
    Classify events using Randy White's annotation codes.

    Recognized codes
    ----------------
    D       : distal
    DVT     : distal volcano-tectonic
    p       : proximal
    SB      : spasmodic burst
    SBurst  : spasmodic burst

    Returns
    -------
    str
        Normalized event classification, or an empty
        string if no recognized classification is found.
    """

    import re

    tokens = re.findall(r"[A-Za-z]+", annotation)

    # More specific classifications first.
    for token in tokens:
        if token.upper() == "DVT":
            return "distal_vt"

    for token in tokens:
        if token.upper() in ("SB", "SBURST"):
            return "spasmodic_burst"

    for token in tokens:
        if token == "D":
            return "distal"

        if token == "p":
            return "proximal"

    return ""

# ---------------------------------------------------------------------
# Earthquake record parser
# ---------------------------------------------------------------------

def parse_event_line(line):
    """
    Parse a single hypocenter record.

    Returns
    -------
    dict
        Parsed fields.

    Raises
    ------
    ValueError
        When a candidate event cannot be parsed.
    """

    match = COORD_RE.search(line)

    if match is None:
        raise ValueError("No valid coordinate fields")

    datetime_text = line[:match.start()].strip()

    dt = parse_time_tokens(datetime_text.split())

    lat, lon = parse_coordinates(match)

    tail = line[match.end():]

    numbers, annotation = split_numeric_and_annotations(tail)

    if len(numbers) < 2:
        raise ValueError(
            "Expected at least depth and second numeric field"
        )

    depth_km = numbers[0]
    magnitude = numbers[1]

    extra_numbers = numbers[2:]

    return {
        "reported_time": dt,
        "latitude": lat,
        "longitude": lon,
        "depth_km": depth_km,
        "magnitude": magnitude,
        "extra_numeric_fields": extra_numbers,
        "annotation": annotation,
    }


# ---------------------------------------------------------------------
# Catalog processing
# ---------------------------------------------------------------------

def parse_catalog(
    source,
    local_utc_offset=None,
):
    """
    Parse complete annotated catalog.

    Returns
    -------
    events, annotations, errors
    """

    events = []
    annotations = []
    errors = []

    current_context = []

    lines = source.read_text(
        encoding="utf-8",
        errors="replace"
    ).splitlines()

    for lineno, line in enumerate(lines, start=1):

        stripped = line.strip()

        if not stripped:
            continue

        # Standalone comments and annotations.
        if not EVENT_PREFIX_RE.match(line):

            annotations.append({
                "source_line": lineno,
                "annotation_type": "standalone",
                "text": stripped,
                "raw_line": line,
            })

            current_context.append(
                f"L{lineno}: {stripped}"
            )

            continue

        try:

            parsed = parse_event_line(line)

        except ValueError as exc:

            # Date-prefixed narrative annotations, such as
            # "91 615 CATACLYSMIC ERUPTION STARTS",
            # are preserved separately.
            if COORD_RE.search(line) is None:

                annotations.append({
                    "source_line": lineno,
                    "annotation_type": "dated_annotation",
                    "text": stripped,
                    "raw_line": line,
                })

                current_context.append(
                    f"L{lineno}: {stripped}"
                )

            else:

                errors.append({
                    "source_line": lineno,
                    "reason": str(exc),
                    "raw_line": line,
                })

            continue

        dt = parsed["reported_time"]

        basis, confidence, time_note = classify_time_basis(dt)

        origin_time = convert_origin_time(
            dt,
            basis,
            local_utc_offset=8
        )

        extra = parsed["extra_numeric_fields"]
        event_class = classify_event(parsed["annotation"])

        event = {
            "origin_time": origin_time,
            "reported_time": dt.isoformat(),
            "time_basis": basis,
            "time_confidence": confidence,
            "time_note": time_note, 
            "latitude": parsed["latitude"],
            "longitude": parsed["longitude"],
            "depth_km": parsed["depth_km"],
            "magnitude": parsed["magnitude"],
            "event_class": event_class,

            # Preserve optional numeric columns.
            "extra_numeric_1": (
                extra[0] if len(extra) > 0 else ""
            ),
            "extra_numeric_2": (
                extra[1] if len(extra) > 1 else ""
            ),
            "extra_numeric_remaining": (
                " ".join(map(str, extra[2:]))
                if len(extra) > 2 else ""
            ),

            "annotation": parsed["annotation"],

            # Standalone annotations since the preceding event.
            "preceding_annotations": " | ".join(
                current_context
            ),

            "source_file": source.name,
            "source_line": lineno,
            "parser": PARSER_ID,
            "raw_line": line,
        }

        events.append(event)

        if parsed["annotation"]:

            annotations.append({
                "source_line": lineno,
                "annotation_type": "inline",
                "text": parsed["annotation"],
                "raw_line": line,
            })

        # Comments apply to the next successfully parsed event.
        current_context = []

    return events, annotations, errors


# ---------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------

EVENT_COLUMNS = [
    "origin_time",
    "reported_time",
    "time_basis",
    "time_confidence",
    "time_note",
    "latitude",
    "longitude",
    "depth_km",
    "magnitude",
    "extra_numeric_1",
    "extra_numeric_2",
    "extra_numeric_remaining",
    "annotation",
    "event_class",
    "preceding_annotations",
    "source_file",
    "source_line",
    "parser",
    "raw_line",
]

ANNOTATION_COLUMNS = [
    "source_line",
    "annotation_type",
    "text",
    "raw_line",
]

ERROR_COLUMNS = [
    "source_line",
    "reason",
    "raw_line",
]


def write_csv(path, rows, columns):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=columns
        )

        writer.writeheader()

        writer.writerows(rows)


# ---------------------------------------------------------------------
# Audit report
# ---------------------------------------------------------------------

def write_audit(
    path,
    source,
    events,
    annotations,
    errors,
    local_utc_offset,
):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    basis_counts = Counter(
        e["time_basis"] for e in events
    )

    annotation_counts = Counter(
        a["annotation_type"] for a in annotations
    )

    event_class_counts = Counter(
        e["event_class"] or "unclassified"
        for e in events
    )

    reported_times = sorted(
        e["reported_time"] for e in events
    )

    depths = [
        e["depth_km"] for e in events
    ]

    latitudes = [
        e["latitude"] for e in events
    ]

    longitudes = [
        e["longitude"] for e in events
    ]

    lines = [
        "STEP 41 — PINATUBO HYPOCENTER AUDIT",
        "=" * 45,
        "",
        f"Parser: {PARSER_ID}",
        f"Source: {source}",
        "",
        f"Parsed earthquakes: {len(events)}",
        f"Annotations: {len(annotations)}",
        f"Parsing errors: {len(errors)}",
        "",
        "Time basis:",
    ]

    for basis, count in sorted(basis_counts.items()):
        lines.append(f"  {basis}: {count}")

    lines.extend([
        "",
        f"Local UTC offset: {local_utc_offset}",
        "",
        "Annotation types:",
    ])

    for kind, count in sorted(annotation_counts.items()):
        lines.append(f"  {kind}: {count}")

    lines.extend([
        "",
        "Event classifications:",
    ])

    for event_class, count in sorted(event_class_counts.items()):
        lines.append(f"  {event_class}: {count}")

    if events:

        lines.extend([
            "",
            "Catalog ranges:",
            f"  First reported time: {reported_times[0]}",
            f"  Last reported time:  {reported_times[-1]}",
            f"  Latitude:  {min(latitudes):.5f} "
            f"to {max(latitudes):.5f}",
            f"  Longitude: {min(longitudes):.5f} "
            f"to {max(longitudes):.5f}",
            f"  Depth:     {min(depths):.2f} "
            f"to {max(depths):.2f} km",
        ])

    if errors:

        lines.extend([
            "",
            "Parsing errors:",
        ])

        for error in errors:

            lines.append(
                f"  Line {error['source_line']}: "
                f"{error['reason']}"
            )

    path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8"
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():

    ap = argparse.ArgumentParser(
        description=(
            "STEP 41: Parse Randy White's annotated "
            "Pinatubo hypocenter catalog"
        )
    )

    ap.add_argument(
        "--pinaall-file",
        "--summary-file",
        dest="source",
        default=DEFAULT_SOURCE,
        help="Input annotated PINAALL file"
    )

    ap.add_argument(
        "--out-csv",
        default="pinatubo_hypocenters.csv"
    )

    ap.add_argument(
        "--annotations-csv",
        default="pinatubo_annotations.csv"
    )

    ap.add_argument(
        "--error-log",
        default="pinatubo_parse_errors.csv"
    )

    ap.add_argument(
        "--audit-report",
        default="pinatubo_parse_summary.txt"
    )

    ap.add_argument(
        "--local-utc-offset",
        type=float,
        default=None,
        help=(
            "UTC offset for early local-time records. "
            "Use 8 for Philippine local time."
        )
    )

    args = ap.parse_args()

    source = Path(args.source)

    if not source.exists():
        ap.error(f"File not found: {source}")

    events, annotations, errors = parse_catalog(
        source,
        local_utc_offset=args.local_utc_offset,
    )

    write_csv(
        Path(args.out_csv),
        events,
        EVENT_COLUMNS
    )

    write_csv(
        Path(args.annotations_csv),
        annotations,
        ANNOTATION_COLUMNS
    )

    write_csv(
        Path(args.error_log),
        errors,
        ERROR_COLUMNS
    )

    write_audit(
        Path(args.audit_report),
        source,
        events,
        annotations,
        errors,
        args.local_utc_offset,
    )

    print()
    print("STEP 41 — PINATUBO HYPOCENTER INDEX")
    print("----------------------------------")
    print(f"Source:       {source}")
    print(f"Earthquakes:  {len(events)}")
    print(f"Annotations:  {len(annotations)}")
    print(f"Errors:       {len(errors)}")
    print()
    print(f"Catalog:      {args.out_csv}")
    print(f"Annotations:  {args.annotations_csv}")
    print(f"Errors:       {args.error_log}")
    print(f"Audit:        {args.audit_report}")
    print()


if __name__ == "__main__":
    main()