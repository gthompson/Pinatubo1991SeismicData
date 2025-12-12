#!/usr/bin/env python3
"""
pha_parser.py

Shared parsing utilities for PHA files (monthly and individual formats).

Functions:
- parse_phase_line(line) -> list of pick dicts or None
- parse_pha_file(path, errors) -> list of events (origin_time + picks)
- parse_individual_pha_file(path) -> flat list of picks

The parser first attempts fixed-width parsing (legacy PHIVOLCS monthly format)
and falls back to a whitespace-token format used by some individual files.
"""
from datetime import datetime
from obspy import UTCDateTime


def parse_phase_line(line):
    """Parse a single PHA line (fixed-width or whitespace token).

    Returns a list of pick dicts (possibly 1 or 2 entries for P and S),
    or None if the line does not contain a pick.
    """
    if not line:
        return None
    s = line.rstrip("\n\r")

    # First attempt: fixed-width / monthly-style parsing
    try:
        station = s[0:3].strip()
        if station and station.lower() != 'xxx' and len(station) >= 2:
            orientation = s[3:4].strip() if len(s) > 3 else ""
            p_arrival_code = s[4:8].replace(' ', '?') if len(s) > 4 else ""
            timestamp_str = None
            if len(s) > 8:
                if s[8] == ' ':
                    timestamp_str = s[9:24].strip().replace(' ', '0') if len(s) >= 24 else None
                else:
                    timestamp_str = s[8:23].strip().replace(' ', '0') if len(s) >= 23 else None

            if timestamp_str:
                # detect S marker in columns 35-40
                s_positions = [i for i, c in enumerate(s) if c == 'S']
                s_positions = [pos for pos in s_positions if 35 <= pos <= 40]
                s_pos = s_positions[0] if len(s_positions) == 1 else 0

                s_wave_delay = ""
                if s_pos > 0 and len(s) > s_pos - 7:
                    s_wave_delay = s[s_pos-7:s_pos-1].strip()

                s_arrival_code = ""
                if s_pos > 0:
                    if len(s) > s_pos + 3:
                        s_arrival_code = s[s_pos-1:s_pos+3].replace(' ', '?')
                    else:
                        s_arrival_code = s[s_pos-1:].ljust(4).replace(' ', '?')

                has_p_wave = len(p_arrival_code) >= 2 and p_arrival_code[1] == 'P'
                has_s_wave = s_pos > 0

                # normalize timestamp
                add_secs = 0
                if timestamp_str.endswith('60.00'):
                    timestamp_str = timestamp_str.replace('60.00', '00.00')
                    add_secs = 60
                if timestamp_str[-7:-5] == '60':
                    timestamp_str = timestamp_str.replace('60', '00', 1)
                    add_secs += 3600

                try:
                    if len(timestamp_str) > 12 and '.' in timestamp_str:
                        dt = datetime.strptime(timestamp_str, "%y%m%d%H%M%S.%f")
                    else:
                        dt = datetime.strptime(timestamp_str[:12], "%y%m%d%H%M%S")
                    timestamp = UTCDateTime(dt) + add_secs
                except Exception:
                    timestamp = None

                if timestamp:
                    if orientation in "ZNE":
                        channel = f"EH{orientation}"
                    elif orientation == "L":
                        channel = "ELZ"
                    else:
                        channel = f"??{orientation}" if orientation else "EHZ"

                    results = []
                    seed_id = f"XB.{station}..{channel}"

                    if has_p_wave:
                        p_clean = p_arrival_code.replace('?', ' ')
                        results.append({
                            'station': station,
                            'channel': channel,
                            'seed_id': seed_id,
                            'phase': 'P',
                            'time': timestamp,
                            'onset': p_clean[0] if len(p_clean) > 0 and p_clean[0] in ['I', 'E'] else None,
                            'first_motion': p_clean[2] if len(p_clean) > 2 and p_clean[2] in ['U', 'D'] else None,
                            'weight': int(p_clean[3]) if len(p_clean) > 3 and p_clean[3].isdigit() else None,
                        })

                    if has_s_wave and s_wave_delay and s_wave_delay.replace('.', '').replace('-', '').isdigit():
                        s_clean = s_arrival_code.replace('?', ' ')
                        s_time = timestamp + float(s_wave_delay)
                        results.append({
                            'station': station,
                            'channel': channel,
                            'seed_id': seed_id,
                            'phase': 'S',
                            'time': s_time,
                            'onset': s_clean[0] if len(s_clean) > 0 and s_clean[0] in ['I', 'E'] else None,
                            'first_motion': s_clean[2] if len(s_clean) > 2 and s_clean[2] in ['U', 'D'] else None,
                            'weight': int(s_clean[3]) if len(s_clean) > 3 and s_clean[3].isdigit() else None,
                        })

                    return results if results else None
    except Exception:
        # fall through to tokenized parsing
        pass

    # Fallback: whitespace tokenized format (individual PHA files)
    toks = s.split()
    if len(toks) >= 4:
        station = toks[0].strip()
        if station.lower() == 'xxxx' or len(station) < 2:
            return None
        phase = toks[1].strip().upper()
        try:
            weight = int(toks[2])
        except Exception:
            weight = None
        timestamp_str = toks[3]
        try:
            # expect YYMMDDHHMMSS(.ff)
            if '.' in timestamp_str:
                base, frac = timestamp_str.split('.', 1)
                dt = datetime.strptime(base, "%y%m%d%H%M%S")
                frac_val = float('0.' + frac)
                pick_time = UTCDateTime(dt) + frac_val
            else:
                dt = datetime.strptime(timestamp_str[:12], "%y%m%d%H%M%S")
                pick_time = UTCDateTime(dt)
        except Exception:
            return None

        base_station = station[:3] if len(station) >= 3 else station
        channel = 'EHZ'
        seed_id = f"XB.{base_station}..{channel}"

        return [{
            'station': base_station,
            'channel': channel,
            'seed_id': seed_id,
            'phase': phase,
            'time': pick_time,
            'onset': None,
            'first_motion': None,
            'weight': weight,
        }]

    return None


def parse_pha_file(path, errors=None):
    """Parse monthly PHA file into list of events (origin_time + picks).

    Events are separated by lines with "10" (or "100").
    """
    events = []
    current_picks = []
    try:
        with open(path, 'r', errors='ignore') as fh:
            for lineno, raw in enumerate(fh, 1):
                line = raw.rstrip('\n\r')
                if not line.strip():
                    continue
                if line.strip() in ('10', '100'):
                    if current_picks:
                        origin_time = min((p['time'] for p in current_picks if p['phase'] == 'P'), default=None)
                        if not origin_time:
                            origin_time = min((p['time'] for p in current_picks), default=None)
                        if origin_time:
                            events.append({'origin_time': origin_time, 'picks': current_picks})
                        current_picks = []
                    continue

                parsed = parse_phase_line(line)
                if parsed:
                    if isinstance(parsed, list):
                        current_picks.extend(parsed)
                    else:
                        current_picks.append(parsed)
                else:
                    if errors is not None:
                        errors.append(f"{Path(path).name}:{lineno}: {line}")
        # final group
        if current_picks:
            origin_time = min((p['time'] for p in current_picks if p['phase'] == 'P'), default=None)
            if not origin_time:
                origin_time = min((p['time'] for p in current_picks), default=None)
            if origin_time:
                events.append({'origin_time': origin_time, 'picks': current_picks})
    except Exception as e:
        if errors is not None:
            errors.append(f"Error reading {path}: {e}")
    return events


def parse_individual_pha_file(path):
    """Parse an individual-event PHA file and return a flat list of picks."""
    picks = []
    try:
        with open(path, 'r', errors='ignore') as fh:
            for raw in fh:
                parsed = parse_phase_line(raw.rstrip('\n\r'))
                if parsed:
                    if isinstance(parsed, list):
                        picks.extend(parsed)
                    else:
                        picks.append(parsed)
    except Exception:
        pass
    return picks
