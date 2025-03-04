import os
import glob
import re
from datetime import datetime, timedelta
from obspy import UTCDateTime
from obspy.core.event import Event, Origin, Arrival, Pick, WaveformStreamID, Catalog

def parse_monthly_phasefile(file_path, catalog, error_log="lines_that_could_not_be_parsed.txt"):
    """
    Parses seismic phase data using **fixed character positions**.
    Creates an ObsPy Event object for each set of picks, separated by a line with just '10'.
    **Fixed-width character slicing ensures reliable parsing**
    """
    current_event = []  # Temporary storage for arrivals in the current event
    print(catalog)
    
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file, open(error_log, 'a', encoding='utf-8') as error_file:
        for line in file:

            line = line.rstrip()  # Strip \n and spaces

            # DEBUG: Print each line to confirm correct reading
            #print(f"DEBUG: Raw line -> '{line}'")

            # Print all positions where 'S' appears
            s_positions = [i for i, char in enumerate(line) if char == 'S']
            # Remove any positions outside the range [30, 40]
            s_positions = [pos for pos in s_positions if 35 <= pos <= 40]
            s_pos = 0
            if len(s_positions)==1:
                s_pos = s_positions[0]
            #print(f"DEBUG: 'S' found at positions -> {s_pos} in line: {repr(line)}")

            # If a "10" separator is found, start a new event
            if line.strip() == "10":
                if current_event:  # Save the previous event if it has arrivals
                    obspyevents = create_obspy_event(current_event)
                    catalog.events.extend(obspyevents) 
                    current_event = []  # Reset for the next event
                continue  # Skip processing the separator itself

            #  **Fixed Character Positions**
            station = line[0:3].strip()  # Positions 1-3
            if station.lower()=='xxx' or len(station)<3:
                continue
            orientation = line[3:4].strip()  # Position 4
            if orientation=='P':
                line = line[0:4] + '  ' + line[4:]
            p_arrival_code = line[4:8].replace(' ', '?')   # Positions 5-8
            if line[8]==' ':
                timestamp_str = line[9:24].strip().replace(" ", "0")  # Positions 10-24 (date/time)
            else:
                timestamp_str = line[8:23].strip().replace(" ", "0")  # Positions 10-24 (date/time)                
            s_wave_delay = line[s_pos-7:s_pos-1].strip() if s_pos else ""
            if s_pos>0:
                if len(line)>s_pos+3:
                    s_arrival_code = line[s_pos-1:s_pos+3].replace(' ', '?')
                else:
                    s_arrival_code = line[s_pos-1:].ljust(4).replace(' ', '?')
            else:
                s_arrival_code = ""  
            unknown_str = line[42:].strip() if len(line) > 47 else ""
            unknown_str = re.sub(r'[^\x20-\x7E]', '', unknown_str).strip()  # Keeps only printable ASCII characters

            #  **Check if P-wave data exists (only if 'P' in position 6, index 5)**
            has_p_wave = len(p_arrival_code) >= 2 and p_arrival_code[1] == "P"

            #  **Check if S-wave data exists (only if 'S' in position 2, index 1 of S-wave code)**
            has_s_wave = s_pos>0

            # Convert spaces and '?' in arrival codes to 'unknown'
            p_arrival_code = p_arrival_code.replace("?", " ") if has_p_wave else "unknown"
            s_arrival_code = s_arrival_code.replace("?", " ") if has_s_wave else "unknown"

            # Convert timestamp to UTC
            add_secs = 0
            if timestamp_str[-5:]=='60.00':
                timestamp_str = timestamp_str.replace('60.00', '00.00')
                add_secs += 60
            if timestamp_str[-7:-5]=='60':
                timestamp_str = timestamp_str.replace('60', '00')
                add_secs += 3600                
            try:
                timestamp = UTCDateTime(datetime.strptime(timestamp_str, "%y%m%d%H%M%S.%f"))
            except:
                try:
                    timestamp = UTCDateTime(datetime.strptime(timestamp_str, "%y%m%d%H%M"))
                except:
                    continue
            timestamp = timestamp + add_secs

            #  **Determine SEED channel**
            if orientation in "ZNE":  # Standard orientations
                channel = f"EH{orientation}"
            elif orientation == "L":  # Special case for "L"
                channel = "ELZ"
            else:
                channel = f'??{orientation}'
                #raise ValueError(f"Unknown orientation '{orientation}' in '{station}'")

            # Construct SEED ID
            seed_id = f"XB.{station}..{channel}"

            #  **Store P-wave arrival**
            if has_p_wave:
                p_arrival = {
                    "seed_id": seed_id,
                    "time": timestamp,
                    "onset": p_arrival_code[0] if p_arrival_code[0] in ["I", "E"] else "unknown",
                    "type": "P",
                    "first_motion": p_arrival_code[2] if p_arrival_code[2] in ["U", "D"] else "unknown",
                    "uncertainty": int(p_arrival_code[3]) if p_arrival_code[3].isdigit() else None,
                    "unknown": unknown_str
                }
                current_event.append(p_arrival)

            #  **Store S-wave arrival**
            if has_s_wave and s_wave_delay.replace(".", "").isdigit():
                s_wave_time = timestamp + timedelta(seconds=float(s_wave_delay))
                s_arrival = {
                    "seed_id": seed_id,
                    "time": s_wave_time,
                    "onset": s_arrival_code[0] if s_arrival_code[0] in ["I", "E"] else "unknown",
                    "type": "S",
                    "first_motion": s_arrival_code[2] if s_arrival_code[2] in ["U", "D"] else "unknown",
                    "uncertainty": int(s_arrival_code[3]) if s_arrival_code[3].isdigit() else None,
                    "unknown": unknown_str
                }
                current_event.append(s_arrival)

            '''
            except Exception as e:
                error_file.write(f"Error parsing line: {line}\nReason: {e}\n")
                error_file.write(f'{station}, {orientation}, {p_arrival_code}, {timestamp_str}, {s_wave_delay}, {s_arrival_code}, {optional_number}'+'\n\n')
                continue  # Skip this line and move to the next
            '''

    # Save the last event if there are remaining arrivals
    if current_event:
        obspyevents = create_obspy_event(current_event)
        catalog.events.extend(obspyevents)

def create_obspy_event(current_event):
    """
    Saves an event (list of arrivals) to an ObsPy Event object.
    
    Returns:
        obspy.core.event.Event: The ObsPy Event object.
    """

    picks = []  # List to store Pick objects

    events = []

    for arrival in current_event:

        seed_id = arrival['seed_id']
        pick_time = UTCDateTime(arrival['time'])
        onset = arrival['onset'] if arrival['onset'] != "unknown" else None
        if onset=='E':
            onset = 'emergent'
        elif onset=='I':
            onset='impulsive'
        else:
            onset=='questionable'
        phase_hint = arrival['type']  # "P" or "S"
        polarity = arrival['first_motion'] if arrival['first_motion'] != "unknown" else None
        if polarity=='U':
            polarity='positive'
        elif polarity=='D':
            polarity='negative'
        else:
            polarity='undecidable'
        uncertainty = float(arrival['uncertainty']) if arrival['uncertainty'] else None

        # Create a Pick object
        pick = Pick(
            time=pick_time,
            waveform_id=WaveformStreamID(seed_string=seed_id),
            onset=onset,
            phase_hint=phase_hint,
            polarity=polarity
        )
        picks.append(pick)
    
    # check that picks are within (say) 30-seconds of each other. arrange into groups
    grouped_picks = group_picks(picks)

    for group in grouped_picks:
        event = Event()  # Create an empty ObsPy Event
        arrivals = []

        for pick in group:

            # Create an Arrival object (linking to the Pick)
            arrival = Arrival(
                pick_id=pick.resource_id,
                phase=phase_hint,
                time_residual=uncertainty if uncertainty is not None else 0.0
            )
            arrivals.append(arrival)
    
        # Create an Origin object (hypocenter)
        origin_time = min(pick.time for pick in picks)  # Use earliest pick as origin time
        origin = Origin(
            time=origin_time,
            arrivals=arrivals
        )

        # Add Origin and Picks to Event
        event.origins.append(origin)
        event.picks.extend(picks)

        # append event to list of events (which gets added to full catalog)
        events.append(event)

    return events

def group_picks(picks, threshold=30):
    """
    Groups ObsPy Pick objects into clusters where each pick's time is within 'threshold' seconds of another in the group.

    :param picks: List of ObsPy Pick objects
    :param threshold: Maximum allowed time difference in seconds
    :return: List of lists, where each sublist contains grouped Pick objects
    """
    if not picks:
        return []
    
    # Sort picks by time
    picks = sorted(picks, key=lambda p: p.time)
    
    groups = []
    current_group = [picks[0]]

    for i in range(1, len(picks)):
        if (picks[i].time - current_group[-1].time) <= threshold:
            current_group.append(picks[i])
        else:
            groups.append(current_group)
            current_group = [picks[i]]
    
    # Append last group
    groups.append(current_group)

    return groups

#  **Process all `pin*91.pha` files**
SOURCE_DIR = '/data/Pinatubo/PHASE'
REPO_DIR = '/home/thompsong/Developer/Pinatubo1991SeismicData'
REA_DIR = '/data/SEISAN_DB/REA/PINAT'
WAV_DIR = '/data/SEISAN_DB/WAV/PINAT'
input_files = sorted(glob.glob(os.path.join(SOURCE_DIR,"pin*91.pha")))
catalogqml = os.path.join(REPO_DIR, 'metadata', 'pinatubo_catalog.xml') 
eventqmldir = os.path.join(REPO_DIR, 'metadata', 'event_qml')

# Create an empty ObsPy Catalog
catalog = Catalog()

for file in input_files:
    print(f"Processing: {file}")
    parse_monthly_phasefile(file, catalog)

import pickle
with open("temp_catalog.pkl", "wb") as f:
    pickle.dump(catalog, f)

# Sort the events by the first origin time
sorted_catalog = sorted(catalog, key=lambda event: event.origins[0].time if event.origins else None)

# Convert back to an ObsPy Catalog
sorted_catalog = catalog.__class__(sorted_catalog)

# Print sorted event times
for event in sorted_catalog:
    print(event.origins[0].time if event.origins else "No origin time")    

# Save the full Catalog as a QuakeML file
sorted_catalog.write(catalogqml, format="QUAKEML")

# Save each event to a separate file too
os.makedirs(eventqmldir, exist_ok=True)
for this_event in sorted_catalog.events:
    one_event_cat = Catalog(events=[this_event])
    otime = this_event.origins[0].time
    one_event_cat.write(os.path.join(eventqmldir, f'{otime.isoformat()}.xml'), format="QUAKEML")

print(f"All events processed and saved in {catalogqml} directory.")
print(" Any lines that could not be parsed have been logged in 'lines_that_could_not_be_parsed.txt'.")
