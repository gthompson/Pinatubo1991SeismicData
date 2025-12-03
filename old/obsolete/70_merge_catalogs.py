import os
from obspy import UTCDateTime, read_events
from obspy.core.event import Catalog

def merge_catalogs(hypo71catalog, picktimecatalog):
    """
    Merges two ObsPy Catalog objects: one from HYPO71 and one from pick-based origins.
    """
    mergedcatalog = Catalog()
    
    # Create sorted list of pick-based origin times
    sorted_pick_time_list = sorted((event.origins[0].time, event) for event in picktimecatalog if event.origins)
    
    for hypo_event in hypo71catalog:
        hypo_time = hypo_event.origins[0].time
        
        # Find the first picktime event that has an origin time larger than the hypo71 event
        matching_event = next((event for pick_time, event in sorted_pick_time_list if pick_time > hypo_time), None)
        
        if matching_event:
            merged_event = Event()
            merged_event.origins.extend([hypo_event.origins[0], matching_event.origins[0]])
            merged_event.magnitudes.extend(hypo_event.magnitudes + matching_event.magnitudes)
            merged_event.picks.extend(matching_event.picks)
            mergedcatalog.append(merged_event)
    
    return mergedcatalog




SOURCE_DIR = '/data/Pinatubo/PHASE'
REPO_DIR = '/home/thompsong/Developer/Pinatubo1991SeismicData'
REA_DIR = '/data/SEISAN_DB/REA/PINAT2'
WAV_DIR = '/data/SEISAN_DB/WAV/PINAT'
#catalogqml = os.path.join(REPO_DIR, 'metadata', 'pinatubo_catalog.xml') 
wavqmlfile = os.path.join(REPO_DIR, 'metadata', 'pinatubo_wavcatalog.xml') 
hypo71qml = os.path.join(REPO_DIR, 'metadata', 'Pinatubo_all_hypo71.xml')
mergedqml = os.path.join(REPO_DIR, 'metadata', 'Pinatubo_total.xml')

picktimecatalog = read_events(wavqmlfile)
hypo71catalog = read_events(hypo71qml)

mergedcatalog = merge_catalogs(hypo71catalog, picktimecatalog)
mergedcatalog.write(mergedqml, format="QUAKEML")

# Print merged events
for event in mergedcatalog:
    print(f"Merged Event: Time {event.origins[0].time}, Magnitudes: {[mag.mag for mag in event.magnitudes]}")