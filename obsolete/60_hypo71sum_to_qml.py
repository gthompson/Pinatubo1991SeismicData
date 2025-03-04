
SOURCE_DIR = '/data/Pinatubo/PHASE'
REPO_DIR = '/home/thompsong/Developer/Pinatubo1991SeismicData'
from libseisGT import parse_hypo71_file

# Example usage
file_path = os.path.join(SOURCE_DIR,"Locations","Pinatubo_all.sum")  # Replace with actual file
hypo71catalog, unparsed_lines = parse_hypo71_file(file_path)


# Print parsed events
'''
for event in hypo71catalog:
    print(f"Time: {event.origins[0].time}, Lat: {event.origins[0].latitude}, Lon: {event.origins[0].longitude}, "
          f"Depth: {event.origins[0].depth}m, Mag: {event.magnitudes[0].mag}, "
          f"n_ass: {event.origins[0].comments[0]}, Time Residual: {event.origins[0].comments[1]}")
'''

hypo71qml = os.path.join(REPO_DIR, 'metadata', 'Pinatubo_all_hypo71.xml')
hypo71catalog.write(hypo71qml, format="QUAKEML")
print(f"\n✅ Saved hypo71 catalog as {hypo71qml}")

print(unparsed_lines)
