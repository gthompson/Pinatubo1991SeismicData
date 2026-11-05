# Pinatubo network history and StationXML draft

Run `05_network_history_and_stationxml.ipynb` from its pipeline directory, or `python -m network_history.reconstruct`. Requires ObsPy, NumPy, pandas, matplotlib, scipy, xarray, pyproj and netCDF4 (available in flovopy_env). All products are regenerated from the included evidence tables and cached modern relief, with no network access and no archive modifications. The optional header rescan reads the original waveform archive.

## Products

- `products/network_early_june.{png,svg}` and `network_july_august.{png,svg}`: historical seismic stations over a modern terrain background. July/August is a union of recorded identities, not a claim of simultaneous continuous operation.
- `products/chronology.md` and `pinatubo_deployment_history.csv`: dated installations, gain changes, failures, repair, repeater and observatory changes, with original precision. Not a claim that all changes are known.
- `products/telemetry.{png,svg}` and `pvo_operations.{png,svg}`: documented links and data flow. Editable edge tables supplied. Rebuilt routes and exact PVO/repeater coordinates remain unresolved.
- `products/timing_issues.md` / `timing_evidence.csv`: acquisition clock domains, tagging paths, drift, reboot/recollection evidence and correction limits. No universal time correction applied.
- `products/pinatubo_provisional.xml`: schema-valid metadata draft; 13 stations, 26 channel epochs. Includes alternative coordinates as station comments, seismic and provisional tilt channels, and unknown electronics explicitly. It intentionally has **no complete response**.
- `products/pinatubo_nominal_voltage_models.xml`: separate nominal sensor + attenuation transfer models, **m/s to V**, never counts. It is a model collection, not calibrated digital waveform metadata.
- `products/pinatubo_channel_epochs.csv`, `pinatubo_stations.csv`, `alternative_coordinates.csv`: editable intermediate products. Authoritative inputs are JSON files under `data`; regenerating overwrites product CSV edits.

## Response assumptions and limitations

The user chose documented nominal sensors with unknown electronics explicit. L4-C uses the selected 1 Hz, damping 0.7, 5500-ohm coil/8905-ohm shunt recipe: loaded flat sensitivity 170.854 V/(m/s), derived from the manufacturer brochure's 7.02 V/(in/s). L22-D uses the EarthScope nominal 2 Hz, damping 0.707, 88 V/(m/s) recipe. Actual deployed coil, shunt, damping and serial calibrations are unverified. L22-D variants differ: the 88 V/(m/s) selection is an assumption, not a recovered 1991 calibration. Attenuation is voltage factor `10**(-dB/20)`; GRN's 24 dB is inferred from calibration pulses. No response is assigned to unknown post-eruption sensor configurations or tilt. Completing digital responses requires VCO/preamp gain, telemetry/discriminator/filter response, ADC counts/V, and epoch evidence. Do not treat omitted electronics as unity gain or use these voltage models directly to remove a response from count data.

## Historical and metadata uncertainties

Published Table 1 coordinates supplied by Wendy match STATIONS.INP horizontally; their full bibliographic citation is pending. Those coordinates/elevations are selected for the seven pre-eruption sites. Other sites use STATIONS.INP positions plus PIN_STA_ALL elevations. Alternative positions and elevations are retained, including Lockhart's BUG latitude. These disagreements are not evidence of a relocation. Historical datum is unresolved; maps assume geographic WGS84. LIL has no published elevation here: the mandatory XML field is an explicitly labelled modern-DEM estimate. PIN_STA.DAT contains malformed longitude fields and is archived without automatically repairing them. DFR/NCT are Redoubt stations (user-verified provenance) and excluded from the Pinatubo network.

Date-only XML starts use conventional UTC midnight; inclusive failure days end at the next midnight. This is indexing convention, not recovered precise timing. Exact local source times are converted nominally with clock uncertainty retained. Adjacent epochs meet at their change boundary; interpret the ledger as half-open intervals. Documented data outages stay in the chronology rather than being assigned fabricated precise channel boundaries. CAB/BUG's post-eruption June 16 boundary is a convention; rebuilt sites start at the first observed header identity, an upper bound on installation only if the header is correct. Post-eruption configuration/gain and most burial depths are unknown. Zero depth for these channels and the unreported UBO/CAB/initial GRN depths is an explicitly documented required-field placeholder; GRN's approximate reburial is in the chronology rather than given a fabricated exact epoch.

PI2L is low gain, mapping to XB.PI2..ELZ. Its epoch is undated; the channel has no start/end or calibration, and must not be interpreted as operating throughout 1991. Earlier waveform conversion could have collapsed it into EHZ. Post-eruption CAB N is observed but sensor/geometry and calibration remain unverified. Tilt WA1/WA2 are provisional radial/tangential axis codes at 600 s sampling: UBO axes 274/4 degrees, polarity needs raw-data verification. PPO tilt installation was unreliable. Auxiliary temperature/battery/rain/lahar identities are documented but omitted as georeferenced XML channels until their positions, channel definitions and calibration are established.

All June–August headers were scanned. BUR/GRN/PIE/PPO/UBO header names occur after documented failures; no diagnosis is asserted without waveform inspection. Header presence alone does not establish valid data or operation. Independent Manila-held helicorders can constrain timing only after the actual recording-station identities, time marks and travel-time differences are established.

## Provenance and previous filenames

`data/sources.json` links primary papers, local PDFs and source hashes. Lockhart/Murray extracted text and raw station tables are included for reproducibility. Evidence from the supplied 2026 Power email is identified separately as recollection. Full PDF originals remain at their recorded source paths. Terrain is GMT Earth Relief/SRTM15 v2.7, 15 arcseconds, downloaded 2026-11-03; it is not the pre-eruption surface.

The earlier chat named `pinatubo_channel_epochs.csv`, `pinatubo_deployment_history.csv`, and `pinatubo_provisional.xml`. No original local copies were recovered; this package recreates and expands those intermediates rather than claiming to recover the old files. Code is local to this project because the historical evidence, parsers and chronology are Pinatubo-specific. The map renderer uses explicit regional bounds and the same modern-relief/scientific plotting approach as the shared mapping examples.

## Validation

Run `python -m network_history.test_reconstruction`. Checks cover XML schema, absent end-to-end count responses, tilt and low-gain identities, voltage response evaluation, attenuation ratios, coordinate provenance and nonoverlapping dated channel epochs.
