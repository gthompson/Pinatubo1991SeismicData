# Timing evidence and correction limits

No universal clock correction is applied. Daily WWV checks were intended, but checks were sporadic; nominal ±0.1 s setting accuracy is not an archive accuracy claim.

## Analog drums / helicorders

**Machine:** Four drum recorders described by Lockhart; individual clock hardware unresolved

**Products:** Paper seismic records

**Clock reference:** Manually synchronized to WWV

**Documented issues:** Daily checks intended but often omitted; manual setting no better than ±0.1 s. Drift/reset history of each drum not quantified. A GOES satellite clock was available but its satellite signal was not receivable there, so it was not used.

**Software offset:** No MDETECT offset applies to drums

**Source:** Lockhart Observatory Timing and Equipment

**Uncertainty:** Do not assume continuous ±0.1 s accuracy; inspect paper time marks and corrections.

## MDETECT

**Machine:** WILLIE (IBM-AT)

**Products:** Triggered digitized seismic events

**Clock reference:** WILLIE PC, manually set to WWV; initially local time

**Documented issues:** PC drift reached minutes on occasions; Power recalls daily/configuration reboots and version/GMT change about May 25.

**Software offset:** Murray: event time tag = PC clock + 8 hours before fix; correction date not established.

**Source:** Murray local/GMT section; Lockhart; Power email Oct 29 2026

**Uncertainty:** Never apply a blanket correction. Conditional local-PC-plus-8 tags could be UTC+16; validate original fields, software versions and epochs.

## SSAM

**Machine:** WILLIE (same PC as MDETECT)

**Products:** 1-minute spectra; later 10-minute BOB summaries

**Clock reference:** Same hardware clock as event acquisition, different early tagging path

**Documented issues:** Software tagged SSAM directly with PC clock; not with the event +8-hour adjustment. Spectral averaging time convention needs confirmation.

**Software offset:** SSAM = PC clock before fix; early SSAM/event tags differ by +8 h for event tags.

**Source:** Murray SSAM and local/GMT sections

**Uncertainty:** Shared clock is not shared timestamp semantics. SSAM_VU/SCANSSAM and XDETECT discussed as later improvements, not proven 1991 deployment versions.

## RSAM / RSAM EVNT

**Machine:** Tandy Model 100

**Products:** Amplitude means and single-channel trigger counts

**Clock reference:** Independent acquisition clock, WWV manual setting reported

**Documented issues:** Clock sometimes drifted by minutes. Transfer to PORKY every 10 minutes is not proof of synchronization.

**Software offset:** No evidence MDETECT +8-hour bug directly applies

**Source:** Murray hardware; Lockhart timing; user-confirmed RSAM trigger interpretation

**Uncertainty:** Check native annual-file padding, missing sentinel and interval start/end semantics separately from actual clock corrections.

## TILT / LDR

**Machine:** SLEEPY (IBM-XT)

**Products:** UBO/PPO tilt, temperature and battery voltage

**Clock reference:** Separate acquisition computer; exact time-tag authority PC vs remote platform unresolved

**Documented issues:** Initial all-clock local-time statement in Murray; precise synchronization practice and later changes for SLEEPY not established.

**Software offset:** No documented MDETECT offset inheritance

**Source:** Murray tilt/hardware; Ewert; Lockhart

**Uncertainty:** Ten-minute logging/transfer does not establish precise timing or alignment with RSAM.

## Lahar / AFM

**Machine:** RICKY (IBM-XT)

**Products:** Lahar detector measurements

**Clock reference:** Separate computer; exact remote/receiver timestamp ownership unresolved

**Documented issues:** Initially standalone; later serial transfer to PORKY via printer port converter and print sharer.

**Software offset:** No documented MDETECT offset inheritance

**Source:** Murray lahar section; BOB STATIONS.TXT names LSAC/USAC/MARL/ABAC

**Uncertainty:** Event-driven transfer cadence; no subsecond clock accuracy claim supported.

## Aggregation / BOB

**Machine:** PORKY (IBM-AT)

**Products:** Copied/converted SSAM, RSAM, tilt and lahar time series

**Clock reference:** Collection/analysis PC, not automatically the source clock

**Documented issues:** Print-sharer buffering and conversion can separate receipt time from measurement time.

**Software offset:** Timestamp copying/conversion details must be checked by product

**Source:** Murray hardware

**Uncertainty:** Do not retime source measurements to PORKY receipt time.

## Picking / hypocenter processing

**Machine:** SQUAB (386)

**Products:** PCEQ phase picks; HYPO71PC origins; tape archive

**Clock reference:** Acquisition timestamps plus analyst processing conventions

**Documented issues:** Processing time is not event time. Catalog local/GMT conventions and any analyst corrections require independent provenance checks.

**Software offset:** Do not infer universal transition from one catalog annotation

**Source:** Murray hardware; catalog time conventions unresolved in this reconstruction

**Uncertainty:** Preserve recorded, interpreted and corrected times separately.

## Analysis / remote access

**Machine:** DAVE and ANDY (IBM-AT)

**Products:** BOB plots, communications, advisories; ANDY 9600-baud modem

**Clock reference:** Analysis computer clocks, not independent physical event references

**Documented issues:** File creation/receipt/plot clocks must not be confused with acquisition clocks.

**Software offset:** Unknown/not established

**Source:** Murray hardware

**Uncertainty:** Record product lineage; network file sharing did not imply clock synchronization.

## Independent references

**Machine:** Local paper records and Manila-held national-network helicorders

**Products:** Potential timing tie points for shared events

**Clock reference:** Must establish station identity and time-mark/clock reliability

**Documented issues:** Manila is about 100 km from Pinatubo, but sensor can be remote from headquarters; account for travel-time differences.

**Software offset:** Use independently validated tie points to estimate offset/drift/reset epochs

**Source:** User-supplied email context; USGS geographical descriptions

**Uncertainty:** Two unchecked clocks establish relative disagreement, not absolute UTC accuracy.