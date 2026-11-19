---
title: 'Pinatubo1991SeismicData: reconstructing a legacy volcano-seismic archive on UTC with Python'
tags:
  - Python
  - seismology
  - volcanology
  - legacy data
  - seismic catalogs
  - data preservation
authors:
  - name: Glenn Thompson
    orcid: 0000-0002-9173-0097
    affiliation: 1
affiliations:
  - name: School of Geosciences, University of South Florida, Tampa, Florida, USA
    index: 1
date: 19 November 2026
bibliography: paper.bib
---

# Summary

Pinatubo1991SeismicData converts the surviving seismic and monitoring records of the 1991 Mount Pinatubo eruption into a consistent, documented archive. The records survived as separate pieces: 21,475 triggered waveform files in the PC-SUDS DMX format [@ward1989], analyst phase cards, two HYPO71 hypocenter summaries [@lee1975], and the BOB archive of amplitude, spectral, tilt, rain, lahar and gas products. Each was time-stamped by a different acquisition computer, using Philippine time or GMT at different times. As recovered, only 12.9% of hypocenters fell inside a waveform record.

The software reconstructs the clock history of every data stream, encodes it as a table of rules, and converts every time to UTC where it is read; afterwards 97.3% of hypocenters fall inside a waveform record. It writes MiniSEED in a SEISAN database [@havskov1999], a linked catalog of 21,753 events that records which of waveforms, picks and hypocenters survive for each, exports to QuakeML [@schorlemmer2011], SEISAN REA and Antelope CSS3.0, StationXML built from the network history, and tabular BOB products. A second workflow measures noise and event windows across all 21,459 converted files. The code builds on ObsPy [@beyreuther2010; @krischer2015] and the FloVoPy library, and is released under the MIT license.

# Statement of need

Early digital volcano-monitoring records can support reassessment of unrest, comparison of recording systems and training, but they are hard to reuse. Event files, phase picks and locations were kept separately, copied into overlapping collections, and stamped by computers whose clocks were never tied to UTC. Published accounts of Pinatubo document the scientific and operational context [@newhall1996] but do not provide machine-readable observations with traceable processing.

The intended users are volcano seismologists, observatory data managers and researchers preserving early digital seismic collections. They need a workflow that corrects what can be corrected, keeps the raw values and the evidence beside every correction, and exposes unmatched records, alternative solutions and processing exclusions rather than only the apparently complete subset. For Pinatubo the decisive problem was time: without explicit clock rules, the waveforms, picks and hypocenters could not be linked at all.

# State of the field

ObsPy provides the waveform and event data structures and the format readers on which this work depends [@beyreuther2010; @krischer2015], and SEISAN and Antelope provide the target databases. Standard formats make recovered observations easier to exchange, but they cannot decide whether two incompletely documented records describe the same earthquake, or which clock a 1991 PC was set to. This project adds what sits above format conversion: an explicit, testable model of each acquisition clock; rules that link picks and hypocenters to triggered records; and provenance that keeps every raw value. Code general to VDAP-era archives (DMX reading and partial-file recovery, phase-card and HYPO71 parsing, the clock-rule engine, REA export, BOB readers) was moved into FloVoPy so other archives can reuse it; Pinatubo decisions stay in this repository. The approach supports FAIR stewardship [@wilkinson2016], though format conversion alone does not make a dataset fully reusable.

# Software design

The conversion is a sequence of numbered Jupyter notebooks (steps 10–72). Each has a configuration cell, a definitions cell that holds only functions and is loaded directly by the tests, and run cells. Paths come from one configuration file, and outputs are written outside the repository.

**Clocks as data.** The history of every acquisition clock is a CSV table (`clock_regimes.csv`): one row per stream and period, with the raw-clock interval, an optional file-order bound and header marker, the offset to UTC, a nominal uncertainty, a confidence level and the evidence. A generic engine in FloVoPy applies the table and refuses records that match no rule or more than one. Each step converts times where it reads them and keeps the raw time, rule and uncertainty in separate columns; QuakeML origins carry the uncertainty as time errors. The rules encode, for example, that the acquisition software converted the seismic PC's clock as if it were in California, that this PC was reset from Philippine time to GMT on 26 May 1991, and that the RSAM and lahar computers switched to GMT around 13 September.

**Stages.**

- *Waveforms* (10–11): DMX to MiniSEED with the original 3125/31 Hz sample rate, SEED identifiers, and a per-file log that accounts for every input file and dropped trace.
- *Picks* (20–23): fixed-column phase-card decoding with logged trims and splits.
- *Waveform–pick events* (30–35): whole source blocks attached to records, with recorded duplicate suppression.
- *Hypocenters* (40–44): both summaries indexed with provenance, then grouped with a stated preferred-origin policy.
- *Linked catalog* (50–54): hypocenters linked through their picks first, then by record window, one to one; seven composition classes; exports to QuakeML, SEISAN REA and CSS3.0.
- *BOB products* (60–69): one notebook per product, each with common quality outputs.
- *Network metadata* (70–71): network history and StationXML, built last, once every stream is on UTC; every archived trace must fall inside an epoch.
- *Clock anchors* (72): ISC and USGS origin times matched against triggered records to bound the residual clock error.

**Analysis.** A second notebook sequence inventories the archive, selects noise and event windows by a fixed, detected or reviewed policy, and computes FloVoPy features and matched Welch and multitaper spectra. Full results per recording go to JSON sidecars under a computation signature, so unchanged recordings are reused, and the complete table is exported to Parquet. Run identifiers hash the configuration, inputs and code.

**Testing.** The conversion has 77 tests: clock rules, parsing, catalog and export semantics, BOB products and network reconstruction. The analysis has 52, and FloVoPy has its own suite.

# Research impact statement

The software produced the UTC version of EarthScope Assembled Dataset 25-028 (PIN 1991 FAIR, network XB 1991):

- 21,459 waveform files (152,933 traces), 58,068 picks and 8,233 hypocenter solutions in 4,690 groups, linked into 21,753 events, of which 4,353 have a waveform, picks and a hypocenter.
- 4,628 of the 4,690 hypocenter groups are linked to a waveform or pick event, against 3,076 under the earlier start-time rule.
- Matching triggered records against regional and teleseismic arrivals bounds the residual absolute clock error at −23 to +12 s after the 26 May reset.

A companion data paper describes the acquisition history, coverage and limitations. The clock-rule approach, which treats each acquisition computer as a separate stream with documented rules, is designed to transfer to other VDAP-era archives; its use outside Pinatubo has not yet been demonstrated.

# AI usage disclosure

OpenAI Codex and Anthropic Claude (Claude Code) assisted with code inspection and refactoring, the clock-reconstruction analyses, tests, documentation and drafting this paper. The author reviewed the code changes and the clock rules and judged the distant-earthquake records used as clock anchors. The author remains responsible for verifying the prose, calculations and references before submission.

# Acknowledgements

We acknowledge the PHIVOLCS and USGS Volcano Disaster Assistance Program personnel who collected and preserved the original observations, and John Power, Randall White and Thomas Murray for sharing surviving materials and explaining the acquisition systems. We thank the ObsPy community for the software infrastructure used here.

# References
