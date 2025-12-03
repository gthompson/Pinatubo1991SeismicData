# 1991 Pinatubo Seismic Data Conversion & FAIR Reconstruction

**A Complete, Reproducible Pipeline for Modernizing the 1991 PHIVOLCS–USGS VDAP Seismic Archive**

**Author:** Glenn Thompson  
**Affiliation:** University of South Florida  
**ORCID:** 0000-0002-9173-0097  
**Email:** thompsong@usf.edu  

⸻

## Project Overview

This repository contains a fully reproducible, open-source workflow for converting, harmonizing, and re-archiving the surviving legacy seismic records from the 1991 eruption of Mount Pinatubo.

The dataset was originally collected jointly by PHIVOLCS and the U.S. Geological Survey Volcano Disaster Assistance Program (VDAP) using a rapidly deployed analog–digital telemetry system.
However:
	•	Waveforms were stored in legacy SUDS/DMX formats
	•	Phase picks and hypocenters exist only in partial, inconsistent PHA and HYPO71 files
	•	Original event-level files are lost
	•	Clock drift, naming inconsistencies, and missing metadata complicate analysis

This repository provides:

✔ A seven-stage Python pipeline for full FAIR reconstruction
✔ Modern MiniSEED + QuakeML + SEISAN REA outputs
✔ A unified, cross-referenced event catalog
✔ Scripts designed for transparency, modularity, and reproducibility
✔ Self-contained helper functions (no FLOVOPY dependency)

A companion JOSS submission will document the pipeline as research-grade software.

⸻

## Repository Structure

Pinatubo1991SeismicData/
│
├── README.md
├── LICENSE
├── environment.yml            # Conda environment for full pipeline
│
├── pipeline/                  # Standalone processing scripts
│   ├── 01_dmx_to_seisanWAV.py
│   ├── 02_index_waveforms.py
│   ├── 03_parse_phase.py
│   ├── 04_parse_hypo71.py
│   ├── 05_associate_phase_hypo71_waveforms.py
│   ├── 06_build_unified_catalog.py
│   ├── 07_qc_and_exports.py
│   ├── flovopy_functions.py   # Self-contained helper library
│   └── run_pinatubo_pipeline.sh
│
├── metadata/                  # Derived catalogs & intermediate metadata
│   ├── wfdisc_catalog.csv
│   ├── wfdisc_catalog.xml
│   ├── trace_id_mapping.csv
│   ├── pha/
│   ├── hypo71/
│   ├── association/
│   └── qc/
│
├── docs/                      # Documentation and figures
│   ├── overview.md
│   ├── pipeline_diagram.png
│   ├── data_structure.md
│   └── figures/
│
├── notebooks/                 # Analysis, visualization & exploration
│   ├── exploratory/
│   ├── figures/
│   └── legacy/
│
├── papers/                    # Manuscripts (Volcanica, DI&B, JOSS)
│   ├── Volcanica/
│   ├── DataInBrief/
│   └── JOSS/
│
└── tests/                     # Unit tests for key components
    ├── test_parse_phase.py
    ├── test_parse_hypo71.py
    ├── test_associate.py
    ├── data/
    └── utils/


⸻

## Installation

This repository uses a conda environment for maximum reproducibility.

mamba env create -f environment.yml
mamba activate pinatubo_fair


⸻

## Pipeline Overview (01–07)

The FAIR reconstruction workflow is divided into seven modular steps:

01 — Convert DMX → MiniSEED
	•	Reads ~21.5k legacy DMX waveform files
	•	Removes telemetry offsets, IRIG channels, empty traces
	•	Normalizes station/channel IDs (short-period EH? codes)
	•	Writes SEISAN-style WAV MiniSEED archive

02 — Index Waveforms & Build wfdisc Catalog
	•	Scans the MiniSEED WAV/ tree
	•	Builds a wfdisc_catalog.csv
	•	Generates a QuakeML wrapper (wfdisc_catalog.xml)

03 — Parse PHA Phase-Arrival Files
	•	Reads manually picked P/S arrivals from:
pinmay91.pha, pinjun91.pha, pinjul91.pha, pinaug91.pha
	•	Normalizes station/channel codes
	•	Handles inconsistent formatting and missing delimiters
	•	Clusters picks into candidate events

04 — Parse HYPO71 Summary Catalog
	•	Reads and parses Pinatubo_all.sum
	•	Outputs Event objects in QuakeML format
	•	Stores clean metadata tables

05 — Associate Picks, Hypocenters & Waveforms
	•	Matches HYPO71 origins to pick clusters via time-window logic
	•	Associates waveform files via start–end time overlaps
	•	Flags ambiguous or unmatched events

06 — Build Unified Catalog
	•	Constructs a consolidated dataset of:
	•	waveform-only events
	•	pick-only events
	•	hypocenter-only events
	•	fully merged events
	•	Outputs:
	•	QuakeML
	•	SEISAN REA
	•	metadata tables

07 — Quality Control & Export
	•	Summaries, plots, and consistency checks
	•	Daily event rates, coverage heatmaps
	•	Clock drift diagnostics
	•	Export of research-ready files

⸻

## Goals of This Repository
	•	Preserve the most complete digital representation of the 1991 Pinatubo seismic dataset
	•	Enable modern waveform and catalog analysis (QuakeML, MiniSEED, SEISAN)
	•	Support machine learning, clustering, and new interpretations
	•	Document the full provenance and reproducibility of the reconstruction
	•	Prepare the dataset for DOI-based archiving (Zenodo + EarthScope/ScienceBase)

⸻

## Citation

Repository:

Thompson, G. (2025). Pinatubo1991SeismicData: FAIR Reconstruction of the 1991 Pinatubo Seismic Archive. GitHub Repository.

JOSS (in preparation):
A citation entry will be provided once the JOSS article is published.

⸻

## Testing

Tests live in tests/ and can be executed via:

pytest -v


⸻

## License

An MIT License has been added.

⸻

## Acknowledgments

This project builds on data collected by PHIVOLCS and the USGS Volcano Disaster Assistance Program (VDAP) during the 1991 eruption.
Special thanks to colleagues who assisted with data discovery, scanning, metadata recovery, and historical insights.


