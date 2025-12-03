---
title: "A Python Pipeline for Recovering, Converting, and FAIR-ifying the 1991 Mount Pinatubo Seismic Dataset"
tags:
  - seismology
  - volcanology
  - data conversion
  - FAIR data
  - Python
  - ObsPy
  - seismic catalogs
  - reproducibility
authors:
  - name: Glenn Thompson
    orcid: 0000-0002-9173-0097
    affiliation: 1
affiliations:
  - name: University of South Florida, School of Geosciences, Tampa, FL, USA
    index: 1
date: 2025-11-19
bibliography: paper.bib
---

# Summary

The 1991 eruption of Mount Pinatubo was one of the largest volcanic events of the twentieth century and a landmark in modern volcanic crisis response. Yet the original seismic dataset—collected through a rapidly deployed PHIVOLCS–USGS Volcano Disaster Assistance Program (VDAP) network—has remained inaccessible for over thirty years. Waveforms were stored in legacy SUDS/DMX formats, phase arrivals scattered across partially surviving `.pha` files, and hypocentral information preserved only in an irregular HYPO71 summary. No complete digital archive existed.

This software presents a fully reproducible Python pipeline for recovering, converting, harmonizing, and FAIR-ifying the surviving seismic records. The workflow converts >21,000 DMX files to MiniSEED, reconstructs phase-pick catalogs from four monthly `.pha` files, parses the surviving HYPO71 summary, associates waveforms with events, and outputs a unified catalog in MiniSEED, SEISAN REA, QuakeML, and CSV formats. All scripts are open source, self-contained, and designed to support transparent provenance.

The accompanying GitHub repository includes the full pipeline, Jupyter notebooks, metadata corrections, station/component mapping tables, and documentation. The software offers a generalizable framework for rescuing and modernizing other early-digital volcano-seismic datasets.

# Statement of Need

Thousands of volcano-seismic datasets from the 1980s–2000s remain trapped in legacy formats (SUDS, AH, CSS, HYPO71) or incomplete analog archives. These datasets contain irreplaceable information on precursory patterns, long-term hazard evolution, and historical crises, yet remain largely unusable.

The Pinatubo archive is especially important:

- It underpinned one of the most successful volcanic evacuations in history.  
- It contains deep long-period (DLP) sequences rarely captured at this scale.  
- It records hybrid, LP, tremor, and caldera-collapse seismicity during a VEI-6 eruption.  
- It provides a rare opportunity to revisit a textbook crisis with modern tools.

Prior to this work, no end-to-end software existed that could:

- read mixed-format DMX/SUDS files,  
- reconstruct event catalogs from partial PHA and HYPO71 files,  
- normalize inconsistent metadata,  
- and generate interoperable FAIR outputs.

This pipeline fills that gap, providing a reusable solution for historical volcanic datasets worldwide.

# Software Description

## Features

The software provides:

- **DMX → MiniSEED conversion**, robust to malformed SUDS variants.  
- **PHA parser** for reconstructing P- and S-pick catalogs from four monthly files.  
- **HYPO71 summary parser** using a flexible grammar with alignment correction.  
- **Event association**, merging picks and hypocenters into a unified catalog.  
- **Waveform association**, linking MiniSEED files to reconstructed events.  
- **Station and channel normalization** to SEED-compliant metadata.  
- **Output formats**: MiniSEED, SEISAN REA, Nordic S-files, QuakeML, and CSV.  
- **Provenance capture** with end-to-end Jupyter notebooks.  
- **FAIR-compliant directory structure** for long-term archiving.

Although tailored to Pinatubo, the modules generalize well to other VDAP-era and early digital networks.

## Implementation

The implementation is entirely in Python and uses:

- **ObsPy** for waveform reading, MiniSEED writing, and QuakeML/SEISAN integration  
- **pandas** for catalog and metadata operations  
- **NumPy** for array handling  
- **Regular-expression-based parsers** for PHA and HYPO71 variants  
- **Standalone utility modules** for SUDS/DMX reading and station/channel normalization  

The package is cross-platform and tested on macOS and Linux.

# State of the Field

Historical seismic datasets are increasingly valuable for:

- machine-learning model training,
- retrospective precursor studies,
- long-term pattern recognition,
- global comparative volcanology,
- and educational use in observatory training programs.

However, most pre-2000 volcano datasets remain inaccessible due to legacy formats, fragmented archives, or missing metadata. Existing seismological software (ObsPy, SAC, SeisComP, SEISAN) does not provide generalized tools for SUDS/DMX variants or for reconstructing mixed-format, partially lost event catalogs.

This pipeline demonstrates a modern, reproducible solution for recovering such archives and provides a template for broader community adoption.

# Design and Architecture

The reconstruction workflow consists of seven modular stages:

1. **Waveform discovery and DMX→MiniSEED conversion**  
2. **Waveform indexing into a wfdisc-like catalog**  
3. **Parsing and reconstruction of PHA phase arrivals**  
4. **Parsing of HYPO71 summary hypocenters**  
5. **Association of picks, hypocenters, and waveform files**  
6. **Construction of a unified ObsPy Catalog**  
7. **FAIR-compliant exports and QC diagnostics**

Each stage is implemented as a script and as an associated Jupyter notebook, enabling both pipeline automation and human-verifiable transparency.

# Validation

Validation includes:

- Comparing reconstructed hypocenters with values published in *Fire and Mud*.  
- Visual review of waveform associations.  
- Alternative-parameter reprocessing to verify stability.  
- Spot-checking timing drift across stations.  
- Testing parsers against synthetic corrupted PHA/HYPO71 lines.  

A minimal pytest suite is included for core parsing components.

# Example Usage

```python
from pinatubo1991 import dmx2mseed, parse_phases, parse_hypo71, merge_catalogs

# Convert DMX files to MiniSEED
dmx2mseed.convert_all("raw_dmx/", "mseed/")

# Read monthly PHA files
phases = parse_phases("pha/")

# Read HYPO71 summary
hypo = parse_hypo71("Pinatubo_all.sum")

# Merge into a unified catalog
cat = merge_catalogs(phases, hypo)
cat.write("Pinatubo_merged.xml", format="QUAKEML")
```

Full pipeline examples are provided in the notebooks/ directory.

# Reproducibility and FAIR Compliance

The workflow follows FAIR principles:
	•	Findable — GitHub repository and planned Zenodo DOI
	•	Accessible — Open formats: MiniSEED, QuakeML, SEISAN
	•	Interoperable — SEED-compliant metadata, StationXML, Nordic, CSV
	•	Reusable — Complete provenance (scripts + notebooks), MIT license

# Acknowledgements

I thank John Power, Randy White, and Tom Murray (USGS, retired) for recovering and sharing the original Pinatubo data. I acknowledge the ObsPy community for tools central to this workflow. This project was conducted as part of ongoing research at the University of South Florida.

# References

\nocite{*}