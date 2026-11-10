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

The 1991 eruption of Mount Pinatubo was one of the largest volcanic events of the 20th century and a landmark eruption for modern volcanic crisis response. Despite this historic significance, the original seismic dataset collected by the joint PHIVOLCS–USGS Volcano Disaster Assistance Program (VDAP) has remained scattered across obsolete formats, inconsistent catalogs, and partially degraded analog and digital sources. These limitations have prevented meaningful reanalysis using modern seismological or machine-learning tools.

This paper presents a Python-based pipeline and open-source software package that recovers, converts, and harmonizes the surviving digital and analog elements of the Pinatubo dataset into fully FAIR-compliant (Findable, Accessible, Interoperable, and Reusable) formats. The workflow converts >21,000 SUDS/DMX waveform files to MiniSEED, reconstructs phase and hypocenter catalogs from four monthly PHA files and a surviving HYPO71 summary, integrates these datasets into SEISAN REA and QuakeML formats, and generates a unified catalog suitable for reproducible analysis and downstream research.

The accompanying GitHub repository includes the full processing pipeline, Jupyter notebooks for reproducibility, metadata mappings, and documentation. The software provides a generalizable framework for rescuing other legacy volcano-seismic datasets from the pre-digital or early-digital monitoring era.

# Statement of Need

Tens of thousands of volcano-seismic datasets from the 1980s–2000s remain trapped in obsolete formats (SUDS, AH, CSS, HYPO71), partial analog archives, or undocumented institutional storage. These records contain irreplaceable information about eruptions, precursory patterns, and crisis decision-making, yet they cannot be used without labor-intensive reverse engineering.

The 1991 Pinatubo dataset is especially important:  
- its seismicity informed one of the most successful evacuations in volcanic history,  
- it documents deep long-period (DLP) seismicity rarely captured at this scale,  
- and it remains essential for understanding dome-forming, hybrid, and LP precursors.

However, prior to this work, no complete digital archive existed due to:  
- reliance on early PC-based telemetry;  
- SUDS/DMX formats with undocumented variants;  
- loss of event-level .HYP and .PHA files;  
- severe clock drift across different acquisition systems;  
- incomplete or inconsistent station metadata.

No existing tools could reconstruct this dataset end-to-end within a reproducible workflow.  
This software fills that gap.

# Software Description

## Features

The software includes:

- **DMX → MiniSEED conversion** using an ObsPy-based reader robust to malformed or partially corrupted SUDS files.
- **Reconstruction of phase-pick catalogs** from four surviving `.pha` files using custom fixed-width parsers.
- **Parsing of HYPO71 summary catalogs** with correction of alignment errors and inconsistent formatting.
- **Temporal clustering and event association algorithms** for merging picks and hypocenters.
- **Waveform association** linking MiniSEED files to reconstructed events.
- **Metadata normalization** converting legacy station names and component codes into SEED-compliant metadata.
- **Output to SEISAN, QuakeML, and CSV formats** for interoperability with modern seismological ecosystems.
- **Full reproducibility** via Jupyter notebooks and automated provenance capture.
- **FAIR-compliant archival structure** ensuring long-term usability and extensibility.

The pipeline is modular and can be adapted to other VDAP-era datasets (e.g., Redoubt 1989–90, Spurr 1992, Soufrière Hills 1995–2010).

## Implementation

The software is written in Python and uses:

- **ObsPy** for waveform handling  
- **pandas** for catalog and metadata management  
- **NumPy** for structured data operations  
- **Jupyter** for reproducibility  
- **regex-based parsers** for nonstandard HYPO71/SUDS variants  
- **SEISAN-compatible utilities** for Nordic file output  

The package is OS-agnostic and runs on Linux, macOS, and Windows.

# State of the Field

Legacy datasets are increasingly crucial for modern research, including:

- training machine-learning models on large historical event catalogs,  
- improving eruption forecasting through comparative studies,  
- retrospective analysis of precursory phenomena such as DLPs,  
- and reconstructing long-term deformation or hazard trends.

Yet for most volcanoes monitored before 2010, the underlying digital datasets remain inaccessible.

Existing tools (ObsPy, SeisComP, SAC, SEISAN) do not support:  
- SUDS/DMX variants used by early VDAP networks,  
- mixed-format legacy catalogs,  
- or end-to-end dataset reconstruction with provenance.

This pipeline demonstrates a modern, reproducible solution for recovering these irreplaceable archives.

# Design and Architecture

The pipeline follows a modular, stage-based architecture:

1. **Discovery** of all DMX files, PHA files, and the HYPO71 summary.  
2. **Waveform conversion** with error-handling wrappers for malformed headers.  
3. **Metadata cleaning** with mapping tables stored under version control.  
4. **Phase parsing** and temporal clustering to reconstruct event identities.  
5. **Hypocenter parsing** using a flexible HYPO71 grammar.  
6. **Catalog integration** using temporal and pick-structure similarity.  
7. **Waveform-event association** using trace time windows.  
8. **Archival output** to SEISAN REA and QuakeML.

Each step is implemented in its own Jupyter notebook to provide transparent provenance.

# Validation

Validation consists of:

- comparing reconstructed hypocenters with published values from *Fire and Mud*,
- visual inspection of waveform associations,
- reproducibility tests re-running the full pipeline with alternative parameters,
- spot checks of timing drift corrected through catalog harmonization,
- synthetic tests using modified PHA files to confirm clustering logic.

A minimal test suite (pytest) is planned for the next version.

# Example Usage

```python
from pinatubo1991 import dmx2mseed, parse_phases, parse_hypo71, merge_catalogs

# Convert DMX files
dmx2mseed.convert_all("raw_dmx/", "mseed/")

# Parse monthly phase arrivals
phases = parse_phases("pha_files/")

# Parse HYPO71 summary file
hypo = parse_hypo71("Pinatubo_all.sum")

# Merge picks + hypocenters
merged = merge_catalogs(phases, hypo)

# Save to QuakeML
merged.write("Pinatubo_merged.xml", format="QUAKEML")

Example notebooks demonstrate full end-to-end reconstruction.

# Reproducibility & FAIR Compliance

The pipeline implements FAIR principles:
	•	Findable: DOI-tagged dataset and GitHub release planned.
	•	Accessible: All converted data use open formats (MiniSEED, QuakeML, CSV).
	•	Interoperable: Metadata normalized to StationXML/SEED; catalogs follow QuakeML.
	•	Reusable: Full provenance provided via notebooks; MIT license ensures permissive reuse.

# Acknowledgements

I thank John Power, Randy White, and Tom Murray (USGS, retired) for recovering and sharing the original Pinatubo data. I also thank Gale Cox, Paul Friberg, and the ObsPy development community for insights and tooling essential to this work. This project was conducted as part of ongoing research at the University of South Florida.

# References

\nocite{*}