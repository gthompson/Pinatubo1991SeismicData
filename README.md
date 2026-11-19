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

<pre>
✔ Jupyter notebooks that convert the whole surviving archive, step by step (10_conversion)
✔ MiniSEED (SEISAN WAV), StationXML, QuakeML, SEISAN REA and Antelope CSS3.0 outputs
✔ A unified event catalogue that records which of waveforms, picks and hypocentres survive for each event
✔ UTC times throughout, with the clock rule and uncertainty recorded for every record
✔ Tabular BOB monitoring products (RSAM, SSAM, tilt, rain, mudflow, COSPEC, counts)
✔ Analysis notebooks for the converted archive (20_analysis)
</pre>

Generic readers and writers for VDAP data (DMX, PHA, HYPO71, BOB, Nordic REA) live in
[FloVoPy](https://github.com/gthompson/flovopy); Pinatubo-specific choices stay in this repository.

A companion JOSS submission will document the pipeline as research-grade software.

⸻

## Goals of This Repository
	•	Preserve the most complete digital representation of the 1991 Pinatubo seismic dataset
	•	Enable modern waveform and catalog analysis (QuakeML, MiniSEED, SEISAN)
	•	Support machine learning, clustering, and new interpretations
	•	Document the full provenance and reproducibility of the reconstruction
	•	Prepare the dataset for DOI-based archiving (Zenodo + EarthScope/ScienceBase)

⸻

## Repository Structure
<pre>
Pinatubo1991SeismicData/
│
├── README.md, LICENSE, CITATION.cff
├── CONFIGURATION.md           # Input/output locations and the UTC time basis
├── RESULTS_LAYOUT.md          # External output layout
├── environment.yml
├── run_notebooks.py           # Runs the numbered notebooks in order (conversion, analysis, paper figures)
│
├── 10_conversion/             # Conversion notebooks, steps 10–72 (see NOTEBOOKS_README.md)
│   ├── conversion_config.json # data_top, output_root, time_basis, clock_regimes
│   ├── clock_regimes.csv      # Clock rule per data stream and period
│   ├── TIMING_AND_CLOCKS.md   # Evidence for the clock rules
│   ├── CODE_REVIEW_2026-11-17.md
│   ├── lib/                   # Small shared helpers (paths, clocks, IDs, PHA wrapper) and tests
│   └── tests/                 # BOB notebook and product tests
│
├── 20_analysis/               # Analysis notebooks (see ANALYSIS_NOTEBOOKS.md)
├── papers/                    # Manuscripts (SRL in LaTeX, JOSS)
└── tests/                     # Configuration tests
</pre>

Generated data are written outside the repository (see [CONFIGURATION.md](CONFIGURATION.md)).

⸻

## Installation

Create a conda environment with Python ≥ 3.11, ObsPy, NumPy, SciPy, pandas, Matplotlib,
pyarrow, JupyterLab and pytest (`environment.yml` lists these), then install FloVoPy from a
checkout in editable mode. FloVoPy's own `flovopy_asl.yml` builds the fuller environment
the pipeline is developed and run in; either works.

```bash
mamba env create -f environment.yml
```

```bash
mamba activate pinatubo_fair
```

```bash
pip install -e ../flovopy
```

⸻

## Pipeline Overview

The conversion runs as numbered notebooks in `10_conversion`; the full table of steps,
inputs and products is in [10_conversion/NOTEBOOKS_README.md](10_conversion/NOTEBOOKS_README.md).

- **10–11 Waveforms.** Convert the 21,475 surviving DMX files to MiniSEED in a SEISAN WAV
  database (IRIG traces removed, 2048-count offset subtracted, legacy station IDs mapped,
  damaged files partly recovered).
- **20–35 Picks.** Parse the individual-event and monthly PHA phase files, merge them, and
  associate picks with waveform files.
- **40–45 Hypocentres.** Parse `Pinatubo_all.sum` and `PINAALL.DAT`, compare them and group
  their origins.
- **50–54 Catalogue.** Build one ObsPy catalogue whose events record which of waveforms (W),
  picks (P) and hypocentre (H) survive, and export it to QuakeML, SEISAN REA and Antelope CSS3.0.
- **60–69 BOB.** Unpack and convert the BOB monitoring products and summarise their coverage.
- **70–71 Network metadata.** Rebuild the network history and StationXML once every dataset is on UTC, then validate them.

- **72 Clock anchors.** Check absolute time against regional and teleseismic arrivals.

The analysis notebooks in `20_analysis` (00–60) then measure noise and event windows on the
UTC archive, and `papers/SRL/make_figures.ipynb` and `papers/SRL/supplement/make_supplement.ipynb`
make the paper and supplement figures. To run everything in
order, executing each notebook in place:

```bash
python run_notebooks.py --figures
```

`--dry-run` lists the notebooks; `--stage`, `--from`, `--to` and `--skip` select a subset; the
run stops at the first failing notebook unless `--keep-going` is given. A log and a CSV summary
are written to `run_logs/` beside the conversion output root.

Every time is converted to UTC where it is read. Each acquisition PC had its own clock,
switched between Philippine time and GMT during 1991; the rules are in
`10_conversion/clock_regimes.csv` and the evidence in `10_conversion/TIMING_AND_CLOCKS.md`.

⸻

## Data directories

`conversion_config.json` sets `data_top`, the archive to read, and `output_root`, where
everything is written. The legacy inputs are expected under `data_top`:
<pre>
data_top/
└── LEGACY/                               [LEGACY_TOP]
    ├── WAVEFORM_DATA/SUDS/.../*.DMX      Legacy waveform files
    ├── EVENT_METADATA/
    │   ├── PHA/                          Individual-event phase files
    │   ├── MONTHLY_PHA/                  Monthly phase files
    │   └── HYPOCENTERS/                  Pinatubo_all.sum, PINAALL.DAT
    ├── STATION_METADATA/NETWORK_HISTORY/ Station evidence for step 70
    └── BOB/                              BOB monitoring products
</pre>
Outputs go under `output_root`:
<pre>
output_root/
├── FAIR/
│   ├── SEISAN/WAV/PNTBO/YYYY/MM/         MiniSEED waveforms
│   ├── SEISAN/REA/YYYY/MM/               Nordic S-files
│   ├── metadata/                         Catalogue (QuakeML, pickle), StationXML, network history
│   └── BOB/<product>/                    BOB products and QC
└── pipeline_tmp/                         Indexes, logs and QC between steps
</pre>

⸻

## Citation

Repository:

Thompson, G. (2025). Pinatubo1991SeismicData: FAIR Reconstruction of the 1991 Pinatubo Seismic Archive. GitHub Repository.

JOSS (in preparation):
A citation entry will be provided once the JOSS article is published.

⸻

## Testing

From `10_conversion` (conversion notebooks, clock rules, parsers, exporters, BOB products):

```bash
python -m pytest -q lib/tests tests
```

From the repository root (configuration checks):

```bash
python -m unittest discover -s tests -p test_pipeline_configuration.py -v
```

⸻

## License

An MIT License has been added.

⸻

## Acknowledgments

This project builds on data collected by PHIVOLCS and the USGS Volcano Disaster Assistance Program (VDAP) during the 1991 eruption.
Special thanks to colleagues who assisted with data discovery, scanning, metadata recovery, and historical insights.




## Notebook configuration and generated outputs

See [CONFIGURATION.md](CONFIGURATION.md). Conversion reads `10_conversion/conversion_config.json`; current analysis reads `20_analysis/analysis_workflow.json`. Derived data, caches and figures are routed outside this Git checkout. Existing in-repository products are not moved or removed automatically.
