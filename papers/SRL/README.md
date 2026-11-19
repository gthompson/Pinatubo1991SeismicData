# SRL manuscript

`pinatubo_srl.tex` is the Seismological Research Letters manuscript, written with the SSA template (`SSA-SRL.cls`, `SSAbib.bst`, `SSA_orcid_logo_Web.pdf`, from SSA's "LaTeX suggestions" page, September 2025). It supersedes the Word draft `Pinatubo-data-paper-draft-v2` (Box, `writing/latest`, and its 18 Nov markdown copy in `writing/202610_version`).

Build:

```bash
latexmk -pdf pinatubo_srl.tex
```

`SSAbib.bst` has one local change: SSA's distributed style ignores the `doi` field, so `fin.entry` now appends ", doi: <doi>" to every entry that has one.

The SSA class needs the `stix2-type1` and `ifsym` packages; on a TeX Live basic install they can be added with `tlmgr --usermode install stix2-type1 ifsym`.

Figures are in `figures/` and are all made by `make_figures.ipynb`; none is edited by hand. Rerun it after the conversion and analysis notebooks, then rebuild the PDF. It draws the clock rules, timing effect, clock anchors (step 72's `plot_records`), station availability, hypocenters, daily counts and the June worked example from the UTC products and analysis run `45e939498e85`; draws the network maps and PVO system diagram with step 70's `maps()` and `diagrams()` with `notes=False` (no title or footnote; the caveats are in the captions); and copies the BOB coverage and RSAM–SSAM lag figures from step 69. Its last cell checks that every figure the `.tex` file includes exists.

Before submission:
- SSA requires every file in one folder: copy the figures beside the `.tex` file and paste the `.bbl` into the `.tex` file (see SSA's LaTeX instructions).
- Entries marked `VERIFY` in `pinatubo.bib` need checking against the sources.
- Numbers come from the conversion products and analysis run `45e939498e85`; the overnight rerun (with the 25 s clock uncertainty) should be checked against them.
