# SRL manuscript

`pinatubo_srl.tex` is the Seismological Research Letters manuscript, written with the SSA template (`SSA-SRL.cls`, `SSAbib.bst`, `SSA_orcid_logo_Web.pdf`, from SSA's "LaTeX suggestions" page, September 2025). It supersedes the Word draft `Pinatubo-data-paper-draft-v2` (Box, `writing/latest`, and its 18 Nov markdown copy in `writing/202610_version`).

Build:

```bash
latexmk -pdf pinatubo_srl.tex
```

The SSA class needs the `stix2-type1` and `ifsym` packages; on a TeX Live basic install they can be added with `tlmgr --usermode install stix2-type1 ifsym`.

Figures are in `figures/`. New figures were drawn from the UTC products (`~/work/Pinatubo1991Processing/utc`) on 19 Nov 2026: clock rules, the timing effect, daily counts, hypocenters, station availability and the clock-anchor records (step 72). The BOB coverage and RSAM–SSAM lag figures are step 69 outputs. The network maps, PVO system diagram and helicorder figure are carried over from the Word draft.

Before submission:
- SSA requires every file in one folder: copy the figures beside the `.tex` file and paste the `.bbl` into the `.tex` file (see SSA's LaTeX instructions).
- Entries marked `VERIFY` in `pinatubo.bib` need checking against the sources.
- Numbers come from the conversion products and analysis run `45e939498e85`; the overnight rerun (with the 25 s clock uncertainty) should be checked against them.
