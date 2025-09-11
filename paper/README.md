# Research Paper Scaffold

This folder contains a LaTeX scaffold for writing a research paper describing the codebase, methodology, and findings from this repository.

## Structure

- `main.tex`: Entry point that compiles the paper.
- `sections/`: Modular section files to keep writing focused.
- `refs.bib`: Bibliography file for citations.
- `figures/`: Put figures here (kept with a `.gitkeep`).

## Build

Option 1: latexmk (recommended)

```
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
```

Option 2: pdflatex + bibtex

```
pdflatex main.tex
bibtex main
pdflatex main.tex
pdflatex main.tex
```

If you use VS Code LaTeX Workshop, open `main.tex` and run `Build LaTeX project`.

## Notes

- Replace author/title and tweak the template as needed for your venue.
- Figures are referenced from `figures/`; remember to export any schema or pipeline diagrams here.
- Citations are placeholders; update `refs.bib` with accurate metadata.

