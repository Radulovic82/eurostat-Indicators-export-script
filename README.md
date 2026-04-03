# Eurostat SDG Extractor

Shareable, rerunnable extractor for Eurostat SDG indicators using official Eurostat programmatic access only.

It is designed for assignments where different people may want to compare different countries while keeping the same SDG indicator-selection logic.

## What it does

- Queries Eurostat SDMX structure metadata first
- Pulls JSON-stat data from the official Eurostat API
- Extracts one headline indicator per SDG where practical
- Adds diagnostic indicators for SDGs 3, 7, 8, 10, 12, 13, and 15
- Writes tidy outputs plus metadata, method notes, and an extraction log
- Logs missing values and missing geography coverage explicitly

## Files

- `eurostat_sdg_extract_cli.py`: main script
- `output/*.csv`, `output/*.xlsx`, `output/*.md`, `output/*.txt`: generated outputs

## Requirements

- Python 3.10+
- Internet access to `ec.europa.eu`

No third-party Python packages are required.

## Quick start

Default run:

```bash
python3 eurostat_sdg_extract_cli.py
```

This uses:

- `ES` = Spain
- `SE` = Sweden
- `EU27_2020` = EU27

## Choose different countries

Example: Germany vs France vs EU27

```bash
python3 eurostat_sdg_extract_cli.py \
  --countries DE,FR,EU27_2020 \
  --labels Germany,France,EU27 \
  --output-stem germany_france_sdg_comparison
```

Example: Italy vs Portugal

```bash
python3 eurostat_sdg_extract_cli.py \
  --countries IT,PT \
  --labels Italy,Portugal \
  --output-stem italy_portugal_sdg_comparison
```

## Arguments

- `--countries`: comma-separated Eurostat geo codes
- `--labels`: optional comma-separated display names matching the same order
- `--start-year`: first year to include, default `2015`
- `--output-dir`: output folder, default `output`
- `--output-stem`: base name for the main CSV/XLSX, default `sdg_comparison`

## Outputs

For a run with `--output-dir output --output-stem germany_france_sdg_comparison`, the script writes:

- `output/germany_france_sdg_comparison.csv`
- `output/germany_france_sdg_comparison.xlsx`
- `output/indicator_metadata.csv`
- `output/method_note.md`
- `output/extraction_log.txt`

The XLSX contains three sheets:

- `data`
- `latest_summary`
- `metadata`

## Indicator logic

The selection logic is intentionally fixed in code so everyone on the assignment is using the same dataset choices unless they explicitly edit them.

Current selection:

- Headlines: one per SDG from SDG 1 to SDG 17
- Diagnostics: extra series for SDGs 3, 7, 8, 10, 12, 13, and 15

Some SDGs have ambiguity notes where Eurostat metadata does not explicitly mark a single “headline” series. Those notes are written to:

- `indicator_metadata.csv`
- `method_note.md`

## Recommended repo structure

If you want to share this with colleagues, putting this folder in a fresh Git repo is the right move.

Minimal repo structure:

```text
eurostat-sdg-extractor/
  README.md
  eurostat_sdg_extract_cli.py
  output/
```

Suggested workflow:

1. Create a new repo.
2. Commit the script and README.
3. Do not commit generated output unless you want example results in the repo.
4. Let colleagues fork it and run their own country combinations.

## Suggested `.gitignore`

If you create a repo, this is a sensible starting point:

```gitignore
__pycache__/
*.pyc
output/
```

## Notes

- Eurostat coverage varies by indicator and geography.
- The script does not invent values.
- Missing values and missing country coverage are logged explicitly.
- Some series have different latest available years across countries.
- In some cases a country group such as `EU27_2020` may be absent for a given dataset even when national data exist.
