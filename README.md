# Eurostat SDG Extractor

Reusable Eurostat SDG extraction script for country comparison assignments.

It uses official Eurostat programmatic access only, keeps a fixed indicator-selection logic, and lets each person choose different countries without rewriting the code.

## What this script does

- Queries official Eurostat SDMX structure metadata first
- Pulls indicator data from the official Eurostat JSON-stat API
- Extracts one headline indicator per SDG where practical
- Adds extra diagnostic indicators for SDGs 3, 7, 8, 10, 12, 13, and 15
- Exports tidy data, metadata, a method note, and an extraction log
- Logs missing values and missing geography coverage explicitly

## Requirements

- Python 3.10 or newer
- Internet access to `ec.europa.eu`

No third-party Python packages are required.

## Main file

- `eurostat_sdg_extract_cli.py`

## Quick start

Run the default comparison:

```bash
python3 eurostat_sdg_extract_cli.py
```

Default geography set:

- `ES` = Spain
- `SE` = Sweden
- `EU27_2020` = EU27

## Use different countries

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

Example: Netherlands vs Belgium vs Germany from 2018 onward

```bash
python3 eurostat_sdg_extract_cli.py \
  --countries NL,BE,DE \
  --labels Netherlands,Belgium,Germany \
  --start-year 2018 \
  --output-stem netherlands_belgium_germany_sdg_comparison
```

## Arguments

- `--countries`: comma-separated Eurostat geo codes
- `--labels`: optional comma-separated display names in the same order
- `--start-year`: first year to include, default `2015`
- `--output-dir`: output folder, default `output`
- `--output-stem`: base name for the main CSV/XLSX files, default `sdg_comparison`

## Output files

For a run with `--output-dir output --output-stem germany_france_sdg_comparison`, the script writes:

- `output/germany_france_sdg_comparison.csv`
- `output/germany_france_sdg_comparison.xlsx`
- `output/indicator_metadata.csv`
- `output/method_note.md`
- `output/extraction_log.txt`

The Excel workbook contains:

- `data`: tidy long-format observations
- `latest_summary`: latest comparable snapshot by selected indicator
- `metadata`: indicator selection, filters, latest-year coverage, and notes

## What the outputs mean

- `*.csv`: main tidy table with one row per indicator-country-year
- `*.xlsx`: spreadsheet version with summary and metadata tabs
- `indicator_metadata.csv`: selected datasets, applied filters, and ambiguity notes
- `method_note.md`: plain-language summary of scope, logic, and caveats
- `extraction_log.txt`: request and missing-data log for reproducibility

## Indicator selection logic

The script keeps a fixed selection logic so everyone using the repo is working from the same Eurostat datasets unless they deliberately edit the code.

Current selection design:

- 1 headline indicator per SDG from SDG 1 to SDG 17
- extra diagnostic indicators for SDGs 3, 7, 8, 10, 12, 13, and 15

Some SDGs do not have an unambiguous single “headline” series in Eurostat’s API metadata. In those cases the choice is documented explicitly in:

- `indicator_metadata.csv`
- `method_note.md`

## Recommended workflow for classmates

1. Fork or clone the repo.
2. Run the script with your chosen country codes.
3. Inspect `indicator_metadata.csv` and `method_note.md` before writing up results.
4. Use the `latest_summary` sheet in the XLSX for quick report tables.
5. Use the tidy CSV if you want to build charts or do additional analysis in Python, R, or Excel.

## Notes and limitations

- Eurostat coverage varies by indicator and geography.
- The script does not invent values.
- Missing values and failed geography coverage are logged explicitly.
- Latest available years differ across indicators.
- A geography such as `EU27_2020` may be available for some datasets and absent for others.
- The script is designed for reproducibility, not for scraping the Eurostat browser UI.
