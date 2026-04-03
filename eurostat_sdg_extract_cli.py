#!/usr/bin/env python3
"""
Shareable Eurostat SDG extractor.

This is a repo-ready, parameterized version of the one-off extractor.
It uses official Eurostat programmatic access only and supports choosing
different countries from the command line.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


STRUCTURE_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT"
DATA_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
USER_AGENT = "eurostat-sdg-extractor/1.0 (+official-api-only)"
DEFAULT_START_YEAR = 2015
DEFAULT_GEOS = [("ES", "Spain"), ("SE", "Sweden"), ("EU27_2020", "EU27")]

NS = {
    "m": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


@dataclass(frozen=True)
class Selection:
    sdg_number: int
    sdg_code: str
    role: str
    dataset_id: str
    preferred_filters: dict[str, list[str]]
    rationale: str
    ambiguity_note: str = ""


SELECTIONS: list[Selection] = [
    Selection(1, "SDG 01", "headline", "SDG_01_10", {"age": ["TOTAL"], "unit": ["PC"]}, "Representative headline indicator for poverty and social exclusion."),
    Selection(2, "SDG 02", "headline", "SDG_02_40", {"unit": ["PC_UAA"], "crops": ["UAAXK0000"], "agprdmet": ["TOTAL"]}, "Selected for annual country comparability and clean country/EU coverage.", "Headline identification is not explicit in Eurostat API metadata; this uses a pragmatic annual comparison series."),
    Selection(3, "SDG 03", "headline", "SDG_03_11", {"unit": ["YR"], "sex": ["T"], "indic_he": ["HLY_0"]}, "Core health outcome indicator with full annual coverage."),
    Selection(3, "SDG 03", "diagnostic", "SDG_03_60", {"age": ["Y_GE16"], "sex": ["T"], "reason": ["TOOEFW"], "unit": ["PC"], "quantile": ["TOTAL"]}, "Diagnostic access-to-care indicator requested for SDG 3."),
    Selection(4, "SDG 04", "headline", "SDG_04_10", {"unit": ["PC"], "wstatus": ["POP"], "age": ["Y18-24"], "sex": ["T"]}, "Representative education headline indicator."),
    Selection(5, "SDG 05", "headline", "SDG_05_50", {"org_inst": ["PARL_NAT"], "sex": ["F"], "unit": ["PC_WMN"]}, "Selected as a stable annual gender representation indicator.", "Dataset contains both parliament and government variants; parliament share was chosen for a single comparable headline series."),
    Selection(6, "SDG 06", "headline", "SDG_06_20", {"ww_tp": ["WWT_GE2"], "unit": ["PC"]}, "Representative water and sanitation outcome indicator."),
    Selection(7, "SDG 07", "headline", "SDG_07_10", {"unit": ["TOE_HAB"]}, "Per-capita primary energy consumption chosen for cross-country comparability.", "Eurostat exposes total, index, and per-capita units; per-capita was selected for country comparison."),
    Selection(7, "SDG 07", "diagnostic", "SDG_07_40", {"nrg_bal": ["REN"], "unit": ["PC"]}, "Diagnostic renewables indicator requested for SDG 7."),
    Selection(8, "SDG 08", "headline", "SDG_08_10", {"unit": ["CLV20_EUR_HAB"], "na_item": ["B1GQ"]}, "Real GDP per capita selected as the main growth and prosperity comparison."),
    Selection(8, "SDG 08", "diagnostic", "SDG_08_30", {"indic_em": ["EMP_LFS"], "sex": ["T"], "age": ["Y20-64"], "unit": ["PC_POP"]}, "Diagnostic labour-market indicator requested for SDG 8."),
    Selection(9, "SDG 09", "headline", "SDG_09_10", {"sectperf": ["TOTAL"], "unit": ["PC_GDP"]}, "Representative innovation and infrastructure indicator."),
    Selection(10, "SDG 10", "headline", "SDG_10_41", {"age": ["TOTAL"], "sex": ["T"], "unit": ["RAT"]}, "Distributional inequality indicator chosen as the main reduced-inequalities series.", "This does not follow the first listed Goal 10 dataset; it was chosen because it better reflects within-country inequality."),
    Selection(10, "SDG 10", "diagnostic", "SDG_10_50", {"indic_il": ["SHARE"]}, "Diagnostic bottom-40-percent income share requested for SDG 10."),
    Selection(11, "SDG 11", "headline", "SDG_11_60", {"unit": ["PC"]}, "Selected for broad annual comparability.", "Goal 11 headline choice is not explicit in the API metadata; recycling rate was selected as a consistent annual series."),
    Selection(12, "SDG 12", "headline", "SDG_12_41", {"unit": ["PC"]}, "Representative circular-economy headline indicator."),
    Selection(12, "SDG 12", "diagnostic", "SDG_12_21", {"indic_env": ["RMC"], "material": ["TOTAL"], "unit": ["T_HAB"]}, "Diagnostic resource-use indicator requested for SDG 12."),
    Selection(13, "SDG 13", "headline", "SDG_13_40", {"statinfo": ["VAL_A"], "unit": ["EUR_HAB_KP"], "stk_flow": ["LOSS"]}, "Chosen as the main SDG 13 comparison because Eurostat currently provides broad comparable coverage.", "The more canonical emissions dataset SDG_13_10 does not always expose a full comparable geography set in the same payload, so climate-related economic losses was used for the headline comparison."),
    Selection(13, "SDG 13", "diagnostic", "SDG_13_10", {"src_crf": ["TOTXMEMO"], "unit": ["T_HAB"]}, "Diagnostic emissions indicator requested for SDG 13."),
    Selection(14, "SDG 14", "headline", "SDG_14_10", {"areaprot": ["MPA"], "unit": ["PC"]}, "Representative oceans and marine protection indicator."),
    Selection(15, "SDG 15", "headline", "SDG_15_11", {"indic_fo": ["FOR"], "unit": ["PC"]}, "Representative terrestrial ecosystems indicator."),
    Selection(15, "SDG 15", "diagnostic", "SDG_15_20", {"areaprot": ["TPA"], "unit": ["PC"]}, "Diagnostic protected-land indicator requested for SDG 15."),
    Selection(16, "SDG 16", "headline", "SDG_16_50", {"unit": ["SC"]}, "Selected as a broad governance and institutions indicator.", "Goal 16 offers several justice and safety series; the Corruption Perceptions Index was chosen as a single headline comparator."),
    Selection(17, "SDG 17", "headline", "SDG_17_10", {"partner": ["ODA"], "unit": ["PC_GNI"]}, "Selected as the direct partnerships-for-development measure.", "Alternative Goal 17 series exist for debt, tax, trade, and connectivity; ODA share of GNI was chosen as the headline partnerships metric."),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Eurostat SDG indicators for selected countries.")
    parser.add_argument("--countries", default="ES,SE,EU27_2020", help="Comma-separated Eurostat geo codes. Default: ES,SE,EU27_2020")
    parser.add_argument("--labels", default="", help="Optional comma-separated display labels matching --countries order.")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR, help="First year to include. Default: 2015")
    parser.add_argument("--output-dir", default="output", help="Directory for generated files. Default: output")
    parser.add_argument("--output-stem", default="sdg_comparison", help="Base filename stem for CSV/XLSX outputs. Default: sdg_comparison")
    return parser.parse_args()


def parse_geos(countries_arg: str, labels_arg: str) -> list[tuple[str, str]]:
    codes = [item.strip() for item in countries_arg.split(",") if item.strip()]
    if not codes:
        raise SystemExit("No countries provided.")
    if labels_arg.strip():
        labels = [item.strip() for item in labels_arg.split(",")]
        if len(labels) != len(codes):
            raise SystemExit("--labels must have the same number of items as --countries.")
        return list(zip(codes, labels))
    defaults = dict(DEFAULT_GEOS)
    return [(code, defaults.get(code, code)) for code in codes]


def ensure_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


def log(log_path: Path, message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{timestamp}] {message}"
    print(line)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def http_get(url: str, expect: str = "json", retries: int = 3, sleep_seconds: float = 1.5) -> Any:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json" if expect == "json" else "application/xml,text/xml;q=0.9,*/*;q=0.1",
    }
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=90) as response:
                payload = response.read()
            if expect == "json":
                return json.loads(payload.decode("utf-8"))
            if expect == "xml":
                return ET.fromstring(payload)
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(sleep_seconds * attempt if attempt < retries else 0)
    raise RuntimeError(f"Failed to retrieve {url}: {last_error}")


def parse_structure(dataset_id: str) -> dict[str, str]:
    url = f"{STRUCTURE_BASE}/{dataset_id}/1.0?references=children"
    root = http_get(url, expect="xml")
    dataflow_name = ""
    datastructure_id = ""
    datastructure_version = ""
    dataflow = root.find(".//s:Dataflow", NS)
    if dataflow is not None:
        name_nodes = dataflow.findall("c:Name", NS)
        preferred_name = None
        fallback_name = None
        for node in name_nodes:
            text = (node.text or "").strip()
            if not text:
                continue
            fallback_name = fallback_name or text
            if node.attrib.get("{http://www.w3.org/XML/1998/namespace}lang", "").lower() == "en":
                preferred_name = text
                break
        dataflow_name = preferred_name or fallback_name or ""
        ref = dataflow.find(".//Ref", NS)
        if ref is not None:
            datastructure_id = ref.attrib.get("id", "")
            datastructure_version = ref.attrib.get("version", "")
    return {
        "structure_url": url,
        "structure_dataset_label": dataflow_name,
        "datastructure_id": datastructure_id,
        "datastructure_version": datastructure_version,
    }


def build_data_url(dataset_id: str, geos: list[tuple[str, str]], start_year: int) -> str:
    params = [("geo", code) for code, _ in geos]
    params.append(("sinceTimePeriod", str(start_year)))
    return f"{DATA_BASE}/{dataset_id}?{urllib.parse.urlencode(params)}"


def ordered_codes(category: dict[str, Any]) -> list[str]:
    index = category.get("index", {})
    if isinstance(index, list):
        return list(index)
    return [code for code, _ in sorted(index.items(), key=lambda item: item[1])]


def choose_code(dataset_id: str, dim_name: str, available: list[str], preferred: list[str] | None, log_path: Path) -> str:
    if preferred:
        for code in preferred:
            if code in available:
                return code
        log(log_path, f"{dataset_id}: none of preferred values {preferred} were available for dimension {dim_name}; falling back to {available[0]!r}")
        return available[0]
    if len(available) == 1:
        return available[0]
    raise RuntimeError(f"{dataset_id}: ambiguous dimension {dim_name} with available codes {available} and no preference provided")


def jsonstat_position(indices: list[int], sizes: list[int]) -> int:
    position = 0
    for i, idx in enumerate(indices):
        multiplier = 1
        for size in sizes[i + 1 :]:
            multiplier *= size
        position += idx * multiplier
    return position


def build_availability_note(latest_by_geo: dict[str, tuple[int | None, Any]], geos: list[tuple[str, str]], latest_common_year: int | None) -> str:
    pieces = []
    for geo_code, geo_name in geos:
        latest_year, _ = latest_by_geo[geo_code]
        pieces.append(f"{geo_name}: {latest_year if latest_year is not None else 'no data'}")
    pieces.append(f"common_latest_year: {latest_common_year if latest_common_year is not None else 'none'}")
    return "; ".join(pieces)


def fetch_series(selection: Selection, structure_meta: dict[str, str], geos: list[tuple[str, str]], start_year: int, log_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    data_url = build_data_url(selection.dataset_id, geos, start_year)
    payload = http_get(data_url, expect="json")
    dims = payload["id"]
    sizes = payload["size"]
    dim_info: dict[str, dict[str, Any]] = {}
    for dim in dims:
        meta = payload["dimension"][dim]
        category = meta["category"]
        codes = ordered_codes(category)
        dim_info[dim] = {
            "label": meta.get("label", dim),
            "codes": codes,
            "code_to_index": {code: idx for idx, code in enumerate(codes)},
            "labels": category.get("label", {}),
        }

    selected_codes: dict[str, str] = {}
    filter_labels: list[str] = []
    for dim in dims:
        if dim in {"geo", "time"}:
            continue
        code = choose_code(selection.dataset_id, dim, dim_info[dim]["codes"], selection.preferred_filters.get(dim), log_path)
        selected_codes[dim] = code
        filter_labels.append(f"{dim_info[dim]['label']}: {dim_info[dim]['labels'].get(code, code)}")

    dataset_label = payload.get("label", structure_meta.get("structure_dataset_label", selection.dataset_id))
    series_label = dataset_label if not filter_labels else f"{dataset_label} [{'; '.join(filter_labels)}]"

    values = payload.get("value", {})
    times = [int(code) for code in dim_info["time"]["codes"] if str(code).isdigit()]
    dataset_latest_year = max(times) if times else None

    rows: list[dict[str, Any]] = []
    latest_by_geo: dict[str, tuple[int | None, float | int | None]] = {geo: (None, None) for geo, _ in geos}
    observed_years_by_geo: dict[str, list[int]] = {geo: [] for geo, _ in geos}

    for geo_code, geo_name in geos:
        for year in range(start_year, (dataset_latest_year or start_year) + 1):
            missing_reason = ""
            value = None
            if str(year) not in dim_info["time"]["code_to_index"]:
                missing_reason = "year_not_in_dataset_dimension"
            elif geo_code not in dim_info["geo"]["code_to_index"]:
                missing_reason = "geo_not_in_dataset_dimension"
            else:
                coords: list[int] = []
                for dim in dims:
                    if dim == "geo":
                        code = geo_code
                    elif dim == "time":
                        code = str(year)
                    else:
                        code = selected_codes[dim]
                    coords.append(dim_info[dim]["code_to_index"][code])
                pos = jsonstat_position(coords, sizes)
                value = values.get(str(pos))
                if value is None:
                    missing_reason = "missing_in_payload"

            if value is not None:
                observed_years_by_geo[geo_code].append(year)
                latest_year, _ = latest_by_geo[geo_code]
                if latest_year is None or year >= latest_year:
                    latest_by_geo[geo_code] = (year, value)

            rows.append(
                {
                    "sdg_number": selection.sdg_number,
                    "sdg_code": selection.sdg_code,
                    "selection_role": selection.role,
                    "dataset_id": selection.dataset_id,
                    "dataset_label": dataset_label,
                    "series_label": series_label,
                    "geo_code": geo_code,
                    "geo_name": geo_name,
                    "year": year,
                    "value": value,
                    "missing": "yes" if value is None else "no",
                    "missing_reason": missing_reason,
                    "unit_code": selected_codes.get("unit", ""),
                    "unit_label": dim_info["unit"]["labels"].get(selected_codes["unit"], "") if "unit" in selected_codes else "",
                    "applied_filters": json.dumps(selected_codes, sort_keys=True),
                    "data_updated": payload.get("updated", ""),
                    "source_url": data_url,
                }
            )

    common_years = None
    for geo_code, _ in geos:
        years = set(observed_years_by_geo[geo_code])
        common_years = years if common_years is None else common_years.intersection(years)
    common_years = sorted(common_years or [])
    latest_common_year = common_years[-1] if common_years else None

    common_values: dict[str, Any] = {}
    if latest_common_year is not None:
        for geo_code, _ in geos:
            common_values[geo_code] = next(
                (row["value"] for row in rows if row["geo_code"] == geo_code and row["year"] == latest_common_year and row["value"] is not None),
                None,
            )

    meta: dict[str, Any] = {
        "sdg_number": selection.sdg_number,
        "sdg_code": selection.sdg_code,
        "selection_role": selection.role,
        "dataset_id": selection.dataset_id,
        "dataset_label": dataset_label,
        "series_label": series_label,
        "rationale": selection.rationale,
        "ambiguity_note": selection.ambiguity_note,
        "preferred_filters": json.dumps(selection.preferred_filters, sort_keys=True),
        "applied_filters": json.dumps(selected_codes, sort_keys=True),
        "filter_labels": " | ".join(filter_labels),
        "data_url": data_url,
        "data_updated": payload.get("updated", ""),
        "dataset_latest_year": dataset_latest_year,
        "latest_common_year": latest_common_year,
        "availability_note": build_availability_note(latest_by_geo, geos, latest_common_year),
        **structure_meta,
    }
    for geo_code, _geo_name in geos:
        safe = geo_code.lower().replace("-", "_")
        meta[f"latest_year_{safe}"] = latest_by_geo[geo_code][0]
        meta[f"latest_value_{safe}"] = latest_by_geo[geo_code][1]
        meta[f"common_value_{safe}"] = common_values.get(geo_code)
    return rows, meta


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def xlsx_escape(value: str) -> str:
    return escape(value).replace("\r", "&#13;")


def excel_col(index: int) -> str:
    result = ""
    current = index
    while current > 0:
        current, rem = divmod(current - 1, 26)
        result = chr(65 + rem) + result
    return result


def make_sheet_xml(rows: list[list[Any]]) -> str:
    xml_rows: list[str] = []
    for row_idx, row in enumerate(rows, start=1):
        cells: list[str] = []
        for col_idx, value in enumerate(row, start=1):
            if value is None or value == "":
                continue
            ref = f"{excel_col(col_idx)}{row_idx}"
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    continue
                cells.append(f'<c r="{ref}"><v>{value}</v></c>')
            else:
                text = xlsx_escape(str(value))
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>')
        xml_rows.append(f'<row r="{row_idx}">{"".join(cells)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
        + "".join(xml_rows)
        + "</sheetData></worksheet>"
    )


def write_xlsx(path: Path, sheets: list[tuple[str, list[dict[str, Any]], list[str]]]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        content_types = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
            '<Default Extension="xml" ContentType="application/xml"/>',
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
            '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
            '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
            '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
        ]
        workbook_rels = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
        ]
        workbook_sheets = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>',
        ]
        app_titles = []

        for idx, (sheet_name, rows, columns) in enumerate(sheets, start=1):
            content_types.append(f'<Override PartName="/xl/worksheets/sheet{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>')
            workbook_rels.append(f'<Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>')
            workbook_sheets.append(f'<sheet name="{xlsx_escape(sheet_name[:31])}" sheetId="{idx}" r:id="rId{idx}"/>')
            matrix = [columns] + [[row.get(column, "") for column in columns] for row in rows]
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", make_sheet_xml(matrix))
            app_titles.append(f"<vt:lpstr>{xlsx_escape(sheet_name[:31])}</vt:lpstr>")

        content_types.append("</Types>")
        workbook_rels.append(f'<Relationship Id="rId{len(sheets)+1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>')
        workbook_rels.append("</Relationships>")
        workbook_sheets.append("</sheets></workbook>")

        created = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        root_rels = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
            "</Relationships>"
        )
        styles = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
            '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
            '<borders count="1"><border/></borders>'
            '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
            '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
            '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
            "</styleSheet>"
        )
        core = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
            "<dc:title>Eurostat SDG comparison</dc:title><dc:creator>Codex</dc:creator>"
            f'<dcterms:created xsi:type="dcterms:W3CDTF">{created}</dcterms:created>'
            f'<dcterms:modified xsi:type="dcterms:W3CDTF">{created}</dcterms:modified>'
            "</cp:coreProperties>"
        )
        app = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
            "<Application>Python</Application>"
            f"<TitlesOfParts><vt:vector size=\"{len(app_titles)}\" baseType=\"lpstr\">{''.join(app_titles)}</vt:vector></TitlesOfParts>"
            f"<HeadingPairs><vt:vector size=\"2\" baseType=\"variant\"><vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant><vt:variant><vt:i4>{len(app_titles)}</vt:i4></vt:variant></vt:vector></HeadingPairs>"
            "</Properties>"
        )

        zf.writestr("[Content_Types].xml", "".join(content_types))
        zf.writestr("_rels/.rels", root_rels)
        zf.writestr("xl/workbook.xml", "".join(workbook_sheets))
        zf.writestr("xl/_rels/workbook.xml.rels", "".join(workbook_rels))
        zf.writestr("xl/styles.xml", styles)
        zf.writestr("docProps/core.xml", core)
        zf.writestr("docProps/app.xml", app)


def extract_unit_label_from_filters(filter_labels: str) -> str:
    for part in filter_labels.split(" | "):
        if part.startswith("Unit of measure: "):
            return part.split(": ", 1)[1]
    return ""


def write_method_note(path: Path, metadata_rows: list[dict[str, Any]], geos: list[tuple[str, str]], start_year: int) -> None:
    geo_text = ", ".join(f"{label} (`{code}`)" for code, label in geos)
    lines = [
        "# Eurostat SDG extraction method note",
        "",
        "## Scope",
        "",
        "This extraction uses official Eurostat programmatic access only.",
        "",
        "- Structure metadata endpoint: `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/{DATASET_ID}/1.0?references=children`",
        "- Data endpoint: `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{DATASET_ID}`",
        f"- Geography filter: {geo_text}",
        f"- Time filter: {start_year} to latest year available in each selected dataset",
        "",
        "The script queries structure metadata first, then requests JSON-stat data, selects one series per configured indicator using explicit dimension preferences, and writes missing observations explicitly as blank values with a `missing_reason`.",
        "",
        "## Selected indicators",
        "",
    ]
    for row in sorted(metadata_rows, key=lambda item: (item["sdg_number"], item["selection_role"] != "headline", item["dataset_id"])):
        lines.append(f"- SDG {row['sdg_number']} {row['selection_role']}: `{row['dataset_id']}` {row['series_label']}")
        if row["ambiguity_note"]:
            lines.append(f"  Note: {row['ambiguity_note']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    geos = parse_geos(args.countries, args.labels)
    output_dir = Path(args.output_dir).resolve()
    ensure_output_dir(output_dir)

    csv_path = output_dir / f"{args.output_stem}.csv"
    xlsx_path = output_dir / f"{args.output_stem}.xlsx"
    metadata_path = output_dir / "indicator_metadata.csv"
    method_note_path = output_dir / "method_note.md"
    log_path = output_dir / "extraction_log.txt"

    log_path.write_text("", encoding="utf-8")
    log(log_path, "Starting Eurostat SDG extraction.")
    log(log_path, f"Requested geographies: {', '.join(f'{label} ({code})' for code, label in geos)}")

    all_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []

    for selection in SELECTIONS:
        log(log_path, f"Processing {selection.dataset_id} ({selection.role}, SDG {selection.sdg_number}).")
        structure_meta = parse_structure(selection.dataset_id)
        rows, meta = fetch_series(selection, structure_meta, geos, args.start_year, log_path)
        all_rows.extend(rows)
        metadata_rows.append(meta)

    all_rows.sort(key=lambda row: (row["sdg_number"], row["selection_role"] != "headline", row["dataset_id"], row["geo_code"], row["year"]))
    metadata_rows.sort(key=lambda row: (row["sdg_number"], row["selection_role"] != "headline", row["dataset_id"]))

    csv_columns = [
        "sdg_number", "sdg_code", "selection_role", "dataset_id", "dataset_label", "series_label",
        "geo_code", "geo_name", "year", "value", "missing", "missing_reason", "unit_code", "unit_label",
        "applied_filters", "data_updated", "source_url",
    ]
    metadata_columns = [
        "sdg_number", "sdg_code", "selection_role", "dataset_id", "dataset_label", "series_label",
        "rationale", "ambiguity_note", "preferred_filters", "applied_filters", "filter_labels",
        "datastructure_id", "datastructure_version", "structure_url", "data_url", "data_updated",
        "dataset_latest_year", "latest_common_year", "availability_note",
    ]
    dynamic_geo_columns: list[str] = []
    for code, _label in geos:
        safe = code.lower().replace("-", "_")
        dynamic_geo_columns.extend([f"latest_year_{safe}", f"latest_value_{safe}", f"common_value_{safe}"])
    metadata_columns.extend(dynamic_geo_columns)

    write_csv(csv_path, all_rows, csv_columns)
    write_csv(metadata_path, metadata_rows, metadata_columns)

    latest_summary_rows = []
    for row in metadata_rows:
        summary = {
            "sdg_number": row["sdg_number"],
            "sdg_code": row["sdg_code"],
            "selection_role": row["selection_role"],
            "dataset_id": row["dataset_id"],
            "series_label": row["series_label"],
            "unit_label": extract_unit_label_from_filters(row["filter_labels"]),
            "latest_common_year": row["latest_common_year"],
            "availability_note": row["availability_note"],
        }
        for code, label in geos:
            safe = code.lower().replace("-", "_")
            label_safe = label.lower().replace(" ", "_")
            summary[f"{label_safe}_common_value"] = row.get(f"common_value_{safe}")
            summary[f"{label_safe}_latest_year"] = row.get(f"latest_year_{safe}")
            summary[f"{label_safe}_latest_value"] = row.get(f"latest_value_{safe}")
        latest_summary_rows.append(summary)

    summary_columns = [
        "sdg_number", "sdg_code", "selection_role", "dataset_id", "series_label", "unit_label", "latest_common_year", "availability_note"
    ]
    for _code, label in geos:
        label_safe = label.lower().replace(" ", "_")
        summary_columns.extend([f"{label_safe}_common_value", f"{label_safe}_latest_year", f"{label_safe}_latest_value"])

    write_xlsx(
        xlsx_path,
        [
            ("data", all_rows, csv_columns),
            ("latest_summary", latest_summary_rows, summary_columns),
            ("metadata", metadata_rows, metadata_columns),
        ],
    )
    write_method_note(method_note_path, metadata_rows, geos, args.start_year)

    missing_rows = sum(1 for row in all_rows if row["missing"] == "yes")
    log(log_path, f"Wrote {csv_path}")
    log(log_path, f"Wrote {xlsx_path}")
    log(log_path, f"Wrote {metadata_path}")
    log(log_path, f"Wrote {method_note_path}")
    log(log_path, f"Extraction completed: {len(metadata_rows)} selected indicators, {len(all_rows)} tidy rows, {missing_rows} explicit missing rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
