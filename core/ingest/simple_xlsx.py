from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile
import xml.etree.ElementTree as ET


NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkg": "http://schemas.openxmlformats.org/package/2006/relationships",
}


def _column_letters(cell_ref: str) -> str:
    letters: list[str] = []
    for char in cell_ref:
        if char.isalpha():
            letters.append(char)
        else:
            break
    return "".join(letters)


def _sheet_targets(zf: ZipFile) -> dict[str, str]:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"].lstrip("/")
        for rel in rels
    }

    targets: dict[str, str] = {}
    for sheet in workbook.find("main:sheets", NS) or []:
        name = sheet.attrib.get("name")
        rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        target = rel_map.get(rel_id or "")
        if name and target:
            targets[name] = target
    return targets


def _shared_strings(zf: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []

    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for item in root.findall("main:si", NS):
        texts = [node.text or "" for node in item.iterfind(".//main:t", NS)]
        values.append("".join(texts))
    return values


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str | None:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("main:v", NS)
    inline_node = cell.find("main:is", NS)

    if value_node is not None:
        value = value_node.text
        if value is None:
            return None
        if cell_type == "s":
            try:
                return shared_strings[int(value)]
            except Exception:
                return value
        return value

    if inline_node is not None:
        return "".join(node.text or "" for node in inline_node.iterfind(".//main:t", NS))

    return None


def read_xlsx_sheet_rows(path: str | Path, sheet_name: str) -> list[dict[str, str]]:
    workbook_path = Path(path)
    with ZipFile(workbook_path) as zf:
        targets = _sheet_targets(zf)
        target = targets.get(sheet_name)
        if not target:
            raise KeyError(f"Sheet not found: {sheet_name}")

        shared_strings = _shared_strings(zf)
        root = ET.fromstring(zf.read(f"xl/{target}"))
        sheet_data = root.find("main:sheetData", NS)
        if sheet_data is None:
            return []

        rows: list[dict[str, str]] = []
        headers: list[str] | None = None

        for row in sheet_data.findall("main:row", NS):
            values_by_col: dict[str, str] = {}
            for cell in row.findall("main:c", NS):
                cell_ref = cell.attrib.get("r") or ""
                column = _column_letters(cell_ref)
                if not column:
                    continue
                value = _cell_value(cell, shared_strings)
                values_by_col[column] = (value or "").strip()

            if not values_by_col:
                continue

            ordered_columns = sorted(values_by_col.keys(), key=lambda col: (len(col), col))
            ordered_values = [values_by_col.get(col, "") for col in ordered_columns]

            if headers is None:
                if not any(ordered_values):
                    continue
                headers = ordered_values
                continue

            row_dict: dict[str, str] = {}
            for index, header in enumerate(headers):
                if not header:
                    continue
                value = ordered_values[index] if index < len(ordered_values) else ""
                row_dict[str(header).strip()] = value

            if any(value for value in row_dict.values()):
                rows.append(row_dict)

    return rows
