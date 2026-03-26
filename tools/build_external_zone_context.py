from __future__ import annotations

import csv
import sys
from datetime import UTC, datetime
from pathlib import Path

import orjson

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.features.location_labels import canonical_zone_label
from core.ingest.simple_xlsx import read_xlsx_sheet_rows
from core.normalization.text import normalize_text_key


RAW_DIR = BASE_DIR / "data" / "raw" / "external_context" / "resources"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

POPULATION_PATH = RAW_DIR / "demography" / "ayuntamiento_madrid" / "poblacion_distrito_barrio_madrid.csv"
IGUALA_PATH = (
    RAW_DIR
    / "socioeconomic"
    / "ayuntamiento_madrid"
    / "ayuntamiento_madrid_iguala_indice_de_vulnerabilidad_territorial_agregado_del_ayuntamiento_de_madrid_300577_11_iguala_vulnerabilidad_xlsx.xlsx"
)
OUTPUT_PATH = PROCESSED_DIR / "madrid_zone_external_context.json"


def ensure_dirs() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".")
    try:
        return int(float(text))
    except Exception:
        return None


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".")
    if "." in str(value) and "," not in str(value):
        text = str(value).strip()
    try:
        return round(float(text), 4)
    except Exception:
        return None


def _parse_population_date(value: str | None) -> datetime | None:
    text = (value or "").strip().lower()
    if not text:
        return None
    months = {
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12,
    }
    bits = text.split()
    if len(bits) >= 5 and bits[1] == "de" and bits[3] == "de":
        day = int(bits[0])
        month = months.get(bits[2], 1)
        year = int(bits[4])
        return datetime(year, month, day)
    return None


def _zone_key(value: str | None) -> str | None:
    return normalize_text_key(canonical_zone_label(value))


def _clean_label(value: str | None) -> str | None:
    return canonical_zone_label(value)


def _load_population_rows() -> tuple[dict[str, dict], dict[str, dict], str | None]:
    latest_rows: list[dict] = []
    latest_date: datetime | None = None

    with POPULATION_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            row_date = _parse_population_date(row.get("fecha"))
            if row_date is None:
                continue
            if latest_date is None or row_date > latest_date:
                latest_date = row_date
                latest_rows = [row]
            elif row_date == latest_date:
                latest_rows.append(row)

    district_map: dict[str, dict] = {}
    neighborhood_map: dict[str, dict] = {}

    for row in latest_rows:
        district_name = (row.get("distrito") or "").strip()
        neighborhood_name = (row.get("barrio") or "").strip()
        population = _to_int(row.get("num_personas"))
        male = _to_int(row.get("num_personas_hombres"))
        female = _to_int(row.get("num_personas_mujeres"))
        district_key = _zone_key(district_name)
        neighborhood_key = _zone_key(neighborhood_name)

        base_payload = {
            "population": population,
            "population_men": male,
            "population_women": female,
            "population_date": latest_date.date().isoformat() if latest_date else None,
        }

        if district_name == neighborhood_name and district_key:
            district_map[district_key] = {
                "zone_label": _clean_label(district_name),
                "zone_level": "district",
                **base_payload,
            }
        elif neighborhood_key:
            neighborhood_map[neighborhood_key] = {
                "zone_label": _clean_label(neighborhood_name),
                "zone_level": "neighborhood",
                "district_label": _clean_label(district_name),
                **base_payload,
            }

    return district_map, neighborhood_map, latest_date.date().isoformat() if latest_date else None


def _merge_payload(target: dict[str, dict], zone_key: str | None, payload: dict) -> None:
    if not zone_key:
        return
    current = target.get(zone_key, {})
    merged = dict(current)
    merged.update({key: value for key, value in payload.items() if value is not None})
    target[zone_key] = merged


def _load_iguala_rows() -> tuple[dict[str, dict], dict[str, dict], int | None]:
    district_context: dict[str, dict] = {}
    neighborhood_context: dict[str, dict] = {}

    vulnerability_district_rows = read_xlsx_sheet_rows(IGUALA_PATH, "Indicadores BI distritos")
    vulnerability_neighborhood_rows = read_xlsx_sheet_rows(IGUALA_PATH, "Indicadores BI barrios")
    descriptives_district_rows = read_xlsx_sheet_rows(IGUALA_PATH, "Descriptivos distritos")
    descriptives_neighborhood_rows = read_xlsx_sheet_rows(IGUALA_PATH, "Descriptivos barrios")

    years: list[int] = []

    for row in vulnerability_district_rows:
        year = _to_int(row.get("Fecha datos"))
        if year is not None:
            years.append(year)
        zone_key = _zone_key(row.get("Nombre distrito"))
        _merge_payload(
            district_context,
            zone_key,
            {
                "zone_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "district",
                "iguala_year": year,
                "vulnerability_index": _to_float(
                    row.get("Índice de Vulnerabilidad Territorial de Bienestar e Igualdad")
                ),
            },
        )

    for row in vulnerability_neighborhood_rows:
        year = _to_int(row.get("Fecha datos"))
        if year is not None:
            years.append(year)
        zone_key = _zone_key(row.get("Nombre barrio"))
        _merge_payload(
            neighborhood_context,
            zone_key,
            {
                "zone_label": _clean_label(row.get("Nombre barrio")),
                "district_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "neighborhood",
                "iguala_year": year,
                "vulnerability_index": _to_float(
                    row.get("Índice de Vulnerabilidad Territorial de Bienestar e Igualdad")
                ),
            },
        )

    for row in descriptives_district_rows:
        zone_key = _zone_key(row.get("Nombre distrito"))
        _merge_payload(
            district_context,
            zone_key,
            {
                "zone_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "district",
                "household_income_eur": _to_float(row.get("Renta neta media por hogar")),
                "cadastral_value_mean": _to_float(row.get("Valor catastral medio (vivienda residencial)")),
                "foreign_population_rate": _to_float(row.get("Tasa de población extranjera")),
                "abstention_rate": _to_float(row.get("Tasa de abstención electoral")),
                "women_share": _to_float(row.get("Porcentaje de mujeres")),
                "age_dependency_share": _to_float(
                    row.get("Porcentaje de población menos de 14 años y mayor de 65 años")
                ),
            },
        )

    for row in descriptives_neighborhood_rows:
        zone_key = _zone_key(row.get("Nombre barrio"))
        _merge_payload(
            neighborhood_context,
            zone_key,
            {
                "zone_label": _clean_label(row.get("Nombre barrio")),
                "district_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "neighborhood",
                "household_income_eur": _to_float(row.get("Renta neta media por hogar")),
                "cadastral_value_mean": _to_float(row.get("Valor catastral medio (vivienda residencial)")),
                "foreign_population_rate": _to_float(row.get("Tasa de población extranjera")),
                "abstention_rate": _to_float(row.get("Tasa de abstención electoral")),
                "women_share": _to_float(row.get("Porcentaje de mujeres")),
                "age_dependency_share": _to_float(
                    row.get("Porcentaje de población menos de 14 años y mayor de 65 años")
                ),
            },
        )

    return district_context, neighborhood_context, max(years) if years else None


def build_zone_context() -> dict:
    population_districts, population_neighborhoods, population_date = _load_population_rows()
    iguala_districts, iguala_neighborhoods, iguala_year = _load_iguala_rows()

    districts = dict(population_districts)
    neighborhoods = dict(population_neighborhoods)

    for key, payload in iguala_districts.items():
        _merge_payload(districts, key, payload)
    for key, payload in iguala_neighborhoods.items():
        _merge_payload(neighborhoods, key, payload)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "sources": {
            "population_csv": str(POPULATION_PATH),
            "population_date": population_date,
            "iguala_xlsx": str(IGUALA_PATH),
            "iguala_year": iguala_year,
        },
        "summary": {
            "district_count": len(districts),
            "neighborhood_count": len(neighborhoods),
            "districts_with_population": sum(1 for payload in districts.values() if payload.get("population")),
            "neighborhoods_with_population": sum(1 for payload in neighborhoods.values() if payload.get("population")),
            "districts_with_vulnerability": sum(
                1 for payload in districts.values() if payload.get("vulnerability_index") is not None
            ),
            "neighborhoods_with_vulnerability": sum(
                1 for payload in neighborhoods.values() if payload.get("vulnerability_index") is not None
            ),
        },
        "districts": districts,
        "neighborhoods": neighborhoods,
    }


def main() -> None:
    ensure_dirs()
    payload = build_zone_context()
    OUTPUT_PATH.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
    print(f"zone_context={OUTPUT_PATH}")
    print(f"districts={payload['summary']['district_count']}")
    print(f"neighborhoods={payload['summary']['neighborhood_count']}")


if __name__ == "__main__":
    main()
