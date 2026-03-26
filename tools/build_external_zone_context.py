from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
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
LICENSES_DIR = RAW_DIR / "housing_urbanism" / "ayuntamiento_madrid"
INSPECCIONES_PATH = LICENSES_DIR / "inspecciones.csv"
CENSO_LOCALES_PATH = LICENSES_DIR / "censo_locales.csv"
VUT_PATH = LICENSES_DIR / "ayuntamiento_madrid_viviendas_de_uso_turistico_con_licencia_300694_1_viviendas_turisticas_geoportal.xlsx"
OUTPUT_PATH = PROCESSED_DIR / "madrid_zone_external_context.json"

MONTHS_ES = {
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
INSPECTION_MONTHS = {
    "ENERO": 1,
    "FEBRERO": 2,
    "MARZO": 3,
    "ABRIL": 4,
    "MAYO": 5,
    "JUNIO": 6,
    "JULIO": 7,
    "AGOSTO": 8,
    "SEPTIEMBRE": 9,
    "OCTUBRE": 10,
    "NOVIEMBRE": 11,
    "DICIEMBRE": 12,
}


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
    if "." in text and "," not in text:
        candidate = text
    else:
        candidate = text.replace(".", "").replace(",", ".")
    try:
        return round(float(candidate), 4)
    except Exception:
        return None


def _parse_population_date(value: str | None) -> datetime | None:
    text = (value or "").strip().lower()
    if not text:
        return None
    bits = text.split()
    if len(bits) >= 5 and bits[1] == "de" and bits[3] == "de":
        day = int(bits[0])
        month = MONTHS_ES.get(bits[2], 1)
        year = int(bits[4])
        return datetime(year, month, day)
    return None


def _parse_ddmmyyyy(value: str | None) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _parse_inspection_date(month_name: str | None, year_value: str | None) -> datetime | None:
    year = _to_int(year_value)
    month = INSPECTION_MONTHS.get((month_name or "").strip().upper())
    if not year or not month:
        return None
    return datetime(year, month, 1)


def _zone_key(value: str | None) -> str | None:
    return normalize_text_key(canonical_zone_label(value))


def _clean_label(value: str | None) -> str | None:
    return canonical_zone_label(value)


def _merge_payload(target: dict[str, dict], zone_key: str | None, payload: dict) -> None:
    if not zone_key:
        return
    current = target.get(zone_key, {})
    merged = dict(current)
    merged.update({key: value for key, value in payload.items() if value is not None})
    target[zone_key] = merged


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        return list(reader)


def _load_population_rows() -> tuple[dict[str, dict], dict[str, dict], str | None]:
    latest_rows: list[dict] = []
    latest_date: datetime | None = None

    for row in _read_csv_rows(POPULATION_PATH):
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
        district_key = _zone_key(district_name)
        neighborhood_key = _zone_key(neighborhood_name)
        base_payload = {
            "population": _to_int(row.get("num_personas")),
            "population_men": _to_int(row.get("num_personas_hombres")),
            "population_women": _to_int(row.get("num_personas_mujeres")),
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
        _merge_payload(
            district_context,
            _zone_key(row.get("Nombre distrito")),
            {
                "zone_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "district",
                "iguala_year": year,
                "vulnerability_index": _to_float(
                    row.get("Indice de Vulnerabilidad Territorial de Bienestar e Igualdad")
                    or row.get("Índice de Vulnerabilidad Territorial de Bienestar e Igualdad")
                ),
            },
        )

    for row in vulnerability_neighborhood_rows:
        year = _to_int(row.get("Fecha datos"))
        if year is not None:
            years.append(year)
        _merge_payload(
            neighborhood_context,
            _zone_key(row.get("Nombre barrio")),
            {
                "zone_label": _clean_label(row.get("Nombre barrio")),
                "district_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "neighborhood",
                "iguala_year": year,
                "vulnerability_index": _to_float(
                    row.get("Indice de Vulnerabilidad Territorial de Bienestar e Igualdad")
                    or row.get("Índice de Vulnerabilidad Territorial de Bienestar e Igualdad")
                ),
            },
        )

    for row in descriptives_district_rows:
        _merge_payload(
            district_context,
            _zone_key(row.get("Nombre distrito")),
            {
                "zone_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "district",
                "household_income_eur": _to_float(row.get("Renta neta media por hogar")),
                "cadastral_value_mean": _to_float(row.get("Valor catastral medio (vivienda residencial)")),
                "foreign_population_rate": _to_float(
                    row.get("Tasa de poblacion extranjera") or row.get("Tasa de población extranjera")
                ),
                "abstention_rate": _to_float(
                    row.get("Tasa de abstencion electoral") or row.get("Tasa de abstención electoral")
                ),
                "women_share": _to_float(row.get("Porcentaje de mujeres")),
                "age_dependency_share": _to_float(
                    row.get("Porcentaje de poblacion menos de 14 anos y mayor de 65 anos")
                    or row.get("Porcentaje de población menos de 14 años y mayor de 65 años")
                ),
            },
        )

    for row in descriptives_neighborhood_rows:
        _merge_payload(
            neighborhood_context,
            _zone_key(row.get("Nombre barrio")),
            {
                "zone_label": _clean_label(row.get("Nombre barrio")),
                "district_label": _clean_label(row.get("Nombre distrito")),
                "zone_level": "neighborhood",
                "household_income_eur": _to_float(row.get("Renta neta media por hogar")),
                "cadastral_value_mean": _to_float(row.get("Valor catastral medio (vivienda residencial)")),
                "foreign_population_rate": _to_float(
                    row.get("Tasa de poblacion extranjera") or row.get("Tasa de población extranjera")
                ),
                "abstention_rate": _to_float(
                    row.get("Tasa de abstencion electoral") or row.get("Tasa de abstención electoral")
                ),
                "women_share": _to_float(row.get("Porcentaje de mujeres")),
                "age_dependency_share": _to_float(
                    row.get("Porcentaje de poblacion menos de 14 anos y mayor de 65 anos")
                    or row.get("Porcentaje de población menos de 14 años y mayor de 65 años")
                ),
            },
        )

    return district_context, neighborhood_context, max(years) if years else None


def _text_bits(row: dict[str, str], *keys: str) -> str:
    return " ".join(str(row.get(key) or "").strip().lower() for key in keys)


def _is_change_of_use(text: str) -> bool:
    return any(
        token in text
        for token in (
            "cambio de uso",
            "transformacion locales en viviendas",
            "transformación locales en viviendas",
            "locales en viviendas",
            "local a vivienda",
        )
    )


def _is_new_dwelling(text: str) -> bool:
    return any(
        token in text
        for token in (
            "incr. n",
            "incremento n",
            "incremento de viviendas",
            "n viviendas",
        )
    )


def _load_license_signals() -> tuple[dict[str, dict], dict[str, dict], str | None]:
    district_counts: dict[str, Counter] = defaultdict(Counter)
    neighborhood_counts: dict[str, Counter] = defaultdict(Counter)
    latest_date: datetime | None = None
    cutoff_24m = datetime.now() - timedelta(days=730)

    files = sorted(
        LICENSES_DIR.glob(
            "ayuntamiento_madrid_licencias_urbanisticas_otorgadas_y_declaraciones_responsables_*_licencias_urbanisticas_xlsx.xlsx"
        )
    )

    for path in files:
        rows = []
        for sheet_name in ("Listado de Licencias concedidas", "DATOS", "Datos"):
            try:
                rows = read_xlsx_sheet_rows(path, sheet_name)
                break
            except KeyError:
                continue
        if not rows:
            continue
        for row in rows:
            granted_at = _parse_ddmmyyyy(row.get("Fecha concesión"))
            if granted_at is None:
                continue
            if latest_date is None or granted_at > latest_date:
                latest_date = granted_at
            if granted_at < cutoff_24m:
                continue

            district_label = _clean_label(row.get("Descripción Distrito"))
            neighborhood_label = _clean_label(row.get("Descripción Barrio"))
            district_key = _zone_key(district_label)
            neighborhood_key = _zone_key(neighborhood_label)

            text = _text_bits(row, "Tipo de expediente", "Objeto de la licencia", "Uso", "Ámbito")
            residential = "residencial" in text or "vivienda" in text
            change_of_use = _is_change_of_use(text)
            new_dwelling = _is_new_dwelling(text)

            if district_key:
                district_counts[district_key]["licenses_24m"] += 1
                if residential:
                    district_counts[district_key]["residential_licenses_24m"] += 1
                if change_of_use:
                    district_counts[district_key]["change_of_use_24m"] += 1
                if new_dwelling:
                    district_counts[district_key]["new_dwelling_24m"] += 1

            if neighborhood_key:
                neighborhood_counts[neighborhood_key]["licenses_24m"] += 1
                if residential:
                    neighborhood_counts[neighborhood_key]["residential_licenses_24m"] += 1
                if change_of_use:
                    neighborhood_counts[neighborhood_key]["change_of_use_24m"] += 1
                if new_dwelling:
                    neighborhood_counts[neighborhood_key]["new_dwelling_24m"] += 1

    districts = {
        key: {
            "urban_licenses_24m": counts.get("licenses_24m", 0),
            "residential_licenses_24m": counts.get("residential_licenses_24m", 0),
            "change_of_use_24m": counts.get("change_of_use_24m", 0),
            "new_dwelling_24m": counts.get("new_dwelling_24m", 0),
        }
        for key, counts in district_counts.items()
    }
    neighborhoods = {
        key: {
            "urban_licenses_24m": counts.get("licenses_24m", 0),
            "residential_licenses_24m": counts.get("residential_licenses_24m", 0),
            "change_of_use_24m": counts.get("change_of_use_24m", 0),
            "new_dwelling_24m": counts.get("new_dwelling_24m", 0),
        }
        for key, counts in neighborhood_counts.items()
    }
    return districts, neighborhoods, latest_date.date().isoformat() if latest_date else None


def _load_inspection_signals() -> tuple[dict[str, dict], str | None]:
    district_counts: dict[str, Counter] = defaultdict(Counter)
    latest_date: datetime | None = None
    cutoff_24m = datetime.now() - timedelta(days=730)

    for row in _read_csv_rows(INSPECCIONES_PATH):
        inspection_date = _parse_inspection_date(row.get("Mes"), row.get("Año"))
        if inspection_date is None:
            continue
        if latest_date is None or inspection_date > latest_date:
            latest_date = inspection_date
        if inspection_date < cutoff_24m:
            continue

        district_key = _zone_key(row.get("Distrito"))
        matter = (row.get("Materia objeto de la inspección") or "").strip().lower()
        if not district_key:
            continue

        district_counts[district_key]["urban_inspections_24m"] += 1
        if "disciplina urban" in matter:
            district_counts[district_key]["discipline_inspections_24m"] += 1

    districts = {
        key: {
            "urban_inspections_24m": counts.get("urban_inspections_24m", 0),
            "discipline_inspections_24m": counts.get("discipline_inspections_24m", 0),
        }
        for key, counts in district_counts.items()
    }
    return districts, latest_date.date().isoformat() if latest_date else None


def _load_locales_signals() -> tuple[dict[str, dict], dict[str, dict], str | None]:
    district_counts: dict[str, Counter] = defaultdict(Counter)
    neighborhood_counts: dict[str, Counter] = defaultdict(Counter)
    latest_date: str | None = None

    for row in _read_csv_rows(CENSO_LOCALES_PATH):
        district_label = _clean_label((row.get("desc_distrito_local") or "").strip())
        neighborhood_label = _clean_label((row.get("desc_barrio_local") or "").strip())
        district_key = _zone_key(district_label)
        neighborhood_key = _zone_key(neighborhood_label)
        status = (row.get("desc_situacion_local") or "").strip().lower()
        latest_date = row.get("fx_carga") or latest_date

        for key, counts in ((district_key, district_counts), (neighborhood_key, neighborhood_counts)):
            if not key:
                continue
            counts[key]["locales_total"] += 1
            if status == "abierto":
                counts[key]["locales_open"] += 1
            elif status == "cerrado":
                counts[key]["locales_closed"] += 1
            elif "vivienda" in status:
                counts[key]["locales_residential_use"] += 1

    districts = {
        key: {
            "locales_total": counts.get("locales_total", 0),
            "locales_open": counts.get("locales_open", 0),
            "locales_closed": counts.get("locales_closed", 0),
            "locales_residential_use": counts.get("locales_residential_use", 0),
        }
        for key, counts in district_counts.items()
    }
    neighborhoods = {
        key: {
            "locales_total": counts.get("locales_total", 0),
            "locales_open": counts.get("locales_open", 0),
            "locales_closed": counts.get("locales_closed", 0),
            "locales_residential_use": counts.get("locales_residential_use", 0),
        }
        for key, counts in neighborhood_counts.items()
    }
    return districts, neighborhoods, latest_date


def _load_vut_signals() -> tuple[dict[str, dict], str | None]:
    district_counts: dict[str, Counter] = defaultdict(Counter)
    rows = read_xlsx_sheet_rows(VUT_PATH, "Hoja1")

    for row in rows:
        district_label = (row.get("DISTRITO") or "").replace("Distrito de", "").strip()
        district_key = _zone_key(district_label)
        if not district_key:
            continue
        district_counts[district_key]["vut_records"] += 1
        district_counts[district_key]["vut_units"] += _to_int(row.get("UNIDADES_VUT")) or 0

    districts = {
        key: {
            "vut_records": counts.get("vut_records", 0),
            "vut_units": counts.get("vut_units", 0),
        }
        for key, counts in district_counts.items()
    }
    return districts, str(len(rows))


def _safe_rate(value: int | float | None, population: int | float | None, multiplier: float) -> float | None:
    if not value or not population:
        return None
    return round((float(value) / float(population)) * multiplier, 4)


def _inherit_district_metrics(neighborhoods: dict[str, dict], districts: dict[str, dict]) -> None:
    for payload in neighborhoods.values():
        district_key = _zone_key(payload.get("district_label"))
        district_payload = districts.get(district_key or "")
        if not district_payload:
            continue
        for key in (
            "urban_inspections_24m",
            "discipline_inspections_24m",
            "vut_records",
            "vut_units",
        ):
            if key in district_payload:
                payload[f"district_{key}"] = district_payload[key]


def _add_derived_rates(payloads: dict[str, dict]) -> None:
    for payload in payloads.values():
        population = payload.get("population")
        payload["change_of_use_per_10k_population"] = _safe_rate(
            payload.get("change_of_use_24m"), population, 10000.0
        )
        payload["urban_inspections_per_10k_population"] = _safe_rate(
            payload.get("urban_inspections_24m") or payload.get("district_urban_inspections_24m"),
            population,
            10000.0,
        )
        payload["closed_locales_per_1k_population"] = _safe_rate(
            payload.get("locales_closed"), population, 1000.0
        )
        payload["vut_units_per_1k_population"] = _safe_rate(
            payload.get("vut_units") or payload.get("district_vut_units"), population, 1000.0
        )


def build_zone_context() -> dict:
    population_districts, population_neighborhoods, population_date = _load_population_rows()
    iguala_districts, iguala_neighborhoods, iguala_year = _load_iguala_rows()
    license_districts, license_neighborhoods, licenses_latest = _load_license_signals()
    inspection_districts, inspections_latest = _load_inspection_signals()
    locales_districts, locales_neighborhoods, locales_date = _load_locales_signals()
    vut_districts, vut_rows_info = _load_vut_signals()

    districts = dict(population_districts)
    neighborhoods = dict(population_neighborhoods)

    for source in (iguala_districts, license_districts, inspection_districts, locales_districts, vut_districts):
        for key, payload in source.items():
            _merge_payload(districts, key, payload)
    for source in (iguala_neighborhoods, license_neighborhoods, locales_neighborhoods):
        for key, payload in source.items():
            _merge_payload(neighborhoods, key, payload)

    _inherit_district_metrics(neighborhoods, districts)
    _add_derived_rates(districts)
    _add_derived_rates(neighborhoods)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "sources": {
            "population_csv": str(POPULATION_PATH),
            "population_date": population_date,
            "iguala_xlsx": str(IGUALA_PATH),
            "iguala_year": iguala_year,
            "licenses_dir": str(LICENSES_DIR),
            "licenses_latest_date": licenses_latest,
            "inspections_csv": str(INSPECCIONES_PATH),
            "inspections_latest_date": inspections_latest,
            "censo_locales_csv": str(CENSO_LOCALES_PATH),
            "censo_locales_date": locales_date,
            "vut_xlsx": str(VUT_PATH),
            "vut_rows_loaded": vut_rows_info,
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
            "districts_with_change_of_use": sum(
                1 for payload in districts.values() if payload.get("change_of_use_24m")
            ),
            "neighborhoods_with_change_of_use": sum(
                1 for payload in neighborhoods.values() if payload.get("change_of_use_24m")
            ),
            "districts_with_inspections": sum(
                1 for payload in districts.values() if payload.get("urban_inspections_24m")
            ),
            "neighborhoods_with_closed_locales": sum(
                1 for payload in neighborhoods.values() if payload.get("locales_closed")
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
