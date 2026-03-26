from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path
from urllib.request import urlopen

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import orjson

from core.geography.madrid_street_catalog import (
    build_street_lookup_key,
    canonical_street_type,
    normalize_street_name_only,
)

VIALES_URL = "https://datos.madrid.es/dataset/200075-0-callejero/resource/200075-3-callejero-csv/download/200075-3-callejero-csv.csv"
DIRECCIONES_URL = "https://datos.madrid.es/dataset/200075-0-callejero/resource/200075-1-callejero-csv/download/200075-1-callejero-csv.csv"

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

VIALES_RAW_PATH = RAW_DIR / "madrid_viales_oficiales.csv"
DIRECCIONES_RAW_PATH = RAW_DIR / "madrid_direcciones_vigentes.csv"
OUTPUT_PATH = PROCESSED_DIR / "madrid_street_catalog.json"


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def download_file(url: str, output_path: Path) -> None:
    with urlopen(url) as response:
        data = response.read()
    output_path.write_bytes(data)


def open_csv_dicts(path: Path):
    encodings_to_try = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

    last_error = None
    for encoding in encodings_to_try:
        try:
            with path.open("r", encoding=encoding, newline="") as f:
                sample = f.read(4096)
                f.seek(0)

                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
                    delimiter = dialect.delimiter
                except Exception:
                    delimiter = ";"

                reader = csv.DictReader(f, delimiter=delimiter)
                rows = []

                for row in reader:
                    clean_row = {}
                    for key, value in row.items():
                        if key is None:
                            continue
                        clean_key = str(key).strip()
                        clean_row[clean_key] = value.strip() if isinstance(value, str) else value
                    rows.append(clean_row)

                return rows

        except UnicodeDecodeError as e:
            last_error = e
            continue

    raise last_error or RuntimeError(f"No se pudo leer el CSV: {path}")


def pick(row: dict, *keys: str) -> str | None:
    def norm(s: str) -> str:
        s = str(s).strip().lower()
        s = (
            s.replace("á", "a")
             .replace("é", "e")
             .replace("í", "i")
             .replace("ó", "o")
             .replace("ú", "u")
        )
        s = " ".join(s.split())
        return s

    normalized_row = {}
    for key, value in row.items():
        if key is None:
            continue
        normalized_row[norm(key)] = value

    for key in keys:
        value = normalized_row.get(norm(key))
        if value not in (None, "", "-"):
            return str(value).strip()

    return None


import re


import re


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("\xa0", " ").strip()

    # 1) primero intentar DMS: 3º40'23.6'' W / 40º29'21.82'' N
    dms_pattern = re.compile(
        r"""^\s*
        (?P<deg>\d+(?:[.,]\d+)?)\s*[º°]\s*
        (?P<min>\d+(?:[.,]\d+)?)\s*'\s*
        (?P<sec>\d+(?:[.,]\d+)?)\s*(?:''|")?\s*
        (?P<hem>[NSEW])?
        \s*$""",
        re.IGNORECASE | re.VERBOSE,
    )

    match = dms_pattern.match(text)
    if match:
        deg = float(match.group("deg").replace(",", "."))
        minutes = float(match.group("min").replace(",", "."))
        seconds = float(match.group("sec").replace(",", "."))
        hemisphere = (match.group("hem") or "").upper()

        decimal = deg + (minutes / 60.0) + (seconds / 3600.0)

        if hemisphere in {"S", "W"}:
            decimal *= -1

        return decimal

    # 2) si no es DMS, intentar decimal normal
    decimal_candidate = text.replace(" ", "").replace(",", ".")
    decimal_candidate = "".join(
        ch for ch in decimal_candidate if ch.isdigit() or ch in ".-"
    )

    if not decimal_candidate or decimal_candidate in {".", "-", "-.", ".-"}:
        return None

    try:
        return float(decimal_candidate)
    except Exception:
        return None

def normalize_district_list(value: str | None) -> list[str]:
    if not value:
        return []

    text = str(value).strip()
    if not text:
        return []

    # soporta formatos tipo "01-03-04-15-20" o "04/05"
    if "-" in text and "/" not in text:
        parts = [p.strip() for p in text.split("-") if p.strip()]
        return parts

    parts = [part.strip() for part in text.split("/") if part.strip()]
    return parts


def build_streets(rows: list[dict]) -> tuple[list[dict], dict[str, str], dict[str, list[str]], dict[str, dict]]:
    streets: list[dict] = []
    by_lookup_key: dict[str, str] = {}
    by_name_only: dict[str, list[str]] = defaultdict(list)
    by_street_code: dict[str, dict] = {}

    for row in rows:
        street_code = pick(row, "Codigo de vía", "Codigo de via", "COD_VIA")
        street_type = canonical_street_type(
            pick(row, "Clase de la via", "Clase de la vía", "VIA_CLASE")
        )
        street_name = normalize_street_name_only(
            pick(row, "Nombre de la via", "Nombre de la vía", "VIA_NOMBRE")
        )
        street_literal = pick(
            row,
            "Literal completo del vial",
            "Literal completo del vial ",
            "LITERAL_COMPLETO_VIAL",
        )
        district_names = normalize_district_list(
            pick(row, "Distritos atravesados", "DISTRITOS_ATRAVESADOS")
        )
        postal_codes_raw = pick(row, "Codigos postales", "Códigos postales", "CODIGOS_POSTALES")

        if not street_code or not street_name:
            continue

        lookup_key = build_street_lookup_key(street_type, street_name)
        postal_codes = []
        if postal_codes_raw:
            postal_codes = [p.strip() for p in postal_codes_raw.split("/") if p.strip()]

        payload = {
            "street_code": street_code,
            "street_type": street_type,
            "street_name": street_name,
            "street_literal": street_literal or (
                f"{street_type} {street_name}" if street_type else street_name
            ),
            "district_names": district_names,
            "postal_codes": postal_codes,
        }

        streets.append(payload)
        by_street_code[street_code] = payload

        if lookup_key and lookup_key not in by_lookup_key:
            by_lookup_key[lookup_key] = street_code

        if street_name not in by_name_only or street_code not in by_name_only[street_name]:
            by_name_only[street_name].append(street_code)

    by_name_only_sorted = {
        key: sorted(values)
        for key, values in by_name_only.items()
    }

    return streets, by_lookup_key, by_name_only_sorted, by_street_code


def build_address_points(rows: list[dict]) -> dict[str, dict[str, dict]]:
    by_street_code: dict[str, dict[str, dict]] = defaultdict(dict)

    for row in rows:
        street_code = pick(row, "Codigo de via", "Codigo de vía", "COD_VIA")
        house_number_literal = pick(row, "Literal de numeracion", "Literal de numeración")
        district_name = pick(row, "Nombre del distrito")
        neighborhood_name = pick(row, "Nombre del barrio")
        postal_code = pick(row, "Codigo postal", "Código postal")
        lat = parse_float(pick(row, "Latitud en S R ETRS89 WGS84"))
        lon = parse_float(pick(row, "Longitud en S R ETRS89 WGS84"))

        if not street_code or not house_number_literal:
            continue

        number_digits = "".join(ch for ch in house_number_literal if ch.isdigit())
        if not number_digits:
            continue

        existing = by_street_code[street_code].get(number_digits)
        candidate = {
            "house_number": number_digits,
            "district_name": district_name,
            "neighborhood_name": neighborhood_name,
            "postal_code": postal_code,
            "lat": lat,
            "lon": lon,
        }

        if existing is None:
            by_street_code[street_code][number_digits] = candidate
        else:
            old_score = int(bool(existing.get("neighborhood_name"))) + int(bool(existing.get("lat")))
            new_score = int(bool(candidate.get("neighborhood_name"))) + int(bool(candidate.get("lat")))
            if new_score > old_score:
                by_street_code[street_code][number_digits] = candidate

    return by_street_code


def main() -> None:
    ensure_dirs()

    print("Descargando viales oficiales...")
    download_file(VIALES_URL, VIALES_RAW_PATH)

    print("Descargando direcciones vigentes...")
    download_file(DIRECCIONES_URL, DIRECCIONES_RAW_PATH)

    print("Leyendo CSV de viales...")
    viales_rows = open_csv_dicts(VIALES_RAW_PATH)

    print("Leyendo CSV de direcciones...")
    direcciones_rows = open_csv_dicts(DIRECCIONES_RAW_PATH)

    print("Construyendo catálogo de calles...")
    streets, by_lookup_key, by_name_only, by_street_code = build_streets(viales_rows)

    print("Construyendo índice de numeración...")
    address_points = build_address_points(direcciones_rows)

    payload = {
        "version": 1,
        "source": {
            "provider": "Ayuntamiento de Madrid - Portal de Datos Abiertos",
            "viales_url": VIALES_URL,
            "direcciones_url": DIRECCIONES_URL,
            "viales_rows": len(viales_rows),
            "direcciones_rows": len(direcciones_rows),
        },
        "streets": streets,
        "by_lookup_key": by_lookup_key,
        "by_name_only": by_name_only,
        "by_street_code": by_street_code,
        "address_points": address_points,
    }

    OUTPUT_PATH.write_bytes(orjson.dumps(payload))
    print(f"Catálogo generado en: {OUTPUT_PATH}")
    print(f"Calles cargadas: {len(streets)}")
    print(f"Calles con numeración indexada: {len(address_points)}")


if __name__ == "__main__":
    main()