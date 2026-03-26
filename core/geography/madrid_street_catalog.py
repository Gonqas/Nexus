from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import unicodedata

import orjson
from rapidfuzz import fuzz, process


def _strip_accents(value: str) -> str:
    return "".join(
        ch
        for ch in unicodedata.normalize("NFD", value)
        if unicodedata.category(ch) != "Mn"
    )


def normalize_geo_text(value: str | None) -> str | None:
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    text = _strip_accents(text)
    text = text.replace("º", " ")
    text = text.replace("ª", " ")
    text = text.replace("'", " ")
    text = text.replace('"', " ")
    text = re.sub(r"\b(c|cl|av|avda|pza|pl|ps|pso|ctra|trav|trva)/\s*", r"\1 ", text)
    text = text.replace("/", " / ")
    text = re.sub(r"\bn\s*[ºo]\s*", " numero ", text)
    text = re.sub(r"\bnum(?:ero)?\s+", " numero ", text)

    # OJO: no borrar comas ni guiones aquí, porque se usan para separar partes
    text = re.sub(r"[();:|]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _cleanup_address_noise(text)
    return text or None


def _split_address_parts_from_text(text: str) -> list[str]:
    text = re.sub(r"\s*-\s*", ", ", text)
    return [p.strip() for p in text.split(",") if p.strip()]


def split_address_parts(value: str | None) -> list[str]:
    text = normalize_geo_text(value)
    if not text:
        return []

    # Convertimos " - " en separador de partes para casos tipo "Lavapiés - Barrio"
    return _split_address_parts_from_text(text)


STREET_TYPE_ALIASES: dict[str, str] = {
    "cl": "calle",
    "c": "calle",
    "c.": "calle",
    "calle": "calle",
    "av": "avenida",
    "av.": "avenida",
    "avda": "avenida",
    "avda.": "avenida",
    "avenida": "avenida",
    "pso": "paseo",
    "pso.": "paseo",
    "ps": "paseo",
    "paseo": "paseo",
    "pl": "plaza",
    "pl.": "plaza",
    "pza": "plaza",
    "pza.": "plaza",
    "plaza": "plaza",
    "rda": "ronda",
    "rda.": "ronda",
    "ronda": "ronda",
    "cam": "camino",
    "cam.": "camino",
    "camino": "camino",
    "ctra": "carretera",
    "ctra.": "carretera",
    "carretera": "carretera",
    "trva": "travesia",
    "trva.": "travesia",
    "trav": "travesia",
    "travesia": "travesia",
    "via": "via",
    "glorieta": "glorieta",
    "pasaje": "pasaje",
    "cuesta": "cuesta",
    "bulevar": "bulevar",
}

STREET_TYPE_PATTERN = re.compile(
    r"^(calle|cl|c\.?|avenida|avda\.?|av\.?|av|paseo|pso\.?|ps|plaza|pza\.?|pl\.?|pl|ronda|rda\.?|camino|cam\.?|carretera|ctra\.?|travesia|trav\.?|trva\.?|via|glorieta|pasaje|cuesta|bulevar)(?=\s|$)",
    re.IGNORECASE,
)

HOUSE_NUMBER_PATTERN = re.compile(r"\b(\d{1,4})(?:\s*([a-zA-Z]))?\b")
POSTAL_CODE_PATTERN = re.compile(r"\b28\d{3}\b")
TRAILING_CITY_PATTERN = re.compile(
    r"(?:,\s*|\s+)(?:madrid(?:\s+capital|\s+centro)?|espana|spain)\s*$",
    re.IGNORECASE,
)
UNIT_ONLY_PATTERN = re.compile(
    r"^(?:"
    r"portal\s+\w+|"
    r"bloque\s+\w+|"
    r"esc(?:alera)?\s+\w+|"
    r"pl(?:anta)?\s+\w+|"
    r"piso\s+\w+|"
    r"puerta\s+\w+|"
    r"pta\.?\s*\w+|"
    r"pto\.?\s*\w+|"
    r"bajo(?:\s+\w+)?|"
    r"bj(?:\s+\w+)?|"
    r"entresuelo(?:\s+\w+)?|"
    r"atico(?:\s+\w+)?|"
    r"interior|exterior|"
    r"izq(?:uierda)?|"
    r"dcha|derecha|"
    r"\d+\s*(?:o|º|ª)?\s*[a-z]{0,3}"
    r")$",
    re.IGNORECASE,
)
UNIT_TAIL_PATTERN = re.compile(
    r"^(?:"
    r"portal\s+\w+|"
    r"bloque\s+\w+|"
    r"esc(?:alera)?\s+\w+|"
    r"pl(?:anta)?\s+\w+|"
    r"piso\s+\w+|"
    r"puerta\s+\w+|"
    r"pta\.?\s*\w+|"
    r"pto\.?\s*\w+|"
    r"bajo(?:\s+\w+)?|"
    r"bj(?:\s+\w+)?|"
    r"entresuelo(?:\s+\w+)?|"
    r"atico(?:\s+\w+)?|"
    r"interior|exterior|"
    r"izq(?:uierda)?|"
    r"dcha|derecha|"
    r"\d+\s*(?:o|º|ª)\s*[a-z]{0,3}"
    r")(?:\b.*)?$",
    re.IGNORECASE,
)

KNOWN_NEIGHBORHOODS: dict[str, tuple[str, str | None]] = {
    "lavapies": ("Embajadores", "Centro"),
    "lavapies zona": ("Embajadores", "Centro"),
    "chueca": ("Justicia", "Centro"),
    "malasana": ("Universidad", "Centro"),
    "moncloa": ("Argüelles", "Moncloa-Aravaca"),
    "hispanoamerica": ("Hispanoamérica", "Chamartín"),
    "prosperidad": ("Prosperidad", "Chamartín"),
    "almagro": ("Almagro", "Chamberí"),
    "bellas vistas": ("Bellas Vistas", "Tetuán"),
    "valdeacederas": ("Valdeacederas", "Tetuán"),
    "canillejas": ("Canillejas", "San Blas-Canillejas"),
    "quintana": ("Quintana", "Ciudad Lineal"),
    "pacifico": ("Pacífico", "Retiro"),
    "cuatro caminos": ("Cuatro Caminos", "Tetuán"),
    "lista": ("Lista", "Salamanca"),
    "recoletos": ("Recoletos", "Salamanca"),
    "castillejos": ("Castillejos", "Tetuán"),
    "fontarron": ("Fontarrón", "Moratalaz"),
    "vinateros": ("Vinateros", "Moratalaz"),
    "ventas": ("Ventas", "Ciudad Lineal"),
    "acacias": ("Acacias", "Arganzuela"),
    "cortes": ("Cortes", "Centro"),
    "imperial": ("Imperial", "Arganzuela"),
    "el pilar": ("Pilar", "Fuencarral-El Pardo"),
    "la paz": ("La Paz", "Fuencarral-El Pardo"),
    "costillares": ("Costillares", "Ciudad Lineal"),
    "el viso": ("El Viso", "Chamartín"),
    "canillas": ("Canillas", "Hortaleza"),
    "arroyofresno": ("Mirasierra", "Fuencarral-El Pardo"),
    "las tablas": ("Valverde", "Fuencarral-El Pardo"),
    "montecarmelo": ("Mirasierra", "Fuencarral-El Pardo"),
    "pinar del rey": ("Pinar del Rey", "Hortaleza"),
    "rios rosas": ("Ríos Rosas", "Chamberí"),
    "la guindalera": ("Guindalera", "Salamanca"),
    "guindalera": ("Guindalera", "Salamanca"),
    "alameda de osuna": ("Alameda de Osuna", "Barajas"),
    "marroquina": ("Marroquina", "Moratalaz"),
    "virgen del cortijo": ("Valdefuentes", "Hortaleza"),
    "simancas": ("Simancas", "San Blas-Canillejas"),
    "fuencarral": ("Valverde", "Fuencarral-El Pardo"),
    "trafalgar": ("Trafalgar", "Chamberí"),
    "almenara": ("Almenara", "Tetuán"),
    "palos de moguer": ("Palos de la Frontera", "Arganzuela"),
    "ilustracion": ("Peñagrande", "Fuencarral-El Pardo"),
    "estrella": ("Estrella", "Retiro"),
    "atocha": ("Atocha", "Arganzuela"),
    "chopera": ("Chopera", "Arganzuela"),
    "berruguete": ("Berruguete", "Tetuán"),
    "nueva espana": ("Nueva España", "Chamartín"),
    "velazquez": ("Recoletos", "Salamanca"),
    "salvador": ("Salvador", "San Blas-Canillejas"),
    "ciudad jardin": ("Ciudad Jardín", "Chamartín"),
    "delicias": ("Delicias", "Arganzuela"),
    "penagrande": ("Peñagrande", "Fuencarral-El Pardo"),
    "piovera": ("Piovera", "Hortaleza"),
    "puerta hierro": ("Ciudad Universitaria", "Moncloa-Aravaca"),
    "ibiza": ("Ibiza", "Retiro"),
    "sol": ("Sol", "Centro"),
    "serrano recoletos": ("Recoletos", "Salamanca"),
    "gaztambide": ("Gaztambide", "Chamberí"),
    "arapiles": ("Arapiles", "Chamberí"),
    "castellana": ("Castellana", "Salamanca"),
    "mirasierra": ("Mirasierra", "Fuencarral-El Pardo"),
    "goya": ("Goya", "Salamanca"),
    "nino jesus": ("Niño Jesús", "Retiro"),
    "adelfas": ("Adelfas", "Retiro"),
    "sanchinarro": ("Valdefuentes", "Hortaleza"),
    "vallehermoso": ("Vallehermoso", "Chamberí"),
}


def _is_city_or_postal_only_part(value: str) -> bool:
    clean = POSTAL_CODE_PATTERN.sub(" ", value)
    clean = re.sub(r"\b(madrid(?:\s+capital|\s+centro)?|espana|spain)\b", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return not clean


def _strip_trailing_unit_noise(value: str) -> str:
    text = value.strip()
    if not text or not STREET_TYPE_PATTERN.search(text):
        return text

    number_match = re.search(r"\b\d{1,4}[a-z]?\b", text)
    if not number_match:
        return text

    prefix = text[:number_match.end()].strip()
    tail = text[number_match.end():].strip(" ,")
    if not tail or tail == "bis":
        return text

    if UNIT_TAIL_PATTERN.match(tail):
        return prefix

    return text


def _cleanup_address_noise(text: str) -> str:
    parts = _split_address_parts_from_text(text)
    cleaned_parts: list[str] = []

    for idx, raw_part in enumerate(parts):
        part = raw_part.strip()
        if not part:
            continue

        if _is_city_or_postal_only_part(part):
            continue

        if idx > 0 and UNIT_ONLY_PATTERN.match(part):
            continue

        part = _strip_trailing_unit_noise(part)
        part = POSTAL_CODE_PATTERN.sub(" ", part)
        if any(ch.isdigit() for ch in part):
            part = TRAILING_CITY_PATTERN.sub("", part)
        part = re.sub(r"\s+", " ", part).strip(" ,")

        if part:
            cleaned_parts.append(part)

    if not cleaned_parts:
        fallback = POSTAL_CODE_PATTERN.sub(" ", text)
        fallback = TRAILING_CITY_PATTERN.sub("", fallback)
        fallback = re.sub(r"\s+", " ", fallback).strip(" ,")
        return fallback

    return ", ".join(cleaned_parts)


def canonical_street_type(value: str | None) -> str | None:
    text = normalize_geo_text(value)
    if not text:
        return None
    return STREET_TYPE_ALIASES.get(text, text)


def normalize_street_name_only(value: str | None) -> str | None:
    text = normalize_geo_text(value)
    if not text:
        return None

    text = STREET_TYPE_PATTERN.sub("", text).strip()
    text = re.sub(r"\bnumero\b", " ", text)
    text = re.sub(r"\b\d{1,4}[a-zA-Z]?\b", " ", text)
    text = re.sub(r"\bn[ºo]\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^(de la|de las|de los|del|de l|de)\s+", "", text).strip()

    return text or None


def parse_house_number(value: str | None) -> tuple[str | None, str | None]:
    text = normalize_geo_text(value)
    if not text:
        return None, None

    match = HOUSE_NUMBER_PATTERN.search(text)
    if not match:
        return None, None

    number = match.group(1)
    suffix = match.group(2).upper() if match.group(2) else None
    return number, suffix


def extract_street_type(value: str | None) -> str | None:
    text = normalize_geo_text(value)
    if not text:
        return None

    match = STREET_TYPE_PATTERN.search(text)
    if not match:
        return None

    return canonical_street_type(match.group(1))


def normalize_neighborhood_key(value: str | None) -> str | None:
    text = normalize_geo_text(value)
    if not text:
        return None

    text = re.sub(r"[()]+", " ", text)
    text = re.sub(r"\b(barrio|zona)\b$", "", text).strip()
    text = text.replace("_", " ")
    text = " ".join(text.split())
    return text or None


def resolve_known_neighborhood(value: str | None) -> tuple[str | None, str | None]:
    key = normalize_neighborhood_key(value)
    if not key:
        return None, None

    if key in KNOWN_NEIGHBORHOODS:
        return KNOWN_NEIGHBORHOODS[key]

    return None, None


def build_street_lookup_key(
    street_type: str | None,
    street_name: str | None,
) -> str | None:
    name = normalize_street_name_only(street_name)
    if not name:
        return None

    st_type = canonical_street_type(street_type)
    if st_type:
        return f"{st_type}|{name}"
    return name


def extract_house_number_from_parts(parts: list[str]) -> tuple[str | None, str | None]:
    if not parts:
        return None, None

    for part in parts:
        number, suffix = parse_house_number(part)
        if number:
            return number, suffix

    return None, None


@dataclass(slots=True)
class ParsedAddress:
    raw_text: str | None
    clean_text: str | None
    street_type: str | None
    street_name: str | None
    lookup_key: str | None
    house_number: str | None
    house_suffix: str | None


@dataclass(slots=True)
class StreetMatch:
    matched: bool
    confidence: float
    match_type: str
    street_code: str | None
    street_type: str | None
    street_name: str | None
    street_literal: str | None
    district_names: list[str]
    neighborhood: str | None
    district: str | None
    postal_code: str | None
    lat: float | None
    lon: float | None


def parse_address_text(value: str | None) -> ParsedAddress:
    clean = normalize_geo_text(value)
    if not clean:
        return ParsedAddress(
            raw_text=value,
            clean_text=None,
            street_type=None,
            street_name=None,
            lookup_key=None,
            house_number=None,
            house_suffix=None,
        )

    parts = split_address_parts(value)

    neighborhood_name, _district = resolve_known_neighborhood(parts[0] if parts else clean)
    if neighborhood_name and not STREET_TYPE_PATTERN.search(parts[0] if parts else clean):
        return ParsedAddress(
            raw_text=value,
            clean_text=clean,
            street_type=None,
            street_name=None,
            lookup_key=None,
            house_number=None,
            house_suffix=None,
        )

    main_part = parts[0] if parts else clean

    street_type = extract_street_type(main_part)
    street_name = normalize_street_name_only(main_part)

    if not street_name and main_part:
        candidate = re.sub(r"\b\d{1,4}[a-zA-Z]?\b", " ", main_part)
        candidate = re.sub(r"\s+", " ", candidate).strip()
        street_name = normalize_street_name_only(candidate)

    lookup_key = build_street_lookup_key(street_type, street_name)

    house_number, house_suffix = extract_house_number_from_parts(parts)
    if not house_number:
        house_number, house_suffix = parse_house_number(clean)

    return ParsedAddress(
        raw_text=value,
        clean_text=clean,
        street_type=street_type,
        street_name=street_name,
        lookup_key=lookup_key,
        house_number=house_number,
        house_suffix=house_suffix,
    )


class MadridStreetCatalog:
    def __init__(self, payload: dict):
        self.version = payload.get("version")
        self.source = payload.get("source", {})
        self.streets = payload.get("streets", [])
        self.address_points = payload.get("address_points", {})
        self.by_lookup_key = payload.get("by_lookup_key", {})
        self.by_name_only = payload.get("by_name_only", {})
        self.by_street_code = payload.get("by_street_code", {})

        self._choices_lookup = list(self.by_lookup_key.keys())
        self._choices_name = list(self.by_name_only.keys())

    @classmethod
    def from_file(cls, path: str | Path) -> "MadridStreetCatalog":
        payload = orjson.loads(Path(path).read_bytes())
        return cls(payload)

    def resolve(self, address_text: str | None) -> StreetMatch:
        parsed = parse_address_text(address_text)

        if not parsed.clean_text:
            return StreetMatch(
                matched=False,
                confidence=0.0,
                match_type="empty",
                street_code=None,
                street_type=None,
                street_name=None,
                street_literal=None,
                district_names=[],
                neighborhood=None,
                district=None,
                postal_code=None,
                lat=None,
                lon=None,
            )

        neighborhood_name, district_name = resolve_known_neighborhood(parsed.clean_text)
        if neighborhood_name and not parsed.street_name:
            return StreetMatch(
                matched=True,
                confidence=0.72,
                match_type="neighborhood_only",
                street_code=None,
                street_type=None,
                street_name=None,
                street_literal=None,
                district_names=[district_name] if district_name else [],
                neighborhood=neighborhood_name,
                district=district_name,
                postal_code=None,
                lat=None,
                lon=None,
            )

        if parsed.lookup_key and parsed.lookup_key in self.by_lookup_key:
            street_code = self.by_lookup_key[parsed.lookup_key]
            return self._build_match_from_code(
                street_code=street_code,
                parsed=parsed,
                confidence=1.0,
                match_type="exact_lookup_key",
            )

        name_only = normalize_street_name_only(parsed.street_name)
        if name_only and name_only in self.by_name_only:
            codes = self.by_name_only[name_only]
            if len(codes) == 1:
                return self._build_match_from_code(
                    street_code=codes[0],
                    parsed=parsed,
                    confidence=0.96,
                    match_type="exact_name_only",
                )

        if parsed.lookup_key and self._choices_lookup:
            result = process.extractOne(
                parsed.lookup_key,
                self._choices_lookup,
                scorer=fuzz.WRatio,
                score_cutoff=92,
            )
            if result:
                best_key, score, _ = result
                street_code = self.by_lookup_key[best_key]
                return self._build_match_from_code(
                    street_code=street_code,
                    parsed=parsed,
                    confidence=round(score / 100.0, 4),
                    match_type="fuzzy_lookup_key",
                )

        if name_only and self._choices_name:
            result = process.extractOne(
                name_only,
                self._choices_name,
                scorer=fuzz.WRatio,
                score_cutoff=92,
            )
            if result:
                best_name, score, _ = result
                codes = self.by_name_only[best_name]
                if len(codes) == 1:
                    return self._build_match_from_code(
                        street_code=codes[0],
                        parsed=parsed,
                        confidence=round((score / 100.0) * 0.97, 4),
                        match_type="fuzzy_name_only",
                    )

        parts = split_address_parts(address_text)
        if parts:
            neighborhood_name, district_name = resolve_known_neighborhood(parts[-1])
            if neighborhood_name:
                return StreetMatch(
                    matched=True,
                    confidence=0.68,
                    match_type="trailing_neighborhood_only",
                    street_code=None,
                    street_type=parsed.street_type,
                    street_name=parsed.street_name,
                    street_literal=None,
                    district_names=[district_name] if district_name else [],
                    neighborhood=neighborhood_name,
                    district=district_name,
                    postal_code=None,
                    lat=None,
                    lon=None,
                )

        return StreetMatch(
            matched=False,
            confidence=0.0,
            match_type="not_found",
            street_code=None,
            street_type=parsed.street_type,
            street_name=parsed.street_name,
            street_literal=None,
            district_names=[],
            neighborhood=None,
            district=None,
            postal_code=None,
            lat=None,
            lon=None,
        )

    def _build_match_from_code(
        self,
        street_code: str,
        parsed: ParsedAddress,
        confidence: float,
        match_type: str,
    ) -> StreetMatch:
        street = self.by_street_code.get(street_code) or {}
        point = self._resolve_address_point(street_code, parsed.house_number)

        district = None
        neighborhood = None
        postal_code = None
        lat = None
        lon = None

        if point:
            district = point.get("district_name")
            neighborhood = point.get("neighborhood_name")
            postal_code = point.get("postal_code")
            lat = point.get("lat")
            lon = point.get("lon")

        if not district:
            districts = street.get("district_names") or []
            district = districts[0] if len(districts) == 1 else None

        return StreetMatch(
            matched=True,
            confidence=confidence,
            match_type=match_type,
            street_code=street.get("street_code"),
            street_type=street.get("street_type"),
            street_name=street.get("street_name"),
            street_literal=street.get("street_literal"),
            district_names=street.get("district_names") or [],
            neighborhood=neighborhood,
            district=district,
            postal_code=postal_code,
            lat=lat,
            lon=lon,
        )

    def _resolve_address_point(
        self,
        street_code: str,
        house_number: str | None,
    ) -> dict | None:
        if not house_number:
            return None

        by_number = self.address_points.get(street_code) or {}
        if not by_number:
            return None

        if house_number in by_number:
            return by_number[house_number]

        try:
            target = int(house_number)
        except Exception:
            return None

        numeric_candidates: list[tuple[int, dict]] = []
        for num_str, payload in by_number.items():
            try:
                numeric_candidates.append((int(num_str), payload))
            except Exception:
                continue

        if not numeric_candidates:
            return None

        nearest_num, nearest_payload = min(
            numeric_candidates,
            key=lambda item: abs(item[0] - target),
        )

        if abs(nearest_num - target) <= 6:
            return nearest_payload

        return None
