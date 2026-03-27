from __future__ import annotations

import orjson

from core.normalization.text import normalize_text, normalize_text_key
from core.runtime_paths import PROCESSED_RESOURCE_DIR


ZONE_LABEL_ALIASES: dict[str, str] = {
    "hispanoamerica barrio": "Hispanoamérica",
    "hispanoamerica zona": "Hispanoamérica",
    "valdeacederas barrio": "Valdeacederas",
    "moncloa zona": "Moncloa",
    "lavapies zona": "Lavapiés",
    "malasana zona": "Malasaña",
    "almagro barrio": "Almagro",
    "prosperidad barrio": "Prosperidad",
    "marroquina barrio": "Marroquina",
    "la paz barrio": "La Paz",
    "rios rosas barrio": "Ríos Rosas",
    "cortes barrio": "Cortes",
    "chopera barrio": "Chopera",
    "berruguete barrio": "Berruguete",
    "estrella barrio": "Estrella",
    "atocha barrio": "Atocha",
    "vinateros barrio": "Vinateros",
    "nueva espana barrio": "Nueva España",
    "costillares barrio": "Costillares",
    "la guindalera barrio": "Guindalera",
    "el pilar barrio": "El Pilar",
    "lista barrio": "Lista",
    "ilustracion zona": "Ilustración",
    "salvador barrio": "Salvador",
    "acacias barrio": "Acacias",
    "canillas barrio": "Canillas",
    "palos de moguer barrio": "Palos de la Frontera",
    "castillejos barrio": "Castillejos",
    "ciudad jardin barrio": "Ciudad Jardín",
    "cuatro caminos barrio": "Cuatro Caminos",
    "castellana barrio": "Castellana",
    "pacifico barrio": "Pacífico",
    "delicias barrio": "Delicias",
    "penagrande barrio": "Peñagrande",
    "piovera barrio": "Piovera",
    "puerta hierro zona": "Puerta de Hierro",
    "ibiza barrio": "Ibiza",
    "trafalgar barrio": "Trafalgar",
    "sol barrio": "Sol",
    "serrano recoletos zona": "Recoletos",
    "velazquez zona": "Recoletos",
}


PRETTY_REPLACEMENTS: dict[str, str] = {
    "Rios Rosas": "Ríos Rosas",
    "Pacifico": "Pacífico",
    "Nino Jesus": "Niño Jesús",
    "Hispanoamerica": "Hispanoamérica",
    "Chamartin": "Chamartín",
    "Tetuan": "Tetuán",
    "Penagrande": "Peñagrande",
    "Nueva Espana": "Nueva España",
    "Ciudad Jardin": "Ciudad Jardín",
    "Palos De La Frontera": "Palos de la Frontera",
    "Ilustracion": "Ilustración",
    "Puerta De Hierro": "Puerta de Hierro",
    "Arguelles": "Argüelles",
    "La Concepcion": "La Concepción",
    "Fontarron": "Fontarrón",
    "Alameda De Osuna": "Alameda de Osuna",
    "Pinar Del Rey": "Pinar del Rey",
}


ZONE_CONTEXT_PATH = PROCESSED_RESOURCE_DIR / "madrid_zone_external_context.json"
_KNOWN_ZONE_LABELS_CACHE: set[str] | None = None


def _base_clean(value: str | None) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    text = text.strip(" ,-/")
    text = " ".join(text.split())
    return text or None


def _load_known_zone_labels() -> set[str]:
    global _KNOWN_ZONE_LABELS_CACHE

    if _KNOWN_ZONE_LABELS_CACHE is not None:
        return _KNOWN_ZONE_LABELS_CACHE

    labels = set(ZONE_LABEL_ALIASES.values()) | set(PRETTY_REPLACEMENTS.values())

    if ZONE_CONTEXT_PATH.exists():
        try:
            payload = orjson.loads(ZONE_CONTEXT_PATH.read_bytes())
            for bucket_name in ("districts", "neighborhoods"):
                bucket = payload.get(bucket_name) or {}
                for item in bucket.values():
                    zone_label = item.get("zone_label")
                    if zone_label:
                        labels.add(str(zone_label))
        except Exception:
            pass

    _KNOWN_ZONE_LABELS_CACHE = labels
    return labels


def _normalize_candidate(text: str) -> str:
    title_text = text.title() if text.upper() == text else text
    return PRETTY_REPLACEMENTS.get(title_text, title_text)


def is_official_zone_label(value: str | None) -> bool:
    text = _base_clean(value)
    if not text:
        return False

    return _normalize_candidate(text) in _load_known_zone_labels()


def canonical_zone_label(value: str | None) -> str | None:
    text = _base_clean(value)
    if not text:
        return None

    key = normalize_text_key(text)
    if key in ZONE_LABEL_ALIASES:
        return ZONE_LABEL_ALIASES[key]

    cleaned = text.replace(" - Barrio", "")
    cleaned = cleaned.replace(" - Zona", "")
    cleaned = cleaned.replace("(Recoletos)", "Recoletos")
    cleaned = " ".join(cleaned.split()).strip(" -,")

    candidate = _normalize_candidate(cleaned)
    if candidate in _load_known_zone_labels():
        return candidate
    return None
