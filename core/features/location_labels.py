from __future__ import annotations

from core.normalization.text import normalize_text


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
    "Recoletos": "Recoletos",
    "Puerta De Hierro": "Puerta de Hierro",
    "Arguelles": "Argüelles",
    "La Concepcion": "La Concepción",
    "Fontarron": "Fontarrón",
    "Legazpi": "Legazpi",
    "Bellas Vistas": "Bellas Vistas",
    "Valdeacederas": "Valdeacederas",
    "Valdefuentes": "Valdefuentes",
    "Justicia": "Justicia",
    "Simancas": "Simancas",
    "Prosperidad": "Prosperidad",
    "Rosas": "Rosas",
    "Alameda De Osuna": "Alameda de Osuna",
    "La Paz": "La Paz",
    "El Pilar": "El Pilar",
    "El Viso": "El Viso",
    "Pinar Del Rey": "Pinar del Rey",
    "Cuatro Caminos": "Cuatro Caminos",
    "Palos De La Frontera": "Palos de la Frontera",
}


def _base_clean(value: str | None) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    text = text.strip(" ,-/")
    text = " ".join(text.split())
    return text or None


def canonical_zone_label(value: str | None) -> str | None:
    text = _base_clean(value)
    if not text:
        return None

    key = text.lower()
    if key in ZONE_LABEL_ALIASES:
        return ZONE_LABEL_ALIASES[key]

    text = text.replace(" - Barrio", "")
    text = text.replace(" - Zona", "")
    text = text.replace("(Recoletos)", "Recoletos")
    text = " ".join(text.split()).strip(" -,")

    if text.upper() == text:
        text = text.title()

    return PRETTY_REPLACEMENTS.get(text, text)