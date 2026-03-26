from __future__ import annotations

import math
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


CKAN_PAGE_SIZE = 100
REQUEST_HEADERS = {
    "User-Agent": "nexus-madrid/0.1 external-context-harvester",
}

OFFICIAL_PORTALS = (
    {
        "id": "ayuntamiento_madrid",
        "name": "Ayuntamiento de Madrid - Datos Abiertos",
        "base_url": "https://datos.madrid.es",
        "api_url": "https://datos.madrid.es/api/3/action/package_search",
        "catalog_url": "https://datos.madrid.es",
        "geography_scope": "madrid_city",
    },
    {
        "id": "comunidad_madrid",
        "name": "Comunidad de Madrid - Datos Abiertos",
        "base_url": "https://datos.comunidad.madrid",
        "api_url": "https://datos.comunidad.madrid/api/3/action/package_search",
        "catalog_url": "https://datos.comunidad.madrid",
        "geography_scope": "community_region",
    },
)

MANUAL_SOURCES = (
    {
        "source_id": "catastro_servicios",
        "provider": "Direccion General del Catastro",
        "title": "Sede Electronica del Catastro",
        "url": "https://www.sedecatastro.gob.es/",
        "source_type": "manual_service",
        "themes": ["housing_urbanism", "boundaries_geography"],
        "coverage": "spain_municipal",
        "notes": (
            "Servicios web, descargas cartograficas y alfanumericas, INSPIRE y estadistica "
            "catastral. Fuente clave para parcelas, inmuebles y geometria oficial."
        ),
    },
    {
        "source_id": "ine_api",
        "provider": "Instituto Nacional de Estadistica",
        "title": "INEbase API / WSTempus",
        "url": "https://servicios.ine.es/wstempus/",
        "source_type": "manual_service",
        "themes": ["demography", "socioeconomic"],
        "coverage": "spain_statistical",
        "notes": (
            "API oficial de series y tablas estadisticas del INE. Util para reforzar "
            "demografia, hogares, renta y series comparables."
        ),
    },
    {
        "source_id": "mivau_observatorio",
        "provider": "Ministerio de Vivienda y Agenda Urbana",
        "title": "Observatorio de Vivienda y Suelo",
        "url": "https://publicaciones.transportes.gob.es/observatorio-de-vivienda-y-suelo-boletin-anual-2024",
        "source_type": "manual_service",
        "themes": ["housing_urbanism", "socioeconomic"],
        "coverage": "spain_regional",
        "notes": (
            "Serie oficial con indicadores agregados de vivienda y suelo. No es granular por "
            "barrio, pero si util para contexto macro y contraste externo."
        ),
    },
)

THEME_KEYWORDS: dict[str, tuple[str, ...]] = {
    "boundaries_geography": (
        "barrio",
        "barrios",
        "distrito",
        "distritos",
        "seccion censal",
        "callejero",
        "cartografia",
        "geojson",
        "geolocalizacion",
        "limites administrativos",
        "zonificacion",
        "portal",
    ),
    "demography": (
        "poblacion",
        "padron",
        "habitantes",
        "edad",
        "sexo",
        "hogares",
        "nacionalidad",
        "demografia",
        "nacimientos",
        "migracion",
    ),
    "housing_urbanism": (
        "vivienda",
        "viviendas",
        "urban",
        "urbanismo",
        "licencias urbanisticas",
        "licencia",
        "declaraciones responsables",
        "catastro",
        "suelo",
        "turistico",
        "arrendamiento",
        "alquiler",
        "mercado inmobiliario",
        "planeamiento",
    ),
    "socioeconomic": (
        "renta",
        "paro",
        "desempleo",
        "vulnerabilidad",
        "iguala",
        "servicios sociales",
        "economica",
        "actividad economica",
        "hogares",
        "ingresos",
    ),
    "amenities_services": (
        "equipamientos",
        "mercados",
        "colegios",
        "centros educativos",
        "hospitales",
        "salud",
        "bibliotecas",
        "deportes",
        "servicios",
        "centros",
        "parques",
        "zonas verdes",
    ),
    "mobility_access": (
        "transporte",
        "emt",
        "metro",
        "bicimad",
        "aparcamientos",
        "movilidad",
        "trafico",
        "autobus",
        "lineas",
        "accesibilidad",
    ),
    "environment": (
        "calidad del aire",
        "aire",
        "ruido",
        "acustica",
        "arbolado",
        "zonas verdes",
        "temperatura",
        "clima",
        "contaminacion",
        "emisiones",
    ),
    "safety_incidents": (
        "accidentes",
        "incidencias",
        "seguridad",
        "siniestralidad",
        "emergencias",
        "trafico",
    ),
}

DOWNLOADABLE_FORMATS = {
    "csv",
    "json",
    "geojson",
    "xlsx",
    "xls",
    "zip",
    "txt",
    "xml",
    "kml",
    "kmz",
    "shp",
    "gpkg",
    "pdf",
}

GEOSPATIAL_FORMATS = {"geojson", "kml", "kmz", "shp", "gpkg", "wms", "wfs"}
TABULAR_FORMATS = {"csv", "xlsx", "xls", "json", "xml", "txt"}
FOCUS_THRESHOLD = 7


def _normalize_text(value: object | None) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().split())


def _slugify(value: str) -> str:
    normalized = _normalize_text(value)
    safe = []
    for char in normalized:
        safe.append(char if char.isalnum() else "_")
    slug = "".join(safe).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "dataset"


def _excerpt(value: object | None, limit: int = 1200) -> str:
    text = "" if value is None else str(value).strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}..."


def _resource_format(resource: dict[str, Any]) -> str:
    return _normalize_text(resource.get("format") or resource.get("mimetype") or "")


def _bool_downloadable(resource: dict[str, Any]) -> bool:
    fmt = _resource_format(resource)
    url = str(resource.get("url") or "")
    if fmt in DOWNLOADABLE_FORMATS:
        return True
    return url.startswith("http://") or url.startswith("https://")


def _dataset_url(portal: dict[str, Any], package: dict[str, Any]) -> str:
    package_name = package.get("name")
    if package_name:
        return f"{portal['base_url']}/dataset/{package_name}"
    return portal["catalog_url"]


def _collect_keywords(package: dict[str, Any]) -> dict[str, str]:
    tags = " ".join(
        str(tag.get("display_name") or tag.get("name") or "").strip()
        for tag in package.get("tags", [])
    )
    groups = " ".join(
        str(group.get("display_name") or group.get("title") or "").strip()
        for group in package.get("groups", [])
    )
    resources = " ".join(
        " ".join(
            str(resource.get(field) or "").strip()
            for field in ("name", "description", "format", "mimetype")
        )
        for resource in package.get("resources", [])
    )
    return {
        "title": _normalize_text(package.get("title")),
        "notes": _normalize_text(package.get("notes")),
        "tags": _normalize_text(tags),
        "groups": _normalize_text(groups),
        "resources": _normalize_text(resources),
        "granularity": _normalize_text(package.get("granularity")),
        "frequency": _normalize_text(package.get("frequency")),
    }


def _score_theme(text_fields: dict[str, str], keywords: tuple[str, ...]) -> tuple[int, list[str]]:
    score = 0
    hits: list[str] = []
    for keyword in keywords:
        token = _normalize_text(keyword)
        if not token:
            continue
        field_hit = False
        if token in text_fields["title"]:
            score += 5
            field_hit = True
        if token in text_fields["tags"]:
            score += 4
            field_hit = True
        if token in text_fields["groups"]:
            score += 3
            field_hit = True
        if token in text_fields["notes"]:
            score += 2
            field_hit = True
        if token in text_fields["resources"]:
            score += 1
            field_hit = True
        if field_hit and keyword not in hits:
            hits.append(keyword)
    return score, hits


def _resource_summaries(resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for resource in resources:
        fmt = _resource_format(resource)
        normalized.append(
            {
                "resource_id": resource.get("id"),
                "name": resource.get("name"),
                "description": _excerpt(resource.get("description"), limit=300),
                "format": fmt.upper() if fmt else None,
                "url": resource.get("url"),
                "size_bytes": resource.get("size"),
                "mimetype": resource.get("mimetype"),
                "last_modified": resource.get("last_modified") or resource.get("metadata_modified"),
                "downloadable": _bool_downloadable(resource),
            }
        )
    return normalized


def normalize_ckan_package(portal: dict[str, Any], package: dict[str, Any]) -> dict[str, Any]:
    text_fields = _collect_keywords(package)
    theme_scores: dict[str, int] = {}
    theme_hits: dict[str, list[str]] = {}

    for theme, keywords in THEME_KEYWORDS.items():
        score, hits = _score_theme(text_fields, keywords)
        theme_scores[theme] = score
        theme_hits[theme] = hits

    resources = _resource_summaries(package.get("resources", []))
    formats = Counter(resource["format"] for resource in resources if resource.get("format"))
    has_geospatial = any(_normalize_text(fmt) in GEOSPATIAL_FORMATS for fmt in formats)
    has_tabular = any(_normalize_text(fmt) in TABULAR_FORMATS for fmt in formats)
    granularity = _normalize_text(package.get("granularity"))
    title_notes = f"{text_fields['title']} {text_fields['notes']}"

    if "barrio" in granularity or "distrito" in granularity:
        theme_scores["boundaries_geography"] += 2
        theme_scores["demography"] += 2

    if any(token in title_notes for token in ("poblacion", "padron", "habitantes", "demografia")):
        theme_scores["demography"] += 2
    if any(token in title_notes for token in ("vivienda", "licencia", "urbanismo", "catastro")):
        theme_scores["housing_urbanism"] += 2

    if has_geospatial:
        theme_scores["boundaries_geography"] += 2
        theme_scores["environment"] += 1
    if has_tabular:
        theme_scores["demography"] += 1
        theme_scores["housing_urbanism"] += 1

    primary_theme = max(theme_scores, key=lambda key: theme_scores[key])
    focus_score = int(theme_scores[primary_theme])

    special_focus = any(
        token in text_fields["title"]
        for token in (
            "barrio",
            "distrito",
            "vivienda",
            "licencias urbanisticas",
            "padron",
            "equipamientos",
            "calidad del aire",
            "zonas verdes",
        )
    )
    is_focus = focus_score >= FOCUS_THRESHOLD or special_focus

    return {
        "source_type": "ckan_dataset",
        "portal_id": portal["id"],
        "portal_name": portal["name"],
        "geography_scope": portal["geography_scope"],
        "dataset_id": package.get("id"),
        "package_name": package.get("name"),
        "title": package.get("title"),
        "dataset_url": _dataset_url(portal, package),
        "notes_excerpt": _excerpt(package.get("notes")),
        "tags": [
            str(tag.get("display_name") or tag.get("name") or "").strip()
            for tag in package.get("tags", [])
            if str(tag.get("display_name") or tag.get("name") or "").strip()
        ],
        "groups": [
            str(group.get("display_name") or group.get("title") or "").strip()
            for group in package.get("groups", [])
            if str(group.get("display_name") or group.get("title") or "").strip()
        ],
        "license_title": package.get("license_title"),
        "license_url": package.get("license_url"),
        "metadata_created": package.get("metadata_created"),
        "metadata_modified": package.get("metadata_modified"),
        "issued": package.get("issued"),
        "modified": package.get("modified"),
        "frequency": package.get("frequency"),
        "granularity": package.get("granularity"),
        "num_resources": package.get("num_resources") or len(resources),
        "resources": resources,
        "formats": dict(formats),
        "has_geospatial": has_geospatial,
        "has_tabular": has_tabular,
        "theme_scores": theme_scores,
        "theme_hits": theme_hits,
        "primary_theme": primary_theme,
        "focus_score": focus_score,
        "is_focus": is_focus,
    }


def fetch_ckan_packages(portal: dict[str, Any], *, page_size: int = CKAN_PAGE_SIZE) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = []
    start = 0
    total = math.inf

    while start < total:
        query = urlencode({"rows": page_size, "start": start})
        request = Request(f"{portal['api_url']}?{query}", headers=REQUEST_HEADERS)
        payload = None
        last_error = None
        for attempt in range(3):
            try:
                with urlopen(request, timeout=120) as response:
                    payload = __import__("json").loads(response.read())
                break
            except Exception as exc:
                last_error = exc
                time.sleep(0.5 * (attempt + 1))
        if payload is None:
            raise last_error or RuntimeError(f"CKAN package_search failed for {portal['id']}")
        if not payload.get("success"):
            raise RuntimeError(f"CKAN package_search failed for {portal['id']}")

        result = payload.get("result") or {}
        total = int(result.get("count") or 0)
        rows = list(result.get("results") or [])
        if not rows:
            break

        packages.extend(rows)
        start += len(rows)
        time.sleep(0.05)

    return packages


def _theme_counter(datasets: list[dict[str, Any]]) -> dict[str, int]:
    counter = Counter(dataset["primary_theme"] for dataset in datasets if dataset.get("primary_theme"))
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _portal_summary(full: list[dict[str, Any]], focus: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for portal in OFFICIAL_PORTALS:
        portal_full = [dataset for dataset in full if dataset.get("portal_id") == portal["id"]]
        portal_focus = [dataset for dataset in focus if dataset.get("portal_id") == portal["id"]]
        result[portal["id"]] = {
            "portal_name": portal["name"],
            "dataset_count": len(portal_full),
            "focus_count": len(portal_focus),
            "theme_breakdown": _theme_counter(portal_focus),
        }
    return result


def _top_focus_by_theme(focus: list[dict[str, Any]], limit: int = 5) -> dict[str, list[dict[str, Any]]]:
    by_theme: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ordered = sorted(
        focus,
        key=lambda item: (-int(item.get("focus_score") or 0), item.get("title") or ""),
    )
    for dataset in ordered:
        theme = dataset.get("primary_theme") or "other"
        if len(by_theme[theme]) >= limit:
            continue
        by_theme[theme].append(
            {
                "title": dataset.get("title"),
                "portal_id": dataset.get("portal_id"),
                "dataset_url": dataset.get("dataset_url"),
                "focus_score": dataset.get("focus_score"),
            }
        )
    return dict(by_theme)


def build_catalog_from_raw(
    raw_by_portal: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    portal_lookup = {portal["id"]: portal for portal in OFFICIAL_PORTALS}
    full_datasets: list[dict[str, Any]] = []

    for portal_id, packages in raw_by_portal.items():
        portal = portal_lookup[portal_id]
        for package in packages:
            full_datasets.append(normalize_ckan_package(portal, package))

    full_datasets.sort(key=lambda item: (item["portal_id"], item["title"] or ""))
    focus_datasets = [dataset for dataset in full_datasets if dataset.get("is_focus")]
    focus_datasets.sort(
        key=lambda item: (-int(item.get("focus_score") or 0), item["portal_id"], item["title"] or "")
    )

    generated_at = datetime.now(UTC).isoformat()
    full_catalog = {
        "generated_at": generated_at,
        "dataset_count": len(full_datasets),
        "portals": [portal["id"] for portal in OFFICIAL_PORTALS],
        "datasets": full_datasets,
    }
    focus_catalog = {
        "generated_at": generated_at,
        "dataset_count": len(focus_datasets),
        "datasets": focus_datasets,
        "manual_sources": list(MANUAL_SOURCES),
    }
    summary = {
        "generated_at": generated_at,
        "dataset_count_full": len(full_datasets),
        "dataset_count_focus": len(focus_datasets),
        "manual_sources_count": len(MANUAL_SOURCES),
        "themes_full": _theme_counter(full_datasets),
        "themes_focus": _theme_counter(focus_datasets),
        "portals": _portal_summary(full_datasets, focus_datasets),
        "top_focus_by_theme": _top_focus_by_theme(focus_datasets),
    }

    return full_catalog, focus_catalog, summary


def harvest_catalog() -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    raw_by_portal: dict[str, list[dict[str, Any]]] = {}
    for portal in OFFICIAL_PORTALS:
        raw_by_portal[portal["id"]] = fetch_ckan_packages(portal)

    full_catalog, focus_catalog, summary = build_catalog_from_raw(raw_by_portal)
    return raw_by_portal, full_catalog, focus_catalog, summary


def select_download_candidates(
    focus_catalog: dict[str, Any],
    *,
    theme: str | None = None,
    portal_id: str | None = None,
    limit: int | None = None,
    allowed_formats: set[str] | None = None,
) -> list[dict[str, Any]]:
    datasets = list(focus_catalog.get("datasets") or [])
    results: list[dict[str, Any]] = []
    allowed = {fmt.lower() for fmt in (allowed_formats or DOWNLOADABLE_FORMATS)}

    for dataset in datasets:
        if theme and dataset.get("primary_theme") != theme:
            continue
        if portal_id and dataset.get("portal_id") != portal_id:
            continue

        for resource in dataset.get("resources") or []:
            fmt = _normalize_text(resource.get("format"))
            if fmt and fmt not in allowed:
                continue
            if not resource.get("downloadable"):
                continue
            results.append(
                {
                    "dataset_title": dataset.get("title"),
                    "dataset_url": dataset.get("dataset_url"),
                    "portal_id": dataset.get("portal_id"),
                    "primary_theme": dataset.get("primary_theme"),
                    "focus_score": dataset.get("focus_score"),
                    "resource": resource,
                    "slug": _slugify(
                        f"{dataset.get('portal_id')}_{dataset.get('title')}_{resource.get('name') or resource.get('resource_id') or fmt}"
                    ),
                }
            )

    results.sort(
        key=lambda item: (
            -int(item.get("focus_score") or 0),
            item.get("primary_theme") or "",
            item.get("dataset_title") or "",
        )
    )
    if limit is not None:
        return results[:limit]
    return results


def safe_slug(value: str) -> str:
    return _slugify(value)
