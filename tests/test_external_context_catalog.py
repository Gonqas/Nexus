from __future__ import annotations

from core.discovery.external_context_catalog import (
    build_catalog_from_raw,
    normalize_ckan_package,
    select_download_candidates,
)


MADRID_PORTAL = {
    "id": "ayuntamiento_madrid",
    "name": "Ayuntamiento de Madrid - Datos Abiertos",
    "base_url": "https://datos.madrid.es",
    "api_url": "https://datos.madrid.es/api/3/action/package_search",
    "catalog_url": "https://datos.madrid.es",
    "geography_scope": "madrid_city",
}


def test_normalize_ckan_package_scores_demography_and_geography() -> None:
    package = {
        "id": "pkg-1",
        "name": "poblacion-barrio",
        "title": "Poblacion por distrito y barrio a 1 de enero",
        "notes": "Serie anual del padron con poblacion, edad y sexo por distrito y barrio.",
        "tags": [{"display_name": "demografia"}, {"display_name": "barrios"}],
        "groups": [],
        "resources": [
            {
                "id": "res-1",
                "name": "poblacion.csv",
                "format": "CSV",
                "url": "https://datos.madrid.es/download/poblacion.csv",
                "size": 2048,
            }
        ],
        "granularity": "Barrio / Distrito",
        "frequency": "Anual",
        "metadata_created": "2026-01-01T00:00:00",
        "metadata_modified": "2026-01-02T00:00:00",
        "issued": "2026-01-01T00:00:00",
        "modified": "2026-01-02T00:00:00",
        "license_title": "CC-BY",
        "license_url": "https://example.com/license",
    }

    dataset = normalize_ckan_package(MADRID_PORTAL, package)

    assert dataset["primary_theme"] == "demography"
    assert dataset["focus_score"] >= 7
    assert dataset["is_focus"] is True
    assert dataset["has_tabular"] is True
    assert dataset["dataset_url"] == "https://datos.madrid.es/dataset/poblacion-barrio"


def test_build_catalog_from_raw_summarizes_portals_and_themes() -> None:
    raw_by_portal = {
        "ayuntamiento_madrid": [
            {
                "id": "pkg-1",
                "name": "barrios",
                "title": "Barrios municipales de Madrid",
                "notes": "Limites administrativos y cartografia oficial por barrio.",
                "tags": [{"display_name": "geoportal"}],
                "groups": [],
                "resources": [
                    {
                        "id": "res-1",
                        "name": "barrios.geojson",
                        "format": "GeoJSON",
                        "url": "https://datos.madrid.es/download/barrios.geojson",
                    }
                ],
                "granularity": "Barrio",
            }
        ],
        "comunidad_madrid": [
            {
                "id": "pkg-2",
                "name": "hospitales",
                "title": "Hospitales y datos asistenciales",
                "notes": "Relacion de hospitales y actividad asistencial.",
                "tags": [{"display_name": "hospitales"}],
                "groups": [],
                "resources": [
                    {
                        "id": "res-2",
                        "name": "hospitales.csv",
                        "format": "CSV",
                        "url": "https://datos.comunidad.madrid/download/hospitales.csv",
                    }
                ],
                "granularity": "Municipio",
            }
        ],
    }

    full_catalog, focus_catalog, summary = build_catalog_from_raw(raw_by_portal)

    assert full_catalog["dataset_count"] == 2
    assert focus_catalog["dataset_count"] == 2
    assert summary["portals"]["ayuntamiento_madrid"]["dataset_count"] == 1
    assert summary["portals"]["comunidad_madrid"]["focus_count"] == 1
    assert "boundaries_geography" in summary["themes_focus"]


def test_select_download_candidates_filters_by_theme_and_format() -> None:
    focus_catalog = {
        "datasets": [
            {
                "title": "Barrios municipales de Madrid",
                "dataset_url": "https://datos.madrid.es/dataset/barrios",
                "portal_id": "ayuntamiento_madrid",
                "primary_theme": "boundaries_geography",
                "focus_score": 12,
                "resources": [
                    {
                        "name": "barrios.geojson",
                        "format": "GEOJSON",
                        "url": "https://datos.madrid.es/download/barrios.geojson",
                        "downloadable": True,
                    },
                    {
                        "name": "barrios.pdf",
                        "format": "PDF",
                        "url": "https://datos.madrid.es/download/barrios.pdf",
                        "downloadable": True,
                    },
                ],
            }
        ]
    }

    candidates = select_download_candidates(
        focus_catalog,
        theme="boundaries_geography",
        allowed_formats={"geojson"},
    )

    assert len(candidates) == 1
    assert candidates[0]["resource"]["format"] == "GEOJSON"
