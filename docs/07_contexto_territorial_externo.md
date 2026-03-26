# Contexto Territorial Externo

## Objetivo

Levantar una capa grande y reutilizable de fuentes publicas oficiales para Madrid que sirva de base a:

- radar por zona mas contextual
- cola operativa con mejores explicaciones
- comparables y captacion con contexto estructural
- joins futuros por distrito, barrio, via, coordenada o ambito geografico

## Que deja este bloque

1. Harvest completo de metadatos oficiales via CKAN para:
   - Ayuntamiento de Madrid
   - Comunidad de Madrid
2. Catalogo focalizado para uso inmobiliario y territorial.
3. Descargador de recursos reales con filtros por tema, portal y limite de tamano.
4. Capa de servicio para que Radar, Cola o nuevas vistas puedan leer el catalogo sin rehacer parsing.

## Archivos principales

- `core/discovery/external_context_catalog.py`
- `core/services/external_context_service.py`
- `tools/build_external_context_catalog.py`
- `tools/download_external_context_resources.py`

## Salidas generadas

- `data/raw/external_context/*_packages_raw.json`
- `data/processed/madrid_external_context_catalog_full.json`
- `data/processed/madrid_external_context_catalog_focus.json`
- `data/processed/madrid_external_context_catalog_summary.json`
- `data/raw/external_context/resources/**`

## Cobertura tematica

El clasificador agrupa datasets en estas familias:

- `boundaries_geography`
- `demography`
- `housing_urbanism`
- `socioeconomic`
- `amenities_services`
- `mobility_access`
- `environment`
- `safety_incidents`

## Fuentes oficiales incluidas

Portales CKAN:

- Ayuntamiento de Madrid: `https://datos.madrid.es`
- Comunidad de Madrid: `https://datos.comunidad.madrid`

Fuentes manuales externas ya referenciadas en el catalogo:

- Catastro: `https://www.sedecatastro.gob.es/`
- INE WSTempus: `https://servicios.ine.es/wstempus/`
- Observatorio de Vivienda y Suelo: `https://publicaciones.transportes.gob.es/observatorio-de-vivienda-y-suelo-boletin-anual-2024`

## Comandos

Generar catalogo:

```powershell
.\.venv\Scripts\python.exe tools\build_external_context_catalog.py
```

Descargar una tanda amplia de recursos:

```powershell
.\.venv\Scripts\python.exe tools\download_external_context_resources.py --limit 30 --max-size-mb 20
```

Descargar solo vivienda y urbanismo:

```powershell
.\.venv\Scripts\python.exe tools\download_external_context_resources.py --theme housing_urbanism --limit 20 --max-size-mb 25
```

## Siguiente uso recomendado

Cuando queramos cruzar esta capa con el nucleo:

1. unificar joins por barrio y distrito
2. crear tablas derivadas por zona
3. sumar estas metricas al radar y a la cola
4. abrir una vista de catalogo/descargas para inspeccion operativa
