# Nexus Madrid — Estado actual del proyecto

## 1. Resumen ejecutivo

Nexus Madrid ya ha superado la fase de prototipo frágil.

A día de hoy el proyecto ya dispone de:

- una base de datos relacional funcional,
- una app de escritorio operativa,
- un importador CSV real,
- una sincronización Casafari funcional en primera versión,
- una capa de reconciliación auditable,
- primeros servicios de inteligencia territorial,
- radar y cola operativa,
- y una línea emergente de ingestión desde Telegram.

Todavía **no** es una plataforma totalmente cerrada ni una IA predictiva madura, pero ya **sí es** una base seria de inteligencia inmobiliaria local.

---

## 2. Stack actual

### Tecnologías principales
- **Python 3.13**
- **PySide6** para la app de escritorio
- **SQLite** como base principal
- **SQLAlchemy** como ORM / capa de modelos
- **RapidFuzz** para similitud textual
- librerías analíticas ya presentes para evolución futura (por ejemplo numpy, polars, scikit-learn en entorno)

### Qué implica este stack
El proyecto hoy está pensado como una **desktop analytical app local**, no como SaaS web.

Esto tiene ventajas claras:
- simplicidad de despliegue,
- velocidad para iterar,
- control local del dato,
- menor complejidad infra.

Y también límites:
- menor escalabilidad multiusuario,
- menor accesibilidad remota,
- y más dependencia del entorno local bien configurado.

---

## 3. Estructura de carpetas actual

## Estructura principal del repo
- `app/` → interfaz, vistas y workers
- `core/` → lógica de negocio
- `db/` → modelos, sesión y repositorios
- `tools/` → utilidades de auditoría e import manual
- `data/` → base SQLite, estado, debug y outputs

### Lectura práctica
La estructura física está razonablemente sana. El principal problema actual no es la forma del repo, sino la **falta de documentación de prioridades y alcance**.

---

## 4. Módulos principales detectados en código

### UI
- `app/ui/main_window.py`
- vistas:
  - `dashboard_view.py`
  - `import_view.py`
  - `casafari_links_view.py`
  - `assets_view.py`
  - `zones_view.py`
  - `radar_view.py`
  - `opportunity_queue_view.py`
  - `sync_view.py`

### Workers
- `csv_import_worker.py`
- `casafari_sync_worker.py`
- `casafari_reconcile_worker.py`

### Conectores / ingestión
- `core/connectors/casafari_history_connector.py`
- `core/ingest/csv_loader.py`
- `core/ingest/telegram_loader.py`

### Normalización / parsing
- `core/normalization/addresses.py`
- `core/normalization/phones.py`
- `core/normalization/property_types.py`
- `core/normalization/text.py`
- `core/normalization/urls.py`
- `core/parsers/price_parser.py`
- `core/parsers/telegram_parser.py`

### Identidad / matching
- `core/identity/asset_matcher.py`
- `core/identity/listing_resolver.py`
- `core/identity/telegram_asset_resolver.py`
- `core/identity/telegram_listing_matcher.py`
- `core/identity/telegram_window_matcher.py`

### Servicios
- `casafari_sync_service.py`
- `casafari_semantics_service.py`
- `casafari_reconciliation_service.py`
- `casafari_links_service.py`
- `csv_import_service.py`
- `telegram_import_service.py`
- `zone_intelligence_service.py`
- `zone_intelligence_service_v2.py`
- `zone_detail_service.py`
- `zone_detail_service_v2.py`
- `radar_service.py`
- `radar_service_v2.py`
- `opportunity_queue_service.py`

### Features / scoring
- `zone_features.py`
- `zone_features_v2.py`
- `zone_scoring.py`
- `zone_scoring_v2.py`

---

## 5. Modelo de datos actual

### Entidades principales
- `Building`
- `Asset`
- `Listing`
- `Contact`
- `ListingSnapshot`
- `MarketEvent`
- `RawHistoryItem`
- `SourceSyncState`
- `CasafariEventLink`
- `IngestionRun`
- `TelegramAlert`

### Explicación simple de cada una
- **Building**: edificio
- **Asset**: inmueble real
- **Listing**: anuncio concreto
- **Contact**: persona o contacto asociado
- **ListingSnapshot**: foto puntual del listing en un momento
- **MarketEvent**: evento interpretado del mercado
- **RawHistoryItem**: dato bruto de Casafari aún no consolidado del todo
- **SourceSyncState**: estado de sincronización de una fuente
- **CasafariEventLink**: enlace entre raw Casafari y entidad conocida
- **IngestionRun**: registro de una importación
- **TelegramAlert**: señal procedente de Telegram

### Punto fuerte importante
La separación entre activo, anuncio, snapshot y evento es correcta y evita uno de los errores más frecuentes en este tipo de producto.

---

## 6. Estado del flujo CSV

## Rol actual
El CSV actúa como **baseline enriquecido del inventario**.

### Qué aporta
- stock conocido,
- precio,
- metros,
- tipología,
- portal,
- listing URL,
- contacto,
- fecha base del lead.

### Qué hace el import actual
- valida formato,
- procesa múltiples filas,
- crea o reutiliza entidades,
- crea snapshots,
- registra `IngestionRun`,
- evita duplicación ciega,
- puede borrar el fichero tras import correcto,
- relanza reconciliación posterior.

### Conclusión
Esta es una de las partes más consolidadas del sistema.

---

## 7. Estado del flujo Casafari

## Rol actual
Casafari hoy funciona sobre todo como **fuente de delta / flow / cambio reciente**, no como verdad maestra del detalle completo del activo.

### Qué hace ya
- abre sesión persistida,
- trabaja en ventana temporal incremental,
- usa overlap de seguridad,
- navega páginas,
- captura payloads JSON,
- filtra mejor ruido del frontend,
- crea raws,
- evita duplicar por `source_uid`,
- guarda información de debug.

### Qué se ha mejorado
- menos ruido técnico,
- mejor limpieza de teléfono,
- mejor lectura de dirección,
- menos falsos precios absurdos,
- mejor tipado inicial de evento.

### Limitación vigente
Sigue siendo más fuerte en **detectar cambios** que en describir fichas perfectas.

---

## 8. Estado del matching / reconciliación

## Qué intenta resolver
Determinar si un raw de Casafari corresponde a un listing conocido.

### Señales usadas
- URL
- external_id
- portal
- teléfono
- nombre de contacto
- similitud de dirección
- cercanía de precio

### Estados actuales
- `resolved`
- `ambiguous`
- `unresolved`
- `pending`

### Lectura importante
El sistema es conservador: prefiere dejar casos sin resolver antes que enlazar mal.

Eso es correcto a nivel de filosofía, aunque todavía reduce cobertura.

---

## 9. Estado semántico del dato

Hay una capa semántica ya real, sobre todo en Casafari:

- tipado de evento,
- uso de `historyType`,
- limpieza de precio,
- limpieza de teléfono,
- clasificación aproximada de precisión de dirección,
- reason taxonomy para unresolved,
- bandas de confianza.

### Qué significa
El sistema ya no solo guarda raws; intenta interpretarlos.

### Qué sigue flojo
- eventos demasiado genéricos en algunos casos,
- precio aún imperfecto,
- dirección a veces demasiado zonal,
- contacto incompleto o poco fiable en ciertos raws.

---

## 10. Estado de la inteligencia territorial

El proyecto ya tiene una línea clara de lectura territorial.

### Componentes presentes
- features de zona
- scoring de zona v1 y v2
- servicios de detalle de zona
- radar
- cola operativa

### Idea conceptual actual
Separar:
- **stock** = lo que existe ahora en baseline
- **flow** = lo que está pasando recientemente

### Variables que ya aparecen en diseño
- activos
- listings
- precio medio
- €/m²
- tipologías
- concentración de contactos
- repetición de teléfonos
- nuevos anuncios
- bajadas
- subidas
- sold / reserved / not_available
- absorción
- net new supply

### Scores principales
- `zone_heat_score`
- `zone_pressure_score`
- `zone_liquidity_score`
- `zone_capture_score`
- `zone_confidence_score`
- `recommended_action`

### Limitación principal
La inteligencia territorial existe, pero la geografía aún no está anclada a un sistema oficial suficientemente fuerte. En algunos casos la “zona” todavía depende demasiado de texto libre de dirección.

---

## 11. Estado de la línea Telegram

La base de código revela una tercera línea relevante además de CSV y Casafari.

### Qué existe
- parser de Telegram,
- loader de Telegram,
- import service,
- resolutores específicos,
- modelo `TelegramAlert`,
- herramientas de auditoría.

### Qué significa estratégicamente
El sistema ya no es solo:
- baseline + delta,

sino que empieza a ser:
- inventario,
- cambios de mercado,
- señales externas o manuales.

### Riesgo
Si esta línea crece sin orden puede abrir un producto paralelo antes de consolidar el núcleo.

---

## 12. Estado de la interfaz

Las superficies visibles del producto ya son útiles:

- Dashboard
- Importar CSV
- Casafari Links
- Activos
- Zonas
- Radar
- Cola operativa
- Sync

### Lectura de producto
La app ya no es un esqueleto técnico. Ya es una herramienta con varias superficies que responden a necesidades distintas:

- observación,
- ingestión,
- auditoría,
- lectura territorial,
- priorización.

---

## 13. Foto de datos actual detectada en la base subida

Conteos observados en la base actual del proyecto:

- **368 assets**
- **385 buildings**
- **459 contacts**
- **381 listings**
- **470 listing_snapshots**
- **137 raw_history_items**
- **137 casafari_event_links**
- **6 market_events**
- **1 ingestion_run**
- **0 telegram_alerts**

### Estado de match observado
- **131 unresolved**
- **6 resolved**

### Interpretación
Esto indica que:
- el baseline ya existe,
- Casafari ya está entrando,
- la reconciliación ya está viva,
- pero el sistema todavía está en una fase temprana de cobertura de resolución.

---

## 14. Qué está realmente consolidado

### Confirmado como núcleo funcional
- arranque de la app
- estructura general del sistema
- importación CSV
- trazabilidad con `IngestionRun`
- deduplicación por hash
- sincronización Casafari
- creación de raws
- vista de auditoría Casafari Links
- primeras capas de zonas / radar / opportunity queue

---

## 15. Qué sigue en consolidación

### Vivo pero todavía en afinado
- matching fino
- semántica de evento
- precisión de precio
- normalización de dirección
- territorialidad más robusta
- radar v2
- cola operativa v2
- explotación real de Telegram

---

## 16. Diagnóstico honesto final

## Lo que ya es
Nexus Madrid ya es una base seria de inteligencia inmobiliaria local.

## Lo que todavía no es
Todavía no es un sistema predictivo fuerte, ni un motor geoespacial maduro, ni una plataforma comercial completa.

## Riesgo principal actual
El mayor riesgo no es técnico: es **abrir demasiadas líneas sin un sistema claro de prioridades**.

## Siguiente foco correcto
Fortalecer núcleo antes de expandir superficie.
