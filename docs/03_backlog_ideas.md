# Nexus Madrid — Backlog de ideas bruto

> Este documento **no es** para trabajar directamente desde aquí.
> Su objetivo es vaciar la cabeza y evitar perder ideas.
> La priorización real siempre se hace en `02_roadmap.md`.

---

## 1. Datos e ingestión

- mejorar validación de CSV
- soportar más variantes de CSV
- guardar más metadata del import
- detectar columnas incompletas o sospechosas
- versionar estructura del CSV esperado
- registrar métricas de calidad por import
- reforzar sync incremental de Casafari
- mejorar auditoría de overlap temporal
- guardar más contexto bruto por raw
- definir mejor rol de Telegram
- importar fuentes manuales adicionales si encajan con el núcleo

---

## 2. Normalización

- normalización seria de direcciones
- normalización de portales
- limpieza de precios raros
- detección de valores sospechosos
- normalización de teléfonos internacionales o incompletos
- mejorar clasificación de property types
- unificar strings repetidos del dominio inmobiliario
- etiquetar nivel de confianza de cada campo crítico

---

## 3. Matching / identidad

- mejorar scoring de listing resolver
- crear dataset de ground truth
- guardar correcciones manuales
- crear interfaz de revisión de candidatos
- analizar falsos negativos
- analizar falsos positivos
- calibrar thresholds
- distinguir mejor same listing vs same asset vs probable duplicate
- reforzar matching en casos sin URL exacta
- aprovechar más señales combinadas de contacto + precio + ubicación

---

## 4. Semántica de eventos

- mejorar tipado de eventos Casafari
- distinguir mejor new / price drop / relist / off-market / sold / reserved
- detectar incertidumbre semántica explícita
- separar eventos fuertes de eventos débiles
- mejorar `reason taxonomy`
- registrar por qué se clasificó cada evento
- panel de calidad semántica

---

## 5. Geografía y territorio

- barrios oficiales
- distritos oficiales
- secciones censales
- geocoding
- coordenadas por asset/listing
- microzonas homogéneas
- diferenciación calle / barrio / distrito
- joins territoriales
- mapas básicos
- mapas de densidad
- lectura territorial por stock y flow
- comparación temporal por zona

---

## 6. Scoring e inteligencia

- refinar zone_heat_score
- refinar zone_pressure_score
- refinar zone_liquidity_score
- refinar zone_capture_score
- refinar zone_confidence_score
- explicar cada score en lenguaje entendible
- hacer scoring más robusto por ventanas 7/14/30 días
- crear score de calidad de dato por zona
- crear score de oportunidad por listing
- crear score de prioridad operativa por señal

---

## 7. Radar

- radar por barrio
- radar por microzona
- radar por confianza
- radar por tipo de evento
- radar con filtros temporales
- radar con comparación contra periodo anterior
- radar con explicación automática
- radar con alertas destacadas

---

## 8. Cola operativa

- ordenar mejor señales
- agrupar señales repetidas
- explicar por qué una señal es prioritaria
- distinguir señales de captación vs seguimiento vs mercado
- filtros por zona, contacto, tipo de evento y confianza
- botón de revisión rápida
- historial de qué señales ya fueron vistas
- estado de trabajo de la señal

---

## 9. Telegram

- mejorar parser
- mejorar matching de alertas
- decidir si Telegram crea solo alertas o también listings
- mejorar auditoría de Telegram
- conectar Telegram con opportunity queue
- deduplicar alertas repetidas
- medir valor real de Telegram frente a ruido

---

## 10. UI / UX

- filtros más potentes
- búsqueda global
- navegación más clara entre entidades
- mayor explicación visual del pipeline
- badges de confianza
- páginas de detalle mejores
- tablas más legibles
- modo auditoría
- métricas visibles en dashboard
- avisos cuando una fuente tiene baja calidad

---

## 11. Observabilidad y calidad

- métricas de sync
- métricas de import
- métricas de matching
- métricas de semántica
- logs más claros
- panel de integridad
- tests unitarios
- tests de regresión del matching
- tests de parsers
- dataset pequeño de ejemplos de referencia

---

## 12. Herramientas internas

- scripts de auditoría más completos
- snapshots comparativos entre syncs
- exportadores para análisis externo
- notebooks de diagnóstico
- utilidades de revisión manual
- utilidades de limpieza de base en entorno de desarrollo

---

## 13. IA y automatización futura

- resumen automático de zonas
- explicación automática de oportunidades
- copiloto para auditoría de casos unresolved
- generación de informes internos
- recomendación de siguientes acciones
- clasificación asistida de raws dudosos
- detección automática de anomalías
- predicción de captación
- predicción de absorción

---

## 14. Reglas de uso del backlog

### Regla 1
Nada entra directo a desarrollo desde aquí sin pasar por roadmap.

### Regla 2
Si una idea no mejora núcleo, se aparca.

### Regla 3
Toda idea debe decir a qué capa pertenece:
- datos,
- identidad,
- geografía,
- inteligencia,
- operativa,
- UX,
- automatización.

### Regla 4
Si una idea abre otro producto, no entra todavía.
