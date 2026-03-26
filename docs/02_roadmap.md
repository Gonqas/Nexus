# Nexus Madrid — Roadmap por prioridad

## 1. Cómo debe usarse este roadmap

Este roadmap no está pensado para meter 200 tareas.

Está pensado para responder a una sola pregunta:

> ¿Qué bloque fortalece más el núcleo del producto ahora mismo?

La regla es simple:
- **Ahora** = cosas que sostienen el producto
- **Después** = cosas que multiplican el valor del núcleo
- **Más adelante** = cosas potentes, pero todavía no críticas

---

## 2. Objetivo del roadmap

Pasar de:
- sistema prometedor pero disperso

A:
- sistema estable, entendible y cada vez más útil operativamente.

---

## 3. AHORA — Prioridad máxima

## Bloque A — Geografía y zonas reales

### Objetivo
Hacer que la lectura territorial deje de depender tanto de texto de dirección mal estructurado.

### Qué implica
- definir modelo oficial de zona
- distinguir calle, barrio, distrito y microzona
- anclar listings/assets a una jerarquía territorial consistente
- corregir inferencias defectuosas de zona

### Por qué es prioridad máxima
Porque hoy la inteligencia territorial existe, pero parte de una geografía todavía débil.

### Resultado esperado
- zonas más fiables,
- radar más útil,
- opportunity queue más precisa,
- menos ruido territorial.

---

## Bloque B — Normalización de direcciones

### Objetivo
Mejorar la comparabilidad de direcciones para identidad, geografía y deduplicación.

### Qué implica
- estandarizar abreviaturas
- limpiar ruido textual
- separar componentes de dirección
- reforzar matching textual de localización

### Por qué va tan arriba
Porque la dirección impacta en tres núcleos a la vez:
- matching,
- zonas,
- calidad general del dato.

### Resultado esperado
- más resoluciones correctas,
- menos pseudozonas,
- mejor agrupación territorial.

---

## Bloque C — Matching que aprende

### Objetivo
Pasar de un matching prudente pero estático a un matching medible y entrenable con revisión humana.

### Qué implica
- guardar decisiones humanas de match / no match
- construir un pequeño dataset de verdad-terreno
- medir precisión y recall
- revisar thresholds
- analizar por qué fallan los unresolved

### Resultado esperado
- mayor cobertura de resolved,
- menos falsos positivos,
- más confianza en la reconciliación.

---

## Bloque D — Métricas núcleo visibles

### Objetivo
No trabajar a ciegas.

### Qué implica
Crear métricas visibles sobre:
- ratio de resolved,
- ratio de unresolved,
- raws sin precio fiable,
- raws con dirección pobre,
- eventos por tipo,
- zonas con baja confianza,
- evolución por sync.

### Resultado esperado
Capacidad de dirigir mejoras con criterio en vez de por intuición.

---

## 4. DESPUÉS — Multiplicadores del núcleo

## Bloque E — FTS / búsqueda avanzada en la app

### Objetivo
Poder encontrar rápido entidades, textos, razones de unresolved y señales sin depender de filtros manuales pobres.

### Qué implica
- búsqueda global por texto
- búsqueda en raws
- búsqueda en direcciones
- búsqueda en teléfonos y contactos
- búsqueda por reason taxonomy

### Resultado esperado
Mejor usabilidad y mayor velocidad de análisis.

---

## Bloque F — Cola operativa v2

### Objetivo
Hacer que la app no solo analice, sino que sugiera mejor trabajo diario.

### Qué implica
- ranking más fino por señal
- agrupación por zona / contacto / evento
- prioridad comercial más inteligible
- explicación de por qué una señal está arriba

### Resultado esperado
Más valor práctico diario y menos sensación de herramienta “solo analítica”.

---

## Bloque G — Radar v2 fuerte

### Objetivo
Hacer del radar una superficie realmente estratégica.

### Qué implica
- combinación más robusta de stock + flow
- filtros por confianza
- variación temporal 7/14/30 días
- lectura de absorción y presión con más matiz
- explicación por zona

### Resultado esperado
Mejor capacidad para detectar dónde está el movimiento importante.

---

## Bloque H — Telegram como fuente operativa real

### Objetivo
Elevar Telegram de línea experimental a fuente útil de señal.

### Qué implica
- definir qué papel exacto tiene Telegram
- decidir si solo genera alertas o también candidatos a activo/listing
- revisar matching específico
- auditar calidad real de esta línea

### Resultado esperado
Aprovechar señales tempranas sin romper el núcleo del producto.

---

## 5. MÁS ADELANTE — Potencia avanzada

## Bloque I — Geoespacial serio y microzonas

### Objetivo
Pasar de zonas textuales a inteligencia espacial más precisa.

### Posibles líneas
- coordenadas y geocoding
- joins territoriales
- microzonas homogéneas
- análisis por rejilla/celda

### Nota
No es lo primero. Primero hay que tener bien resueltas direcciones y zonas oficiales.

---

## Bloque J — Predicción real

### Objetivo
Pasar de heurísticas avanzadas a modelos más predictivos.

### Posibles casos
- probabilidad de absorción
- probabilidad de salida rápida
- detección de señales tempranas de oportunidad
- ranking predictivo de captación

### Nota
Esto solo tiene sentido con buen dato y buen matching.

---

## Bloque K — IA explicativa dentro de la app

### Objetivo
Añadir una capa que traduzca el sistema a lenguaje más accionable.

### Casos posibles
- resumen de zona en lenguaje natural
- explicación automática de señal
- resumen de qué ha cambiado desde la última sync
- sugerencias operativas guiadas

### Nota
Muy útil, pero no debe tapar carencias del núcleo.

---

## 6. Roadmap recomendado por fases

## Fase 1 — Consolidación del núcleo
- geografía real
- direcciones
- matching medible
- métricas núcleo

## Fase 2 — Utilidad operativa fuerte
- radar v2
- cola operativa v2
- búsqueda avanzada

## Fase 3 — Multiplicación de fuentes
- Telegram bien definido
- nuevas señales externas si de verdad aportan

## Fase 4 — Potencia avanzada
- geoespacial fino
- predicción
- IA explicativa

---

## 7. Regla de prioridad con TDAH

Cada idea nueva debe clasificarse en una sola de estas categorías:

### Nivel 1 — Mantiene vivo el núcleo
Si no se hace, el producto entiende peor la realidad.

### Nivel 2 — Multiplica valor del núcleo
Si se hace, el producto se vuelve mucho más útil.

### Nivel 3 — Entusiasma, pero distrae
Es atractiva, pero todavía no toca el cuello de botella principal.

### Regla
No trabajar dos iniciativas grandes de Nivel 1 a la vez, salvo bug crítico.

---

## 8. Prioridad concreta recomendada hoy

1. **Zonas oficiales / geografía real**
2. **Normalización de direcciones**
3. **Learning loop del matching**
4. **Métricas núcleo visibles**
5. **FTS / búsqueda potente**
6. **Radar y cola operativa v2**
7. **Telegram como fuente fuerte**
8. **Geoespacial fino / IA avanzada**

---

## 9. Criterio final de roadmap

La pregunta correcta no es:
> “¿Qué otra idea puedo meter?”

La pregunta correcta es:
> “¿Qué mejora más la capacidad del sistema para entender identidad, territorio y cambio?”
