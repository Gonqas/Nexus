# Nexus Madrid — Glosario técnico explicado

## Asset
**Activo inmobiliario real**.

No es lo mismo que un anuncio. Un mismo asset puede aparecer representado por distintos listings a lo largo del tiempo o en distintos portales.

---

## Listing
**Anuncio concreto**.

Es la manifestación pública o registrada de un inmueble en una fuente concreta.

---

## Building
**Edificio**.

Entidad superior que agrupa activos que pertenecen al mismo edificio o bloque.

---

## Contact
**Contacto asociado**.

Puede ser persona, agencia o entidad relacionada con un listing o asset.

---

## Snapshot
**Foto puntual del estado de algo en un momento del tiempo**.

En Nexus Madrid, `ListingSnapshot` sirve para conservar el estado observado del listing en una importación determinada.

---

## Market Event
**Evento de mercado**.

Representa un cambio interpretado: por ejemplo nueva entrada, bajada de precio, salida del mercado, reservado, etc.

---

## Raw
**Dato bruto no totalmente consolidado**.

En este proyecto, `RawHistoryItem` es la materia prima que entra desde Casafari antes de quedar bien interpretada y enlazada.

---

## Baseline
**Base estable de referencia**.

Es el punto a partir del cual comparas cambios.

En Nexus Madrid, el CSV funciona hoy como baseline del inventario conocido.

---

## Delta
**Cambio respecto al baseline**.

Ejemplo: si Casafari detecta un movimiento reciente, eso funciona como delta o flow frente al inventario base.

---

## Flow
**Movimiento reciente del mercado**.

No describe solo lo que existe, sino lo que está pasando.

Ejemplos:
- nuevos anuncios,
- bajadas,
- subidas,
- sold,
- reserved,
- off-market.

---

## Stock
**Lo que hay actualmente en inventario o universo observado**.

Ejemplo:
- listings activos,
- assets por zona,
- precio medio,
- €/m² medio.

---

## Entity Resolution / Record Linkage
Problema técnico que consiste en decidir si dos registros distintos corresponden a la **misma entidad real**.

Ejemplo:
- un raw Casafari,
- un listing del CSV,
- una alerta Telegram,

¿representan el mismo piso o no?

---

## Matching
Proceso de puntuación y comparación entre registros para resolver identidad.

No siempre da un sí o no exacto: muchas veces devuelve grados de confianza.

---

## Resolved
Estado en el que el sistema cree que ya ha podido enlazar correctamente un raw con una entidad conocida.

---

## Unresolved
Estado en el que el sistema no tiene base suficiente para enlazar con seguridad.

Importante: unresolved no significa necesariamente error; a veces significa prudencia correcta.

---

## Ambiguous
Estado en el que hay varios candidatos razonables, pero no suficiente confianza para elegir uno.

---

## Pending
Estado transitorio antes de analizar o resolver un caso.

---

## Confidence
Nivel de confianza del sistema respecto a un dato, una clasificación o un match.

---

## Threshold / Umbral
Punto a partir del cual decides que algo se acepta.

Ejemplo:
- por debajo de cierto score, un match no se acepta;
- por encima, sí.

---

## Ground Truth
Conjunto de ejemplos revisados manualmente que se toman como “verdad de referencia”.

Sirve para evaluar si el sistema está acertando o no.

---

## Precision
De todo lo que el sistema marcó como positivo, cuánto era realmente correcto.

Ejemplo sencillo:
- si dijo “estos 10 matches son buenos” y solo 8 lo eran, la precisión no es perfecta.

---

## Recall
De todos los positivos reales que existían, cuántos consiguió capturar el sistema.

Ejemplo sencillo:
- si realmente había 20 matches correctos y solo detectó 8, el recall es bajo.

---

## False Positive
Caso que el sistema marca como correcto cuando no lo era.

Ejemplo: enlazar dos pisos distintos como si fueran el mismo.

---

## False Negative
Caso que sí era correcto pero el sistema no lo detectó.

Ejemplo: dejar unresolved algo que en realidad sí era el mismo inmueble.

---

## Normalización
Proceso de limpiar y convertir datos a una forma más comparable y consistente.

Ejemplos:
- teléfonos,
- direcciones,
- tipologías,
- URLs,
- nombres de portales.

---

## Parsing
Extracción estructurada a partir de texto menos limpio.

Ejemplo: sacar un precio válido desde una cadena con ruido.

---

## Semántica del dato
Interpretación del significado de un dato.

No solo importa guardar “qué vino”, sino entender “qué representa”.

Ejemplo:
- distinguir entre una simple actualización y una bajada real de precio.

---

## Heurística
Regla práctica aproximada, no necesariamente perfecta, que ayuda a tomar decisiones cuando no hay un modelo formal completo.

Muchas partes de un sistema maduro empiezan como heurísticas bien pensadas.

---

## Scoring
Asignación de una puntuación a una entidad, zona o señal.

Ejemplo:
- score de calor de zona,
- score de liquidez,
- score de oportunidad.

---

## Radar
Superficie que resume dónde está pasando algo relevante.

Su función ideal es dirigir atención, no solo mostrar datos.

---

## Opportunity Queue / Cola operativa
Lista priorizada de señales o acciones sugeridas.

Es el puente entre análisis y trabajo diario.

---

## Full-text search
Sistema de búsqueda de texto más potente que una coincidencia exacta simple.

Permite encontrar frases, términos y cadenas útiles dentro de mucho texto.

---

## Geocoding
Proceso de convertir una dirección en coordenadas geográficas.

---

## Spatial Join
Cruce entre geometrías o capas geográficas.

Ejemplo:
- saber a qué barrio oficial pertenece un punto.

---

## Microzona
Subdivisión espacial más pequeña y homogénea que un barrio grande.

Sirve para hacer lecturas territoriales más finas.

---

## Observabilidad
Capacidad de ver cómo está funcionando el sistema internamente.

Incluye:
- logs,
- métricas,
- diagnósticos,
- paneles de calidad.

---

## Trazabilidad
Capacidad de reconstruir qué pasó, cuándo pasó y por qué pasó.

`IngestionRun` es un ejemplo de trazabilidad útil.
