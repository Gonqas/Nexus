# Nexus Madrid — North Star

## 1. Qué es Nexus Madrid

Nexus Madrid es una **workstation de inteligencia inmobiliaria local** orientada a Madrid.

Su función principal no es “guardar pisos” ni “scrapear portales”, sino **unificar varias fuentes de datos, resolver identidad entre registros dispersos y convertir ese dato en señales operativas útiles para captación, lectura territorial y seguimiento del mercado**.

En una frase:

> Nexus Madrid fusiona inventario, cambios de mercado y señales externas para ayudar a decidir **qué está pasando, dónde está pasando y qué conviene hacer primero**.

---

## 2. Problema que resuelve

En operativa inmobiliaria local, la información suele estar fragmentada:

- un CSV interno refleja el inventario conocido,
- una fuente externa como Casafari detecta cambios antes que el baseline,
- algunas señales llegan por canales manuales o semi-estructurados,
- y todo eso suele convivir sin reconciliación fiable.

El resultado típico es:

- duplicados,
- confusión entre activo real y anuncio,
- dificultad para leer zonas,
- imposibilidad de priorizar con criterio,
- y decisiones comerciales demasiado intuitivas.

Nexus Madrid busca resolver ese caos creando una capa intermedia que haga 5 cosas bien:

1. **ingestar** datos,
2. **normalizar** datos,
3. **reconciliar** identidad,
4. **interpretar** cambios,
5. **priorizar** acción.

---

## 3. Para quién es

Usuario principal actual:

- perfil inmobiliario operativo,
- foco local en Madrid,
- necesidad de detectar oportunidades, cambios y zonas,
- interés en traducir datos en decisiones comerciales.

Usuario futuro potencial:

- director comercial,
- captador,
- analista local de mercado,
- responsable de expansión o prospección,
- pequeño equipo con workflow territorial.

---

## 4. Qué no es

Es importante fijar esto para no desordenar el producto.

Nexus Madrid **no es**, al menos en su fase actual:

- un CRM completo,
- un portal inmobiliario,
- una IA predictiva hardcore ya entrenada,
- una plataforma multiusuario enterprise,
- un mapa geoespacial avanzado cerrado,
- una herramienta generalista para toda España,
- un sistema cuyo valor sea “tener más scraping”.

Su valor no está en captar más ruido, sino en **ordenar mejor la realidad local**.

---

## 5. Principio de diseño del producto

El proyecto debe obedecer estas reglas madre:

- **CSV = baseline de inventario**
- **Casafari = delta / flow / eventos recientes**
- **mejor unresolved que falso match**
- **no mezclar anuncio, activo, snapshot y evento**
- **la inteligencia solo vale si termina en operativa útil**
- **cada nueva idea debe fortalecer el núcleo, no abrir un producto paralelo**

---

## 6. Núcleo funcional del sistema

El núcleo del producto hoy puede explicarse como una tubería de 4 capas:

### Capa 1 — Datos base
Recibe e inventaría la realidad conocida.

### Capa 2 — Reconciliación
Decide cuándo dos registros hablan de la misma entidad real.

### Capa 3 — Inteligencia territorial y operativa
Convierte stock y flow en lectura de zona, radar y cola de oportunidad.

### Capa 4 — Acción
Presenta señales priorizadas para que el usuario actúe.

---

## 7. Métrica de éxito

La métrica de éxito no debe ser “tener más tablas” ni “más módulos”.

Las métricas útiles del producto son:

### Métricas núcleo
- mayor % de matching correcto,
- mayor cobertura útil de listings reconciliados,
- menor tasa de ruido semántico,
- menor tiempo para detectar cambio relevante,
- mayor calidad territorial de lectura por zona.

### Métricas operativas
- más señales útiles por semana,
- más claridad sobre dónde priorizar captación,
- más rapidez para localizar oportunidades,
- más confianza del usuario al interpretar mercado local.

---

## 8. Prioridad estratégica actual

La prioridad actual no es añadir una gran capa futurista.

La prioridad correcta es:

1. consolidar el núcleo,
2. mejorar la calidad de identidad y semántica,
3. anclar mejor la geografía,
4. elevar la utilidad operativa.

Dicho de forma simple:

> Antes de meter más inteligencia, hay que conseguir que el sistema entienda mejor **qué es cada cosa, dónde está y qué cambio representa**.

---

## 9. Visión a medio plazo

Si el proyecto madura bien, Nexus Madrid puede evolucionar hacia:

- motor local de inteligencia territorial,
- sistema de priorización de captación,
- auditor de cambios de mercado,
- radar de oportunidades por microzona,
- capa de decisión comercial asistida.

Pero esa evolución solo tiene sentido si el núcleo actual sigue siendo limpio y auditable.

---

## 10. Regla final de producto

Cada nueva funcionalidad debe pasar esta prueba:

### Pregunta de validación
> ¿Esto mejora de verdad la capacidad del sistema para unificar datos, leer mercado local o priorizar acción?

Si la respuesta es no, probablemente no pertenece al núcleo actual.
