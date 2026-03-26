# Nexus Madrid — Registro de decisiones de producto y arquitectura

> Este documento sirve para no reabrir debates una y otra vez.
> Cada decisión debe poder leerse de forma simple: qué se decidió, por qué y qué implica.

---

## D-001 — El proyecto se centra en Madrid

### Decisión
El alcance actual del producto se mantiene **local y centrado en Madrid**.

### Motivo
La lectura territorial, la semántica y la operativa tienen mucho más valor cuando se trabaja un mercado local con profundidad.

### Implicación
No se debe abrir expansión geográfica prematura si eso debilita el núcleo.

---

## D-002 — CSV es baseline de inventario

### Decisión
El CSV se considera la fuente base o baseline del inventario conocido.

### Motivo
Es la fuente más estable y estructurada para consolidar stock y reconstruir universo conocido.

### Implicación
El sistema se apoya en el CSV para:
- inventario base,
- snapshots,
- reconstrucción,
- enriquecimiento principal.

---

## D-003 — Casafari es fuente de flow / delta, no de inventario agresivo

### Decisión
Casafari se usa principalmente como fuente de cambio reciente, no como motor para inflar inventario sin control.

### Motivo
Su mayor valor actual está en detectar movimientos antes que el baseline, no en convertirse en verdad maestra del activo.

### Implicación
El sistema debe priorizar:
- eventos,
- cambios,
- señales recientes,
- trazabilidad de sincronización.

---

## D-004 — Mejor unresolved que falso match

### Decisión
Se prefiere dejar un caso unresolved antes que enlazarlo mal.

### Motivo
El coste de un falso match suele ser peor que el coste de una cobertura algo más baja.

### Implicación
El matching debe ser prudente, auditable y medible.

---

## D-005 — No mezclar asset, listing, snapshot y event

### Decisión
Estas entidades se mantienen conceptualmente separadas.

### Motivo
Cada una representa una capa distinta de realidad:
- activo real,
- anuncio,
- estado temporal,
- cambio interpretado.

### Implicación
Se evita simplificar el modelo de forma que destruya trazabilidad o semántica.

---

## D-006 — La trazabilidad es una pieza de producto, no un extra

### Decisión
El registro de imports y syncs forma parte del núcleo del sistema.

### Motivo
Sin trazabilidad el sistema se vuelve opaco, difícil de depurar y poco fiable.

### Implicación
`IngestionRun`, estados de sync, auditorías y logs deben mantenerse como parte prioritaria.

---

## D-007 — La app de escritorio sigue siendo válida en esta fase

### Decisión
Se mantiene la orientación desktop/local en la fase actual.

### Motivo
Permite iterar rápido, mantener complejidad contenida y trabajar con foco en producto y dato antes que en infraestructura web.

### Implicación
No abrir migración a SaaS/web salvo necesidad estratégica clara.

---

## D-008 — El valor del producto no está en “más scraping”

### Decisión
La prioridad no es sumar fuentes por sumar.

### Motivo
El problema principal no es escasez de dato, sino calidad, identidad, lectura territorial y operatividad.

### Implicación
Nuevas fuentes solo entran si fortalecen el núcleo.

---

## D-009 — La inteligencia territorial es núcleo estratégico

### Decisión
La lectura de zona, radar y opportunity queue forman parte central del producto.

### Motivo
Es donde el sistema deja de ser una base de datos y se convierte en una herramienta de decisión.

### Implicación
Estas superficies deben evolucionar, pero apoyadas en mejor geografía y mejor matching.

---

## D-010 — Zonas oficiales antes de microzonas sofisticadas

### Decisión
Antes de entrar en mallas avanzadas o microzonas complejas, hay que consolidar una geografía oficial fiable.

### Motivo
No tiene sentido sofisticar la capa espacial si la base territorial aún es inestable.

### Implicación
Primero:
- barrio,
- distrito,
- estructura oficial.

Después:
- microzonas,
- geoespacial fino,
- segmentación avanzada.

---

## D-011 — La normalización de direcciones es prioridad alta

### Decisión
La mejora de direcciones se considera una iniciativa crítica del núcleo.

### Motivo
La dirección impacta en:
- matching,
- deduplicación,
- territorialidad,
- calidad general del dato.

### Implicación
Debe tratarse como un habilitador central, no como detalle técnico secundario.

---

## D-012 — El matching debe pasar a ser medible

### Decisión
La siguiente evolución del matching debe incluir revisión humana y métricas.

### Motivo
Sin ground truth y sin métricas, el sistema mejora solo por intuición.

### Implicación
Habrá que incorporar:
- dataset etiquetado,
- correcciones manuales,
- precision / recall,
- thresholds revisables.

---

## D-013 — Telegram existe, pero no debe romper el foco

### Decisión
La línea Telegram se reconoce como valiosa, pero todavía no debe dominar el roadmap.

### Motivo
Puede aportar señal temprana, pero si se sobredimensiona antes de consolidar el núcleo abre otra línea de producto demasiado pronto.

### Implicación
Telegram se trabaja como expansión controlada, no como eje principal todavía.

---

## D-014 — Toda nueva funcionalidad debe pasar la prueba del núcleo

### Decisión
Antes de implementar algo nuevo se debe responder: ¿mejora identidad, territorio, cambio u operativa?

### Motivo
Evita dispersión, sobre todo en un proyecto con muchas ideas posibles.

### Implicación
Si una idea no fortalece uno de esos cuatro ejes, se aparca.

---

## D-015 — La documentación deja de ser opcional

### Decisión
El proyecto debe mantener documentación mínima viva.

### Motivo
Sin documentación, cada iteración obliga a reconstruir mentalmente qué existe, qué se decidió y qué toca hacer.

### Implicación
Los siguientes documentos pasan a formar parte del sistema de trabajo:
- north star,
- estado actual,
- roadmap,
- backlog,
- glosario,
- decisiones.
