# Nexus Madrid — Sistema de trabajo para no agobiarte

## 1. Objetivo de este documento

Este documento no habla del producto: habla de **cómo trabajar el producto sin romperte la cabeza**.

Si tienes TDAH o tendencia a abrir demasiadas líneas, el riesgo no suele ser “falta de capacidad”, sino esto:

- demasiadas ideas a la vez,
- demasiadas opciones abiertas,
- mezclar visión, bugs, arquitectura y sueños futuros,
- y abandonar porque todo parece enorme.

Este sistema está pensado para que puedas seguir avanzando sin perder el hilo.

---

## 2. Regla madre

### Nunca trabajes desde el caos bruto

No trabajes directamente desde:
- mensajes sueltos,
- notas mentales,
- ideas que te van viniendo,
- o listas infinitas.

Siempre trabaja desde una estructura de 3 niveles:

1. **qué es el producto**,
2. **en qué estado está**,
3. **qué toca ahora**.

---

## 3. Las 6 piezas que ordenan el proyecto

Tu carpeta `docs/` debe ser tu ancla mental.

### 1. `00_north_star.md`
Sirve para recordar qué es Nexus Madrid y qué no es.

### 2. `01_estado_actual.md`
Sirve para no imaginarte el proyecto peor o mejor de lo que es.

### 3. `02_roadmap.md`
Sirve para decidir qué toca antes y qué toca después.

### 4. `03_backlog_ideas.md`
Sirve para vaciar la cabeza sin perder ideas.

### 5. `04_glosario.md`
Sirve para aprender términos técnicos sin bloquearte.

### 6. `05_decisiones.md`
Sirve para no reabrir el mismo debate todas las semanas.

---

## 4. Regla de priorización simple

Cada tarea o idea que aparezca se clasifica en una sola categoría.

### Tipo A — Mantiene vivo el núcleo
Ejemplos:
- bug serio,
- fallo de import,
- matching roto,
- zonas mal inferidas,
- precio mal parseado.

### Tipo B — Multiplica valor del núcleo
Ejemplos:
- mejor búsqueda,
- mejores métricas,
- mejor oportunidad queue,
- mejor explicación en UI.

### Tipo C — Entusiasma, pero todavía distrae
Ejemplos:
- IA súper ambiciosa,
- geoespacial ultra fino demasiado pronto,
- sistema gigante multiusuario,
- automatizaciones enormes.

### Regla
Si una tarea no es A o B, no se trabaja ahora.

---

## 5. Regla de foco

### Solo un frente grande a la vez
No abras dos mejoras grandes simultáneas salvo urgencia real.

Ejemplos de “frente grande”:
- direcciones,
- zonas oficiales,
- matching learning loop,
- rediseño fuerte de radar.

### Qué sí puedes mezclar
- 1 frente grande
- + 1 tarea pequeña de mantenimiento
- + 1 tarea muy pequeña visual o de usabilidad

No más.

---

## 6. Método semanal recomendado

## Paso 1 — Releer solo 3 documentos
Antes de tocar código, relee:
- north star,
- estado actual,
- roadmap.

Eso evita entrar a programar con el cerebro desordenado.

## Paso 2 — Elegir una misión semanal
Formula de la misión:

> Esta semana voy a mejorar ___ para que el sistema entienda mejor ___.

Ejemplos:
- “Voy a mejorar direcciones para que el sistema agrupe mejor por zona.”
- “Voy a mejorar matching para aumentar resolved sin subir falsos positivos.”

## Paso 3 — Partir la misión en mini pasos
Cada frente grande debe partirse en trozos visibles de 30–90 minutos.

Ejemplo para direcciones:
- revisar cómo se normalizan hoy,
- detectar 10 ejemplos problemáticos,
- definir reglas de limpieza,
- aplicar las reglas,
- volver a probar.

## Paso 4 — Cerrar con nota de salida
Al terminar una sesión, deja siempre escrito:
- qué tocaste,
- qué quedó funcionando,
- qué quedó pendiente,
- cuál es el siguiente paso exacto.

Esto es clave para no perder el hilo al día siguiente.

---

## 7. Método de sesión corta

Cuando estés bloqueado, usa este esquema:

### Duración sugerida
45 minutos.

### Estructura
- 5 min: releer objetivo
- 25 min: ejecutar un subpaso
- 10 min: probar / verificar
- 5 min: dejar nota de continuidad

### Regla
No terminar una sesión sin dejar el siguiente paso escrito.

---

## 8. Cómo evitar abrir diez líneas a la vez

Cuando se te ocurra una idea nueva, no la trabajes al momento.

Haz esto:
1. la apuntas en `03_backlog_ideas.md`,
2. la etiquetas como A / B / C,
3. vuelves al frente actual.

### Regla clave
Tener una idea **no** obliga a desarrollarla ahora.

---

## 9. Qué hacer cuando te sientas perdido

Si te notas saturado, no abras el código aún.

Haz este checklist:

### Checklist de rescate
- releer north star
- releer roadmap
- decir en una frase qué bloque estás atacando
- decir por qué importa
- elegir el siguiente paso más pequeño posible

Si no puedes formular el siguiente paso pequeño, todavía estás demasiado arriba y necesitas dividir más.

---

## 10. Qué hacer cuando quieras cambiar de idea a mitad

Antes de cambiar de frente, responde:

1. ¿esto arregla algo crítico del núcleo?
2. ¿o solo me está apeteciendo más ahora mismo?
3. ¿el frente actual está realmente bloqueado o solo me está cansando?

### Regla
No abandonar un frente porque se haya vuelto menos excitante.

Eso suele ser la trampa clásica.

---

## 11. Sistema mínimo de seguimiento

Puedes llevarlo en un markdown simple o en una nota.

### Plantilla sugerida
- **Frente actual:**
- **Objetivo de la semana:**
- **Subpaso de hoy:**
- **Bloqueo actual:**
- **Siguiente paso exacto:**

Esto te da una continuidad brutal con muy poca fricción.

---

## 12. Reglas anti-abandono

### Regla 1
No abrir una mejora grande nueva sin cerrar o congelar explícitamente la anterior.

### Regla 2
No tocar arquitectura por ansiedad si el problema es de prioridad.

### Regla 3
No reescribir todo cuando lo que hace falta es ordenar y refinar.

### Regla 4
No meter ideas futuristas para evitar enfrentarte al cuello de botella actual.

### Regla 5
Celebrar cierres pequeños. En proyectos largos, cerrar mini bloques vale muchísimo más que pensar gigantesco.

---

## 13. Frase guía para este proyecto

> No necesito hacerlo todo ahora. Necesito fortalecer el siguiente bloque correcto.

Y otra todavía más importante:

> Mi trabajo no es perseguir todas las ideas. Mi trabajo es construir una máquina cada vez más clara, fiable y útil.
