# Tarea 3 — Estado, duplicados e idempotencia con Apache Beam

**Streaming de datos y sus aplicaciones** — Maestría en Inteligencia Artificial y
Análisis de Datos, Facultad Politécnica (UNA). Prof. Rodrigo Parra, M.Sc.

Alumna: **Clara Almirón** — C.I. 3980921

Derivado del proyecto base de la cátedra
[rparrapy/streaming-fpuna-clase6-tarea](https://github.com/rparrapy/streaming-fpuna-clase6-tarea).
`notebook.py` tiene implementados los TODO 1–8: totales confirmados por comercio y
minuto que no cambian si los pagos llegan fuera de orden, duplicados, tarde o si el
sink se reintenta.

## Estado

| Verificación | Resultado |
|---|---|
| `uv run pytest` | **39 pruebas en verde**: las 13 provistas (`tests/test_assignment.py`, sin modificar) y 26 propias (`tests/test_propias.py`) |
| `uv run ruff check .` | Sin observaciones |
| `uv run marimo check --strict notebook.py` | Sin observaciones |

La salida completa está en [`evidencia/pytest.txt`](evidencia/pytest.txt). El notebook
ejecutado, con todas sus tablas, está en [`evidencia/notebook.html`](evidencia/notebook.html)
(descargarlo y abrirlo en el navegador).

## Cómo ejecutarlo

### Con uv

Requiere [uv](https://docs.astral.sh/uv/). El proyecto fija Python 3.12 y
`apache-beam` 2.74.0; `uv` descarga el intérprete si hace falta.

```bash
uv sync --frozen
uv run pytest                       # suite completa
uv run marimo edit notebook.py      # editor
uv run marimo run notebook.py       # solo lectura, como aplicación
```

Validaciones de estilo y estructura:

```bash
uv run ruff check .
uv run marimo check --strict notebook.py
```

Regenerar la evidencia:

```bash
uv run pytest -v > evidencia/pytest.txt
uv run marimo export html notebook.py -o evidencia/notebook.html
```

### Con Docker

```bash
docker compose up --build notebook                 # editor en http://localhost:2718
docker compose run --rm notebook /app/.venv/bin/pytest -q
docker compose down
```

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse a una red pública.

## Resultado con la configuración por defecto

Ventana fija de 60 s, lateness de 120 s, deduplicación activa, sobre
`data/payments.jsonl`:

- **Entran 9 eventos; se aceptan 5.** Uno de ellos, `p-004`, es una revisión tardía.
- **Se descartan 4:** `p-003` (`PENDING`) y `p-008` (`REJECTED`) no están
  confirmados, la segunda copia de `p-002` es un duplicado y `p-007` llega
  169 s después de su evento, fuera de tolerancia.
- **Se producen 4 totales:**

| Comercio | Ventana | Total | Cómo se forma |
|---|---|---:|---|
| `m-azul` | 13:00–13:01 | 170 000 | `p-001` + `p-004`, que llega fuera de orden |
| `m-verde` | 13:00–13:01 | 80 000 | `p-002` una sola vez; `p-007` queda fuera por tardío |
| `m-verde` | 13:01–13:02 | 90 000 | `p-005` |
| `m-azul` | 13:02–13:03 | 200 000 | `p-006`; `p-008` fue rechazado |

Con reintentos, 4 resultados × 2 intentos dan 8 filas de auditoría: el `POST`
materializa 8 y el `UPSERT`, 4.

## Decisiones y trade-offs

### TODO 1 · `parse_utc`: UTC explícito o error

Solo acepta ISO-8601 terminado en `Z`. Un timestamp sin zona se interpretaría con
la hora local de la máquina que corre el pipeline, y uno con otro offset indica que
el productor no respeta el contrato. En los dos casos es preferible fallar con un
`ValueError` claro a asignar el evento a la ventana equivocada sin que nadie se
entere. Conserva fracciones de segundo.

*Trade-off:* un productor que mande `+00:00` en lugar de `Z` también se rechaza,
aunque sea el mismo instante. Se eligió así porque el contrato dice `Z` y conviene
detectar al productor que se desvía.

### TODO 2 · `assign_fixed_window`: la misma alineación que Beam

La ventana es `[inicio, fin)`, alineada a múltiplos de `size` desde la época Unix:
es exactamente lo que hace `FixedWindows(size)` con offset 0, y una prueba propia lo
compara contra Beam. Si el oráculo y el pipeline alinearan distinto, compararlos no
tendría sentido. El cálculo usa `timedelta`, que es exacto, en lugar de dividir
segundos en float. Rechaza `datetime` sin zona por la misma razón que el TODO 1.

### TODO 3 · `summarize_payments`: el oráculo

- **Orden de llegada.** Se recorre por `arrival_time`, no en el orden del archivo:
  «primera aparición» y «llegó después del cierre» dependen de cuándo llegó cada
  evento. En el archivo `p-007` está antes que `p-008`, pero llega después.
- **Duplicado significa «ya se contó».** El conjunto de vistos (`seen`) guarda solo
  los pagos aceptados, es decir, los que ya sumaron a un total. Así, un `PENDING`
  previo con el mismo `event_id` no bloquea la confirmación posterior (igual que en
  el pipeline, donde el `Filter` va antes del estado). Además, el reintento de un
  pago rechazado por tardío vuelve a salir `too_late` y no `duplicate`, porque el
  original nunca entró. Una primera versión registraba todo `CONFIRMED` y reportaba
  ese reintento como duplicado; se corrigió tras una revisión cruzada.
- **Aislamiento por comercio.** La identidad es `(merchant_id, event_id)`: el mismo
  id en dos comercios son dos pagos.
- **Precedencia de razones.** `not_confirmed` → `duplicate` → `too_late` →
  `accepted`. Las banderas `duplicate` y `too_late` se calculan por separado, así
  que un duplicado tardío queda marcado con las dos aunque la razón sea una sola.
  Con `deduplicate=False` el duplicado sigue marcado, pero se cuenta: sirve para
  medir su efecto (`m-verde` 13:00 pasa de 80 000 a 160 000).
- **Revisión.** Es un evento aceptado con `arrival_time ≥ fin de ventana`. La
  ventana `[inicio, fin)` está completa en `fin`, así que lo que llega desde ese
  instante corrige un resultado ya emitido.

#### Lateness: la convención del curso frente a Beam

El oráculo sigue la convención del laboratorio de la clase 6: un evento es tardío si
su **atraso individual** (`arrival_time − event_time`) supera la lateness. Beam, en
cambio, lo mide contra el **watermark**: descarta un elemento cuando el watermark
pasó `fin de ventana + lateness`.

Las dos reglas coinciden en este dataset, pero no son la misma. Un pago con
`event_time` 13:00:05 que llega a las 13:02:30 tiene 145 s de atraso y el oráculo lo
rechaza. Para Beam, con un watermark que sigue la llegada, todavía se acepta: la
ventana cierra a las 13:01:00 y la tolerancia vence a las 13:03:00. El oráculo es
más estricto con los eventos del principio de la ventana. Es una aproximación
determinista, útil como contrato de pruebas; en producción manda el watermark.

### TODO 4 · `build_windowed_totals_pipeline`

`Create → TimestampedValue(event_time) → Filter(CONFIRMED) → WindowInto →
(merchant_id, evento) → DeduplicatePayments → CombinePerKey(sum)`, con los límites
de ventana tomados de `WindowParam`.

- **La clave es el comercio antes del estado.** El estado de Beam es por clave y
  ventana, así que la deduplicación queda aislada por comercio sin código adicional.
  La clave se declara `str` con un type hint: el estado exige un coder de clave
  determinista y, sin el hint, Beam advierte que no puede garantizarlo.
- **La deduplicación va dentro del pipeline.** Sin ella, el pipeline y el oráculo no
  coincidirían ante un duplicado.
- **Parámetros extra, todos opcionales:**
  - `events` también acepta una PCollection, como la salida de un `TestStream`.
  - `windowing` recibe la política de triggers.
  - `dedup_fn` inyecta otra instancia del deduplicador.
  - `with_pane` agrega el timing y el índice del pane.

  Con los valores por defecto la salida es exactamente la que exige la prueba
  provista.

*Hallazgo:* **en batch no hay datos tardíos.** Con `Create` el watermark salta al
final recién después de leer todo, así que el pipeline batch también cuenta `p-007`.
Coincide con el oráculo sin límite de lateness, no con el de 120 s, y hay una prueba
propia que lo verifica. Para que la lateness exista hay que reproducir el orden de
llegada con `TestStream`.

### TODO 5 y 5b · `DeduplicatePayments`: estado con expiración

`process` emite el elemento solo si su `event_id` no está en el `SetState`. Si no
estaba, lo agrega y arma un timer de watermark en
`window.max_timestamp() + allowed_lateness`. `expire` limpia el estado cuando ese
timer vence.

**Por qué el estado necesita expiración.** Guarda un id por cada pago aceptado. Los
ids no se repiten entre ventanas y el stream no termina: sin expiración, la memoria
del worker, el tamaño de los checkpoints y el costo de cada `read()` crecen sin
límite.

**Por qué el timer vence en fin + lateness y no en fin de ventana.** Hasta ese
instante una copia todavía puede ser aceptada; después, el runner la descarta igual
por tardía y recordar el id ya no aporta nada. Este dataset lo muestra: la segunda
copia de `p-002` llega a las 13:01:41, **después** del cierre de su ventana (13:01:00).
Con el timer en `fin` (como en el laboratorio de la clase), el estado ya estaría
limpio y la copia produciría un pane LATE de 160 000 en `m-verde`, es decir, el pago
contado dos veces. La prueba `test_timer_sin_lateness_deja_pasar_el_duplicado_tardio`
reproduce ese error; con el valor por defecto no ocurre.

*Trade-off:* `read()` materializa el conjunto y buscar un id cuesta O(n) por
elemento. Para un comercio con muchísimos pagos por ventana convendría un
`ValueState` por `event_id` (usar el id como parte de la clave) o un Bloom filter,
que acepta falsos positivos.

### TODO 6 · `build_trigger_policy`

```python
WindowInto(
    FixedWindows(60),
    trigger=AfterWatermark(early=AfterProcessingTime(30), late=AfterCount(1)),
    allowed_lateness=120,
    accumulation_mode=ACCUMULATING,
)
```

- **On-time:** cuando el watermark pasa el fin de la ventana.
- **Early:** cada 30 s de processing time mientras la ventana está abierta
  (configurable con `early_seconds`). Sirve para ver una estimación sin esperar al
  watermark; a cambio, se emiten más panes.
- **Late:** un pane por cada evento tardío aceptado, así la corrección llega de
  inmediato.
- **ACCUMULATING**, porque el sink hace UPSERT: cada pane trae el total completo y el
  último valor escrito es el correcto. Con `DISCARDING` cada pane traería solo el
  incremento y el UPSERT borraría lo anterior.

*Incompatibilidad de la prueba provista.* `test_trigger_policy_has_lateness_and_accumulating_panes`
lee `windowfn.size.seconds` y `allowed_lateness.seconds`, pero el `Duration` de Beam
2.74 (la versión que fija el `uv.lock`) solo expone `.micros`. Con un `WindowInto`
común la prueba falla con `AttributeError`, sea cual sea la política. Para no tocar
la prueba, la función pasa a Beam una subclase de `Duration` que agrega `.seconds`.
`Duration.of()` devuelve sin cambios un objeto que ya es `Duration`, así que la
subclase llega intacta a la política. Para el runner sigue siendo un `Duration`
común: el `TestStream` del notebook y las pruebas propias ejecutan esta misma
política.

### TODO 7 y 8 · Clave idempotente y reintentos del sink

- **La clave es `merchant_id|window_start`:** identifica la entidad lógica, no la
  versión. Si incluyera el total o el índice de pane, cada revisión sería una fila
  nueva en lugar de un reemplazo. Un `merchant_id` que contenga `|` se rechaza,
  porque la clave sería ambigua.
- **Un reintento reescribe el bundle entero**, como hace Beam: cada intento escribe
  otra vez todos los resultados, no solo el que falló. La auditoría registra cada
  intento, con la operación y la cantidad de filas del sink después de escribir.
- **`POST` (append) frente a `UPSERT`:** con dos intentos, el `POST` duplica todos
  los totales. El `UPSERT` converge a una fila por clave.

*Evidencia de punta a punta:* los 8 panes que emite el `TestStream` (early, on-time
y late), escritos con UPSERT en el orden en que salen, dejan exactamente los 4
totales del oráculo.

*Limitación:* el UPSERT es *last-writer-wins*. Si el sink recibiera los panes de una
misma clave desordenados, un total viejo podría pisar a uno nuevo. Beam emite los
panes de una clave en orden, pero un sink externo con reintentos concurrentes podría
reordenarlos. La mitigación es guardar el `pane_index` y escribir solo si es mayor
que el guardado, es decir, un UPSERT condicional.

## Evidencia temporal con `TestStream`

El notebook y las pruebas reproducen el dataset en orden de llegada. Antes de cada
evento, el watermark y el reloj de procesamiento avanzan hasta su `arrival_time`.
Estos son los panes que emite la política por defecto:

| Comercio | Ventana | Pane | Total |
|---|---|---|---:|
| `m-azul` | 13:00 | EARLY | 120 000 |
| `m-verde` | 13:00 | EARLY | 80 000 |
| `m-azul` | 13:00 | ON_TIME | 120 000 |
| `m-verde` | 13:00 | ON_TIME | 80 000 |
| `m-azul` | 13:00 | **LATE** | **170 000** ← `p-004`, tardío pero aceptado |
| `m-verde` | 13:01 | ON_TIME | 90 000 |
| `m-azul` | 13:02 | EARLY | 200 000 |
| `m-azul` | 13:02 | ON_TIME | 200 000 |

`p-007` no genera ningún pane: llega a las 13:03:40, cuando el watermark ya pasó
13:01:00 + 120 s. El duplicado de `p-002` tampoco genera un pane, porque lo frena el
estado.

## Pruebas propias (`tests/test_propias.py`)

| Tema | Qué fija |
|---|---|
| Contrato temporal | `parse_utc` rechaza valores sin zona, con otro offset, con doble zona, vacíos o `None`, y conserva fracciones de segundo. El borde derecho de la ventana es exclusivo. La ventana coincide con `FixedWindows` de Beam. |
| Oráculo | Conteos de la configuración por defecto; auditoría en orden de llegada; efecto de no deduplicar; un `PENDING` previo no bloquea la confirmación; el reintento de un tardío no es duplicado, pero la copia tardía de un pago contado sí; una lateness en el límite exacto se acepta. |
| Pipeline | En batch deduplica y coincide con el oráculo sin tardíos. `TestStream` con revisión LATE aceptada y tardío descartado. El timer sin lateness deja pasar el duplicado tardío. Estructura de los triggers early y late. |
| Idempotencia | 4 × 2 intentos; los panes acumulativos convergen al último total; la clave no depende del total; separador inválido; `attempts` inválido. |

## Estructura

```text
notebook.py              notebook marimo con la implementación y las celdas de evidencia
data/payments.jsonl      dataset provisto (sin modificar)
tests/conftest.py        carga las definiciones desde notebook.py (provisto)
tests/test_assignment.py suite obligatoria (provista, sin modificar)
tests/test_propias.py    casos límite y prueba temporal con TestStream
evidencia/               salida de pytest y notebook ejecutado en HTML
```
