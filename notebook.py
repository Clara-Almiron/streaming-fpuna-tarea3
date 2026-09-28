import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    from collections import Counter
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.testing.test_stream import TestStream
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Counter,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TestStream,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        json,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Alumna: **Clara Almirón** — C.I. 3980921

    Este notebook parte del esqueleto de la cátedra
    ([rparrapy/streaming-fpuna-clase6-tarea](https://github.com/rparrapy/streaming-fpuna-clase6-tarea))
    con los TODO 1–8 implementados. Las decisiones y sus trade-offs están
    explicados en el `README.md` del repositorio.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        # El contrato exige UTC explícito. Un valor sin zona se interpretaría con
        # la hora local de la máquina que corre el pipeline, y uno con otro offset
        # indica que el productor no respeta el contrato: ambos se rechazan.
        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError(
                f"timestamp inválido {raw_value!r}: se espera ISO-8601 en UTC "
                "terminado en 'Z' (p. ej. 2026-07-24T13:00:05Z)"
            )
        try:
            return datetime.fromisoformat(raw_value[:-1] + "+00:00")
        except ValueError as error:
            raise ValueError(
                f"timestamp inválido {raw_value!r}: no es un ISO-8601 válido"
            ) from error

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        # Import local: las pruebas compilan solo las definiciones de la tarea,
        # sin las celdas de imports del notebook.
        from datetime import UTC, timedelta

        if timestamp.utcoffset() is None:
            raise ValueError("assign_fixed_window requiere un datetime con zona horaria")
        if size_seconds <= 0:
            raise ValueError(f"size_seconds debe ser positivo, no {size_seconds}")

        # Misma alineación que FixedWindows de Beam (offset 0 desde la época Unix),
        # así el oráculo y el pipeline asignan cada evento a la misma ventana.
        # La aritmética con timedelta es exacta: no hay redondeos de float.
        size = timedelta(seconds=size_seconds)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        start = epoch + ((timestamp - epoch) // size) * size
        return start, start + size

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, datetime, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        from datetime import timedelta

        lateness = timedelta(seconds=allowed_lateness_seconds)
        totals: dict[tuple[str, datetime, datetime], int] = {}
        seen: set[tuple[str, str]] = set()
        audit: list[dict[str, Any]] = []

        # Se recorre en orden de llegada, no en el del archivo: «primera aparición»
        # y «llegó después del cierre» se definen respecto de cuándo llegó cada
        # evento. `sorted` es estable, así que un empate respeta el orden original.
        for event in sorted(events, key=lambda item: parse_utc(item["arrival_time"])):
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            delay = arrival_time - event_time
            window_start, window_end = assign_fixed_window(event_time, window_seconds)

            # `seen` guarda solo los pagos ya contados en un total: «duplicado»
            # significa «este pago ya se contó». Un PENDING previo con el mismo
            # event_id no bloquea la confirmación posterior, y el reintento de un
            # pago rechazado por tardío vuelve a salir too_late, no duplicate.
            confirmed = event["status"] == "CONFIRMED"
            identity = (event["merchant_id"], event["event_id"])
            duplicate = confirmed and identity in seen

            # Convención del curso: el atraso tolerado se mide por evento,
            # arrival_time - event_time (ver README, «Lateness»).
            too_late = delay > lateness

            if not confirmed:
                reason = "not_confirmed"
            elif duplicate and deduplicate:
                reason = "duplicate"
            elif too_late:
                reason = "too_late"
            else:
                reason = "accepted"
            accepted = reason == "accepted"

            if accepted:
                seen.add(identity)
                window_key = (event["merchant_id"], window_start, window_end)
                totals[window_key] = totals.get(window_key, 0) + event["amount"]

            audit.append(
                {
                    "event_id": event["event_id"],
                    "merchant_id": event["merchant_id"],
                    "status": event["status"],
                    "window_start": window_start.isoformat(),
                    "delay_seconds": int(delay.total_seconds()),
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    # La ventana [inicio, fin) está completa en `fin`: lo que llega
                    # desde ese instante ya corrige un resultado emitido.
                    "revision": accepted and arrival_time >= window_end,
                    "reason": reason,
                }
            )

        total_rows = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "total": total,
            }
            for (merchant_id, window_start, window_end), total in sorted(
                totals.items(), key=lambda item: (item[0][1], item[0][0])
            )
        ]
        return total_rows, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(json, mo):
    payment_events = [
        json.loads(_line)
        for _line in (mo.notebook_dir() / "data" / "payments.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if _line.strip()
    ]
    return (payment_events,)


@app.cell
def _(Counter, mo, payment_events, summarize_payments):
    oracle_totals, oracle_audit = summarize_payments(payment_events)
    _reasons = Counter(_row["reason"] for _row in oracle_audit)
    _revisions = sum(_row["revision"] for _row in oracle_audit)

    mo.vstack(
        [
            mo.md(
                f"""
                ### Evidencia: oráculo con la configuración por defecto

                Ventana de 60 s, lateness de 120 s, deduplicación activa.

                - **Entran {len(payment_events)} eventos**; se aceptan
                  **{_reasons["accepted"]}** ({_revisions} de ellos como revisión
                  tardía).
                - Se descartan {len(oracle_audit) - _reasons["accepted"]}:
                  {_reasons["not_confirmed"]} no confirmados,
                  {_reasons["duplicate"]} duplicado y {_reasons["too_late"]}
                  fuera de tolerancia.
                - Se producen **{len(oracle_totals)} totales** (comercio × ventana).
                """
            ),
            mo.ui.table(oracle_totals, selection=None, label="Totales"),
            mo.ui.table(oracle_audit, selection=None, label="Auditoría, en orden de llegada"),
        ]
    )
    return (oracle_totals,)


@app.cell
def _(Any, DeduplicatePayments, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
        windowing: Any = None,
        deduplicate: bool = True,
        dedup_fn: Any = None,
        with_pane: bool = False,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.

        `events` puede ser una lista (se crea con `Create`) o una PCollection,
        p. ej. la salida de un `TestStream`. `windowing` reemplaza las ventanas
        fijas por defecto, típicamente con `build_trigger_policy()`;
        `dedup_fn` reemplaza la instancia de `DeduplicatePayments` (p. ej. con
        otra lateness), y `with_pane` agrega el timing y el índice del pane.
        """

        def with_event_time(event):
            # El timestamp de Beam es el event_time, nunca el de llegada.
            return beam.window.TimestampedValue(
                event, parse_utc(event["event_time"]).timestamp()
            )

        def format_total(
            merchant_total,
            window=beam.DoFn.WindowParam,
            pane=beam.DoFn.PaneInfoParam,
        ):
            merchant_id, total = merchant_total
            row = {
                "merchant_id": merchant_id,
                "window_start": window.start.to_utc_datetime(has_tz=True).isoformat(),
                "window_end": window.end.to_utc_datetime(has_tz=True).isoformat(),
                "total": total,
            }
            if with_pane:
                from apache_beam.utils.windowed_value import PaneInfoTiming

                row["pane_timing"] = PaneInfoTiming.to_string(pane.timing)
                row["pane_index"] = pane.index
            return row

        if isinstance(events, beam.pvalue.PCollection):
            source = events
        else:
            source = pipeline | "Crear eventos" >> beam.Create(events)

        keyed = (
            source
            | "Tiempo de evento" >> beam.Map(with_event_time)
            | "Solo CONFIRMED" >> beam.Filter(lambda event: event["status"] == "CONFIRMED")
            | "Ventanas" >> (windowing or beam.WindowInto(beam.window.FixedWindows(window_seconds)))
            # La clave es el comercio antes del estado: así la deduplicación y el
            # total quedan aislados por comercio.
            | "Clave comercio"
            >> beam.Map(lambda event: (event["merchant_id"], event)).with_output_types(
                # Clave str: coder determinista, requisito del estado por clave.
                beam.typehints.Tuple[str, beam.typehints.Any]
            )
        )
        if deduplicate:
            keyed = keyed | "Deduplicar" >> beam.ParDo(dedup_fn or DeduplicatePayments())

        return (
            keyed
            | "Monto" >> beam.MapTuple(lambda merchant_id, event: (merchant_id, event["amount"]))
            | "Total por comercio" >> beam.CombinePerKey(sum)
            | "Formatear" >> beam.Map(format_total)
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            super().__init__()
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            # El estado de Beam es por clave y por ventana: los ids vistos son
            # los de este comercio en esta ventana. Alcanza, porque un duplicado
            # repite el event_time y cae siempre en la misma ventana.
            event_id = element[1]["event_id"]
            if event_id in set(seen_ids.read()):
                return
            seen_ids.add(event_id)
            # Hay que recordar el id mientras una copia todavía pueda ser
            # aceptada: hasta el cierre de la ventana más la lateness. Pasado ese
            # punto el runner descarta cualquier copia por tardía, así que el
            # estado ya no aporta nada y se limpia.
            expiry.set(window.max_timestamp() + self.allowed_lateness_seconds)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        early_seconds: int = 30,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        class SecondsDuration(Duration):
            """Duration de Beam que además expone `.seconds`.

            El `Duration` de Beam 2.74 solo tiene `.micros`, y la prueba provista
            inspecciona `size.seconds` y `allowed_lateness.seconds`. Como
            `Duration.of()` devuelve sin cambios un objeto que ya es `Duration`,
            esta subclase llega intacta a la política y el runner la usa igual
            que un `Duration` común.
            """

            @property
            def seconds(self) -> float:
                return self.micros / 1_000_000

        return beam.WindowInto(
            beam.window.FixedWindows(SecondsDuration(window_seconds)),
            trigger=trigger.AfterWatermark(
                # Estimación provisoria mientras la ventana sigue abierta.
                early=trigger.AfterProcessingTime(early_seconds),
                # Cada evento tardío aceptado corrige el total de inmediato.
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=SecondsDuration(allowed_lateness_seconds),
            # Cada pane trae el total completo, no el incremento: es lo que
            # permite que el sink haga UPSERT y el último valor escrito sea correcto.
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.

    **Respuesta.** El `SetState` guarda un `event_id` por cada pago aceptado.
    Los ids no se repiten entre ventanas y el stream no termina, así que
    sin expiración el conjunto solo crece: la memoria del worker, el tamaño de
    los checkpoints y el costo de cada `read()` aumentan sin límite. El timer de
    watermark vence en `fin de ventana + lateness`, que es justo el momento en
    que el runner empieza a descartar cualquier copia por tardía. Antes de ese
    punto limpiar sería un error, porque un duplicado todavía se contaría;
    después, recordar los ids no aporta nada.
    """)
    return


@app.cell
def _(beam, json):
    def run_and_collect(build, *, streaming: bool = False) -> list[dict]:
        """Ejecutar un pipeline local y devolver las filas que produce.

        Las filas se escriben a un archivo temporal porque el runner serializa
        las funciones: agregarlas a una lista del notebook no funcionaría.
        """
        import tempfile
        from pathlib import Path

        from apache_beam.options.pipeline_options import PipelineOptions

        with tempfile.TemporaryDirectory() as tmp_dir:
            sink_path = Path(tmp_dir) / "salida.jsonl"

            def write_row(row):
                with sink_path.open("a", encoding="utf-8") as sink:
                    sink.write(json.dumps(row) + "\n")

            with beam.Pipeline(options=PipelineOptions(streaming=streaming)) as pipeline:
                _ = build(pipeline) | "Recolectar" >> beam.Map(write_row)

            if not sink_path.exists():
                return []
            return [json.loads(line) for line in sink_path.read_text().splitlines()]

    return (run_and_collect,)


@app.cell
def _(
    build_windowed_totals_pipeline,
    mo,
    oracle_totals,
    payment_events,
    run_and_collect,
    summarize_payments,
):
    batch_totals = sorted(
        run_and_collect(
            lambda _pipeline: build_windowed_totals_pipeline(_pipeline, payment_events)
        ),
        key=lambda _row: (_row["window_start"], _row["merchant_id"]),
    )
    _oracle_without_lateness, _ = summarize_payments(
        payment_events, allowed_lateness_seconds=10**9
    )

    mo.vstack(
        [
            mo.md(
                f"""
                ### Evidencia: el mismo pipeline en batch

                Con `Create` todo el dataset está disponible de entrada y el
                watermark salta al final recién después de leerlo: **no hay datos
                tardíos**. Por eso el batch cuenta también `p-007` y coincide con
                el oráculo sin límite de lateness
                (**{batch_totals == _oracle_without_lateness}**), no con el de
                120 s (**{batch_totals == oracle_totals}**). Para ver lateness hace
                falta reproducir el orden de llegada: eso hace el `TestStream` de
                abajo.
                """
            ),
            mo.ui.table(batch_totals, selection=None, label="Totales del pipeline batch"),
        ]
    )
    return


@app.cell
def _(TestStream, beam, parse_utc):
    def build_replay_stream(events):
        """Reproducir los eventos en orden de llegada con un TestStream.

        Antes de cada evento, el watermark y el reloj de procesamiento avanzan
        hasta su `arrival_time`: es un watermark perfecto que refleja la llegada,
        así los atrasos del dataset se vuelven atrasos reales para Beam.
        """
        stream = TestStream()
        previous_arrival = None
        for event in sorted(events, key=lambda item: parse_utc(item["arrival_time"])):
            arrival = parse_utc(event["arrival_time"])
            if previous_arrival is not None and arrival > previous_arrival:
                stream = stream.advance_processing_time(
                    (arrival - previous_arrival).total_seconds()
                )
            stream = stream.advance_watermark_to(arrival.timestamp())
            stream = stream.add_elements(
                [
                    beam.window.TimestampedValue(
                        event, parse_utc(event["event_time"]).timestamp()
                    )
                ]
            )
            previous_arrival = arrival
        return stream.advance_watermark_to_infinity()

    return (build_replay_stream,)


@app.cell
def _(
    build_replay_stream,
    build_trigger_policy,
    build_windowed_totals_pipeline,
    mo,
    payment_events,
    run_and_collect,
):
    stream_panes = run_and_collect(
        lambda _pipeline: build_windowed_totals_pipeline(
            _pipeline,
            _pipeline | "Replay" >> build_replay_stream(payment_events),
            windowing=build_trigger_policy(),
            with_pane=True,
        ),
        streaming=True,
    )

    mo.vstack(
        [
            mo.md(
                """
                ### Evidencia: `TestStream` con la política de triggers

                Cada fila es un pane emitido, en el orden en que salió del
                pipeline. `p-004` llega a las 13:01:35, tarde para la ventana
                13:00 de `m-azul`, pero dentro de la lateness: produce un pane
                **LATE** que corrige el total de 120 000 a 170 000. `p-007` llega
                169 s después de su evento y el runner lo descarta: la ventana
                13:00 de `m-verde` se queda en 80 000.
                """
            ),
            mo.ui.table(stream_panes, selection=None, label="Panes emitidos"),
        ]
    )
    return (stream_panes,)


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        # La clave identifica la entidad lógica (comercio × ventana), no la
        # versión: ni el total ni el índice de pane forman parte de ella, porque
        # entonces cada revisión sería una fila nueva en lugar de un reemplazo.
        merchant_id = result["merchant_id"]
        if "|" in merchant_id:
            raise ValueError(
                f"merchant_id {merchant_id!r} contiene el separador '|': "
                "la clave sería ambigua"
            )
        return f"{merchant_id}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts < 1:
            raise ValueError(f"attempts debe ser al menos 1, no {attempts}")

        operation = "UPSERT" if idempotent else "POST"
        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []

        # Beam reintenta bundles completos: cada intento vuelve a escribir todos
        # los resultados, no solo el que falló.
        for attempt in range(1, attempts + 1):
            for result in results:
                key = make_idempotency_key(result)
                row = {**result, "idempotency_key": key, "attempt": attempt}
                if idempotent:
                    upsert_sink[key] = row
                else:
                    append_sink.append(row)
                audit.append(
                    {
                        "attempt": attempt,
                        "operation": operation,
                        "idempotency_key": key,
                        "total": result["total"],
                        "sink_rows": len(upsert_sink) if idempotent else len(append_sink),
                    }
                )

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return (simulate_sink_retries,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.
    """)
    return


@app.cell
def _(mo, oracle_totals, simulate_sink_retries, stream_panes):
    _posted, _post_audit = simulate_sink_retries(oracle_totals, attempts=2, idempotent=False)
    _upserted, _upsert_audit = simulate_sink_retries(oracle_totals, attempts=2, idempotent=True)

    # Los panes del TestStream escritos en el orden en que salieron: el UPSERT
    # se queda con el último de cada clave, que en modo ACCUMULATING es el total.
    _pane_rows = [
        {_k: _row[_k] for _k in ("merchant_id", "window_start", "window_end", "total")}
        for _row in stream_panes
    ]
    _from_panes, _ = simulate_sink_retries(_pane_rows, attempts=1, idempotent=True)
    _final_from_panes = sorted(
        (
            {_k: _row[_k] for _k in ("merchant_id", "window_start", "window_end", "total")}
            for _row in _from_panes
        ),
        key=lambda _row: (_row["window_start"], _row["merchant_id"]),
    )

    mo.vstack(
        [
            mo.md(
                f"""
                ### Evidencia: reintentos contra el sink

                {len(oracle_totals)} resultados × 2 intentos =
                **{len(_post_audit)} filas de auditoría** en ambos modos.
                El `POST` materializa **{len(_posted)}** filas; el `UPSERT`,
                **{len(_upserted)}**.

                Además, los {len(_pane_rows)} panes del `TestStream` escritos con
                `UPSERT` convergen a los mismos totales que el oráculo:
                **{_final_from_panes == oracle_totals}**.
                """
            ),
            mo.ui.table(_upsert_audit, selection=None, label="Auditoría UPSERT"),
            mo.ui.table(_posted, selection=None, label="Sink POST materializado"),
            mo.ui.table(_upserted, selection=None, label="Sink UPSERT materializado"),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.

    Las pruebas propias (`tests/test_propias.py`) agregan casos límite y la
    prueba temporal con `TestStream`.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
