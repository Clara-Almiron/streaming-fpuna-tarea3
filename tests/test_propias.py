"""Pruebas propias: casos límite y la prueba temporal con TestStream.

Complementan `test_assignment.py` (provisto por la cátedra, sin modificar) y
usan el mismo fixture `solution`, que carga las funciones desde `notebook.py`.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to

DATA_PATH = Path(__file__).parents[1] / "data" / "payments.jsonl"


def load_events() -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def payment(event_id, merchant_id, event_time, arrival_time, amount, status="CONFIRMED"):
    return {
        "event_id": event_id,
        "merchant_id": merchant_id,
        "event_time": event_time,
        "arrival_time": arrival_time,
        "amount": amount,
        "status": status,
    }


def replay_stream(solution, events) -> BeamTestStream:
    """Reproducir en orden de llegada, con watermark = arrival_time.

    Es la misma construcción que `build_replay_stream` del notebook; se repite
    acá porque el fixture solo expone las definiciones de la tarea.
    """
    stream = BeamTestStream()
    previous = None
    for event in sorted(events, key=lambda item: solution.parse_utc(item["arrival_time"])):
        arrival = solution.parse_utc(event["arrival_time"])
        if previous is not None and arrival > previous:
            stream = stream.advance_processing_time((arrival - previous).total_seconds())
        stream = stream.advance_watermark_to(arrival.timestamp())
        stream = stream.add_elements(
            [
                beam.window.TimestampedValue(
                    event, solution.parse_utc(event["event_time"]).timestamp()
                )
            ]
        )
        previous = arrival
    return stream.advance_watermark_to_infinity()


def streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def pane_rows(solution, pipeline, events, dedup_fn=None):
    """Panes (comercio, inicio, total, timing) del pipeline sobre un TestStream."""
    rows = solution.build_windowed_totals_pipeline(
        pipeline,
        pipeline | replay_stream(solution, events),
        windowing=solution.build_trigger_policy(),
        dedup_fn=dedup_fn,
        with_pane=True,
    )
    return rows | "Tupla" >> beam.Map(
        lambda row: (row["merchant_id"], row["window_start"], row["total"], row["pane_timing"])
    )


# --- 1. Tiempo de evento y ventanas -------------------------------------------------


@pytest.mark.parametrize(
    "raw_value",
    [
        "2026-07-24T13:00:05",  # sin zona: se interpretaría con la hora local
        "2026-07-24T13:00:05+03:00",  # otro offset: no respeta el contrato UTC
        "2026-07-24T13:00:05+00:00Z",  # doble zona
        "no es una fecha Z",
        "",
        None,
    ],
)
def test_parse_utc_rechaza_valores_fuera_de_contrato(solution, raw_value):
    with pytest.raises(ValueError, match="timestamp inválido"):
        solution.parse_utc(raw_value)


def test_parse_utc_conserva_fracciones_de_segundo(solution):
    parsed = solution.parse_utc("2026-07-24T13:00:05.250Z")

    assert parsed == datetime(2026, 7, 24, 13, 0, 5, 250_000, tzinfo=UTC)


def test_borde_derecho_de_la_ventana_es_exclusivo(solution):
    start, end = solution.assign_fixed_window(datetime(2026, 7, 24, 13, 1, tzinfo=UTC), 60)

    assert (start, end) == (
        datetime(2026, 7, 24, 13, 1, tzinfo=UTC),
        datetime(2026, 7, 24, 13, 2, tzinfo=UTC),
    )


def test_ventana_rechaza_datetime_sin_zona(solution):
    with pytest.raises(ValueError, match="zona horaria"):
        solution.assign_fixed_window(datetime(2026, 7, 24, 13, 0, 42), 60)


def test_ventana_alineada_igual_que_fixedwindows_de_beam(solution):
    timestamp = datetime(2026, 7, 24, 13, 7, 42, tzinfo=UTC)
    beam_window = beam.window.FixedWindows(300).assign(
        beam.window.WindowFn.AssignContext(timestamp.timestamp())
    )[0]

    start, end = solution.assign_fixed_window(timestamp, 300)

    assert start.timestamp() == beam_window.start.micros / 1e6
    assert end.timestamp() == beam_window.end.micros / 1e6


# --- 2. Oráculo determinista ---------------------------------------------------------


def test_conteos_de_la_configuracion_por_defecto(solution):
    events = load_events()
    totals, audit = solution.summarize_payments(events)

    assert len(events) == 9
    assert Counter(row["reason"] for row in audit) == {
        "accepted": 5,
        "not_confirmed": 2,
        "duplicate": 1,
        "too_late": 1,
    }
    assert len(totals) == 4
    # p-004 es el único aceptado que llega después del cierre de su ventana.
    assert [row["event_id"] for row in audit if row["revision"]] == ["p-004"]


def test_auditoria_en_orden_de_llegada(solution):
    _, audit = solution.summarize_payments(load_events())

    # En el archivo p-007 está antes que p-008, pero llega después.
    ids = [row["event_id"] for row in audit]
    assert ids.index("p-008") < ids.index("p-007")


def test_sin_deduplicar_el_duplicado_infla_el_total(solution):
    totals, _ = solution.summarize_payments(load_events(), deduplicate=False)

    assert {
        (row["merchant_id"], row["window_start"]): row["total"] for row in totals
    }[("m-verde", "2026-07-24T13:00:00+00:00")] == 160_000


def test_pending_previo_no_bloquea_la_confirmacion(solution):
    events = [
        payment("p-x", "m-a", "2026-07-24T13:00:05Z", "2026-07-24T13:00:06Z", 10, "PENDING"),
        payment("p-x", "m-a", "2026-07-24T13:00:05Z", "2026-07-24T13:00:30Z", 10),
    ]

    totals, audit = solution.summarize_payments(events)

    assert [row["total"] for row in totals] == [10]
    assert [row["reason"] for row in audit] == ["not_confirmed", "accepted"]


def test_reintento_de_un_tardio_no_es_duplicado(solution):
    """Un pago rechazado por tardío nunca se contó: su reintento no es un duplicado."""
    events = [
        payment("p-x", "m-a", "2026-07-24T13:00:00Z", "2026-07-24T13:05:00Z", 10),
        payment("p-x", "m-a", "2026-07-24T13:00:00Z", "2026-07-24T13:06:00Z", 10),
    ]

    totals, audit = solution.summarize_payments(events)

    assert totals == []
    assert [(row["reason"], row["duplicate"]) for row in audit] == [
        ("too_late", False),
        ("too_late", False),
    ]


def test_copia_tardia_de_un_pago_contado_es_duplicado(solution):
    """Si el original sí se contó, la copia es duplicado aunque además sea tardía."""
    events = [
        payment("p-x", "m-a", "2026-07-24T13:00:00Z", "2026-07-24T13:00:01Z", 10),
        payment("p-x", "m-a", "2026-07-24T13:00:00Z", "2026-07-24T13:05:00Z", 10),
    ]

    totals, audit = solution.summarize_payments(events)

    assert [row["total"] for row in totals] == [10]
    assert [(row["reason"], row["duplicate"], row["too_late"]) for row in audit] == [
        ("accepted", False, False),
        ("duplicate", True, True),
    ]


def test_lateness_en_el_limite_exacto_se_acepta(solution):
    events = [payment("p-x", "m-a", "2026-07-24T13:00:00Z", "2026-07-24T13:02:00Z", 10)]

    _, audit = solution.summarize_payments(events, allowed_lateness_seconds=120)

    assert audit[0]["delay_seconds"] == 120
    assert audit[0]["accepted"] is True
    assert audit[0]["revision"] is True


# --- 3. Pipeline Beam ----------------------------------------------------------------


def test_pipeline_batch_deduplica_y_coincide_con_el_oraculo_sin_tardios(solution):
    events = load_events()
    # En batch el watermark no refleja la llegada: ningún evento es tardío.
    expected, _ = solution.summarize_payments(events, allowed_lateness_seconds=10**9)

    with BeamTestPipeline() as pipeline:
        output = solution.build_windowed_totals_pipeline(pipeline, events)
        assert_that(output, equal_to(expected))


def test_teststream_revision_late_aceptada_y_tardio_descartado(solution):
    with streaming_pipeline() as pipeline:
        panes = pane_rows(solution, pipeline, load_events())
        final_and_late = panes | beam.Filter(lambda row: row[3] in ("ON_TIME", "LATE"))
        assert_that(
            final_and_late,
            equal_to(
                [
                    # p-004 llega tarde pero dentro de la lateness: pane LATE que
                    # corrige 120 000 → 170 000 (ACCUMULATING: total completo).
                    ("m-azul", "2026-07-24T13:00:00+00:00", 120_000, "ON_TIME"),
                    ("m-azul", "2026-07-24T13:00:00+00:00", 170_000, "LATE"),
                    # Ni el duplicado p-002 ni el tardío p-007 generan un pane LATE.
                    ("m-verde", "2026-07-24T13:00:00+00:00", 80_000, "ON_TIME"),
                    ("m-verde", "2026-07-24T13:01:00+00:00", 90_000, "ON_TIME"),
                    ("m-azul", "2026-07-24T13:02:00+00:00", 200_000, "ON_TIME"),
                ]
            ),
        )


def test_timer_sin_lateness_deja_pasar_el_duplicado_tardio(solution):
    """Si el estado expira al cierre de la ventana, el duplicado de p-002
    (llega 13:01:41, ventana cerrada a las 13:01:00) se cuenta dos veces."""
    with streaming_pipeline() as pipeline:
        panes = pane_rows(
            solution,
            pipeline,
            load_events(),
            dedup_fn=solution.DeduplicatePayments(allowed_lateness_seconds=0),
        )
        late = panes | beam.Filter(lambda row: row[3] == "LATE")
        assert_that(
            late,
            equal_to(
                [
                    ("m-azul", "2026-07-24T13:00:00+00:00", 170_000, "LATE"),
                    ("m-verde", "2026-07-24T13:00:00+00:00", 160_000, "LATE"),
                ]
            ),
        )


def test_politica_de_triggers_early_y_late(solution):
    from apache_beam.transforms import trigger

    policy = solution.build_trigger_policy(early_seconds=15)
    triggerfn = policy.windowing.triggerfn

    # Beam envuelve el early en Repeatedly: la estimación se repite cada 15 s de
    # processing time mientras la ventana siga abierta, no una sola vez.
    assert isinstance(triggerfn.early, trigger.Repeatedly)
    assert isinstance(triggerfn.early.underlying, trigger.AfterProcessingTime)
    assert triggerfn.early.underlying.delay == 15
    assert isinstance(triggerfn.late, trigger.Repeatedly)
    assert triggerfn.late.underlying == trigger.AfterCount(1)


# --- 4. Idempotencia -----------------------------------------------------------------


def test_cuatro_resultados_dos_intentos(solution):
    totals, _ = solution.summarize_payments(load_events())

    posted, post_audit = solution.simulate_sink_retries(totals, attempts=2, idempotent=False)
    upserted, upsert_audit = solution.simulate_sink_retries(totals, attempts=2, idempotent=True)

    assert (len(post_audit), len(posted)) == (8, 8)
    assert (len(upsert_audit), len(upserted)) == (8, 4)
    assert {row["idempotency_key"] for row in upserted} == {
        solution.make_idempotency_key(row) for row in totals
    }


def test_upsert_de_panes_acumulativos_converge_al_ultimo_total(solution):
    panes = [
        {"merchant_id": "m-a", "window_start": "w0", "window_end": "w1", "total": 120},
        {"merchant_id": "m-a", "window_start": "w0", "window_end": "w1", "total": 170},
    ]

    materialized, _ = solution.simulate_sink_retries(panes, attempts=1, idempotent=True)

    assert [row["total"] for row in materialized] == [170]


def test_clave_idempotente_no_depende_del_total(solution):
    first = {"merchant_id": "m-a", "window_start": "2026-07-24T13:00:00+00:00", "total": 1}
    revision = {**first, "total": 2}

    assert solution.make_idempotency_key(first) == solution.make_idempotency_key(revision)


def test_clave_idempotente_rechaza_el_separador(solution):
    with pytest.raises(ValueError, match="separador"):
        solution.make_idempotency_key(
            {"merchant_id": "m|a", "window_start": "2026-07-24T13:00:00+00:00"}
        )


def test_reintentos_invalidos(solution):
    with pytest.raises(ValueError, match="attempts"):
        solution.simulate_sink_retries([], attempts=0)
