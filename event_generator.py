"""
US-24 / TASK 1 y TASK 2 - Simulador de eventos del channel manager (F2)

Genera eventos de ciclo de vida de reserva (created, modified, cancelled,
check_in, check_out, no_show) siguiendo el esquema JSON definido y validado
en US-12 (ver carpeta schemas/), y simula entregas fuera de orden según lo
exigido por:
  - HU-24 AC2 (algunas llegan fuera de orden, p.ej. modificación antes de
    su creación)
  - Enunciado 5.2 F2: "deben llegar fuera de orden ocasionalmente"
  - Enunciado 5.3: "Eventos de modificación que llegan antes del evento
    de creación" (defecto de calidad a inyectar con tasa configurable)
  - CA-09: "Un evento de modificación que llega antes de la creación se
    procesa correctamente al llegar la creación."

Diseño:
- Cada reserva tiene una secuencia lógica de eventos con un
  reservation_version incremental (1, 2, 3, ...), igual que en los
  ejemplos de US-12.
- El generador arma primero la secuencia COMPLETA y en orden correcto
  para cada reserva (esto es la "verdad" de negocio).
- Luego, un paso de shuffle controlado decide, por reserva, si esa
  secuencia se entrega en orden o si se simula desorden moviendo un
  evento de versión > 1 antes que su evento de versión 1 (creación).
  La tasa de reservas afectadas es configurable (--out-of-order-rate).
- No se asume ningún framework de testing: se valida manualmente
  ejecutando el script y revisando output/events.jsonl (ver sección
  __main__ y README).
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Catálogo de referencia (coherente con F2/F3: 28 hoteles, canales de venta)
# ---------------------------------------------------------------------------

HOTEL_IDS = [f"hotel-{i:03d}" for i in range(1, 29)]  # 28 hoteles (F3)
ROOM_TYPES = ["SINGLE", "DOUBLE", "TWIN", "SUITE"]
CHANNELS = ["booking_channel_a", "booking_channel_b", "expedia", "direct_ota"]
CURRENCIES = ["EUR", "USD"]
CANCELLATION_REASONS = ["guest_request", "no_payment", "overbooking_hotel", "channel_error"]

SOURCE = "F2"
EVENT_SCHEMA_VERSION = 1


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


@dataclass
class EventIdSequencer:
    """Genera event_id correlativos y legibles, igual que evt-000001 en US-12."""

    counter: int = 0

    def next(self) -> str:
        self.counter += 1
        return f"evt-{self.counter:06d}"


@dataclass
class ReservationState:
    reservation_id: str
    hotel_id: str
    room_type: str
    channel: str
    currency: str
    base_check_in: datetime
    base_check_out: datetime
    adults: int
    children: int
    rooms: int
    rate_per_night: float
    version: int = 0

    def next_version(self) -> int:
        self.version += 1
        return self.version


def _envelope(event_id: str, event_type: str, occurred_at: datetime,
              reservation_id: str, reservation_version: int, hotel_id: str,
              data: dict) -> dict:
    """Arma el envelope común, igual al definido en US-12 (reservation_base.json)."""
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_version": EVENT_SCHEMA_VERSION,
        "occurred_at": _iso(occurred_at),
        "source": SOURCE,
        "reservation_id": reservation_id,
        "reservation_version": reservation_version,
        "hotel_id": hotel_id,
        "data": data,
    }


# ---------------------------------------------------------------------------
# Generación de una reserva base y su secuencia completa de eventos
# ---------------------------------------------------------------------------

def _make_base_reservation(rng: random.Random, reservation_seq: int,
                            start_date: datetime) -> ReservationState:
    hotel_id = rng.choice(HOTEL_IDS)
    room_type = rng.choice(ROOM_TYPES)
    channel = rng.choice(CHANNELS)
    currency = rng.choice(CURRENCIES)
    lead_days = rng.randint(1, 120)
    check_in = start_date + timedelta(days=lead_days)
    stay_nights = rng.randint(1, 7)
    check_out = check_in + timedelta(days=stay_nights)
    adults = rng.randint(1, 3)
    children = rng.choice([0, 0, 0, 1, 2])
    rooms = 1 if adults + children <= 3 else rng.choice([1, 2])
    rate_per_night = round(rng.uniform(60, 320), 2)

    return ReservationState(
        reservation_id=f"res-{10000 + reservation_seq}",
        hotel_id=hotel_id,
        room_type=room_type,
        channel=channel,
        currency=currency,
        base_check_in=check_in,
        base_check_out=check_out,
        adults=adults,
        children=children,
        rooms=rooms,
        rate_per_night=rate_per_night,
    )


def _total_amount(res: ReservationState, check_in: datetime, check_out: datetime) -> float:
    nights = max((check_out - check_in).days, 0)
    return round(nights * res.rooms * res.rate_per_night, 2)


def _build_event_sequence(res: ReservationState, rng: random.Random,
                           sequencer: EventIdSequencer, now: datetime) -> list[dict]:
    """
    Construye la secuencia de eventos EN ORDEN CORRECTO para una reserva.
    Cada reserva recorre un "guion" de vida elegido al azar entre varios
    posibles, siempre empezando por 'created' (version=1).
    """
    events: list[dict] = []
    t = now + timedelta(seconds=rng.uniform(0, 2))

    # --- 1. created (version 1), siempre presente ---
    total = _total_amount(res, res.base_check_in, res.base_check_out)
    created_data = {
        "status": "confirmed",
        "check_in": _date(res.base_check_in),
        "check_out": _date(res.base_check_out),
        "room_type": res.room_type,
        "rooms": res.rooms,
        "guests": {"adults": res.adults, "children": res.children},
        "channel": res.channel,
        "currency": res.currency,
        "total_amount": total,
    }
    events.append(_envelope(
        sequencer.next(), "reservation.created", t,
        res.reservation_id, res.next_version(), res.hotel_id, created_data,
    ))

    # --- 2. guion de vida posterior ---
    lifecycle = rng.choices(
        population=["simple", "modified_then_cancelled", "modified_then_stay",
                    "cancelled", "no_show"],
        weights=[30, 15, 25, 15, 15],
        k=1,
    )[0]

    current_check_in, current_check_out = res.base_check_in, res.base_check_out

    if lifecycle in ("modified_then_cancelled", "modified_then_stay"):
        t += timedelta(minutes=rng.uniform(1, 600))
        changed_fields = []
        if rng.random() < 0.6:
            current_check_out = current_check_out + timedelta(days=rng.choice([-1, 1, 2]))
            changed_fields.append("check_out")
        if rng.random() < 0.3:
            res.rooms = max(1, res.rooms + rng.choice([-1, 1]))
            changed_fields.append("rooms")
        new_total = _total_amount(res, current_check_in, current_check_out)
        if new_total != total:
            changed_fields.append("total_amount")
        total = new_total
        if not changed_fields:
            changed_fields = ["total_amount"]

        modified_data = {
            "status": "confirmed",
            "check_in": _date(current_check_in),
            "check_out": _date(current_check_out),
            "room_type": res.room_type,
            "rooms": res.rooms,
            "currency": res.currency,
            "total_amount": total,
            "changed_fields": changed_fields,
        }
        events.append(_envelope(
            sequencer.next(), "reservation.modified", t,
            res.reservation_id, res.next_version(), res.hotel_id, modified_data,
        ))

    if lifecycle in ("modified_then_cancelled", "cancelled"):
        t += timedelta(minutes=rng.uniform(1, 1440))
        cancelled_data = {
            "status": "cancelled",
            "cancellation_reason": rng.choice(CANCELLATION_REASONS),
            "cancelled_at": _iso(t),
            "currency": res.currency,
            "total_amount": total,
        }
        events.append(_envelope(
            sequencer.next(), "reservation.cancelled", t,
            res.reservation_id, res.next_version(), res.hotel_id, cancelled_data,
        ))

    elif lifecycle == "no_show":
        # no-show ocurre el día de check-in, sin check-in real
        t = datetime.combine(current_check_in.date(), datetime.min.time(),
                              tzinfo=timezone.utc) + timedelta(hours=23)
        no_show_data = {
            "status": "no_show",
            "check_in": _date(current_check_in),
            "check_out": _date(current_check_out),
            "channel": res.channel,
            "currency": res.currency,
            "total_amount": total,
        }
        events.append(_envelope(
            sequencer.next(), "reservation.no_show", t,
            res.reservation_id, res.next_version(), res.hotel_id, no_show_data,
        ))

    elif lifecycle in ("modified_then_stay", "simple"):
        # check-in real
        t_checkin = datetime.combine(current_check_in.date(), datetime.min.time(),
                                      tzinfo=timezone.utc) + timedelta(hours=15)
        room_number = str(rng.randint(100, 499))
        checkin_data = {
            "status": "checked_in",
            "actual_check_in": _iso(t_checkin),
            "room_number": room_number,
        }
        events.append(_envelope(
            sequencer.next(), "reservation.check_in", t_checkin,
            res.reservation_id, res.next_version(), res.hotel_id, checkin_data,
        ))

        t_checkout = datetime.combine(current_check_out.date(), datetime.min.time(),
                                       tzinfo=timezone.utc) + timedelta(hours=11)
        checkout_data = {
            "status": "checked_out",
            "actual_check_out": _iso(t_checkout),
        }
        events.append(_envelope(
            sequencer.next(), "reservation.check_out", t_checkout,
            res.reservation_id, res.next_version(), res.hotel_id, checkout_data,
        ))

    return events


# ---------------------------------------------------------------------------
# TASK 2: simulación de entrega fuera de orden
# ---------------------------------------------------------------------------

def apply_out_of_order(all_sequences: list[list[dict]], rng: random.Random,
                        rate: float) -> list[dict]:
    """
    Recibe la lista de secuencias (una por reserva, cada una en orden
    correcto) y devuelve UNA lista plana que representa el orden de
    PUBLICACIÓN/ENTREGA.

    Por defecto, intercala las secuencias por tiempo (simulando que varias
    reservas generan eventos concurrentemente, como pide F2 con >=10
    eventos/seg). Luego, para una fracción `rate` de las reservas que
    tienen más de un evento, mueve su segundo evento (normalmente
    'modified', version=2) a una posición ANTERIOR a su evento 'created'
    (version=1) en la lista final, simulando el defecto de calidad descrito
    en la sección 5.3 del enunciado y el AC2 de HU-24.
    """
    # 1) orden "feliz": intercalar por occurred_at como si todo llegara en orden
    flat = [ev for seq in all_sequences for ev in seq]
    flat.sort(key=lambda e: e["occurred_at"])

    # 2) elegir qué reservas se ven afectadas por desorden
    eligible_reservations = [
        seq[0]["reservation_id"] for seq in all_sequences if len(seq) >= 2
    ]
    n_affected = round(len(eligible_reservations) * rate)
    affected = set(rng.sample(eligible_reservations, k=min(n_affected, len(eligible_reservations))))

    if not affected:
        return flat

    # índice de cada evento dentro de `flat` para poder moverlo
    index_by_event_id = {ev["event_id"]: i for i, ev in enumerate(flat)}

    reordered = list(flat)
    for seq in all_sequences:
        res_id = seq[0]["reservation_id"]
        if res_id not in affected:
            continue
        created_ev = seq[0]  # version 1, siempre 'created'
        second_ev = seq[1]   # version 2 (modified, cancelled, check_in o no_show)

        created_pos = index_by_event_id[created_ev["event_id"]]
        second_pos = index_by_event_id[second_ev["event_id"]]

        if second_pos < created_pos:
            continue  # ya estaba desordenado por azar, no hace falta forzar

        # mover second_ev a justo antes de created_ev
        reordered.remove(second_ev)
        new_created_pos = reordered.index(created_ev)
        reordered.insert(new_created_pos, second_ev)

        # marcar explícitamente para trazabilidad/depuración (no forma parte
        # del esquema de negocio: es metadata de simulación)
        second_ev.setdefault("_simulation", {})["delivered_out_of_order"] = True
        second_ev["_simulation"]["expected_after_event_id"] = created_ev["event_id"]

        # refrescar índices tras el insert/remove
        index_by_event_id = {ev["event_id"]: i for i, ev in enumerate(reordered)}

    return reordered


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------

def generate(num_reservations: int, out_of_order_rate: float, seed: int,
             start_date: Optional[datetime] = None) -> list[dict]:
    rng = random.Random(seed)
    sequencer = EventIdSequencer()
    now = start_date or datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)

    sequences = []
    for i in range(1, num_reservations + 1):
        res = _make_base_reservation(rng, i, now)
        seq = _build_event_sequence(res, rng, sequencer, now)
        sequences.append(seq)

    return apply_out_of_order(sequences, rng, out_of_order_rate)


def main():
    parser = argparse.ArgumentParser(description="Simulador de eventos F2 (channel manager)")
    parser.add_argument("--reservations", type=int, default=50,
                         help="Número de reservas base a simular (default: 50)")
    parser.add_argument("--out-of-order-rate", type=float, default=0.1,
                         help="Fracción (0-1) de reservas con su 2do evento fuera de orden (default: 0.1)")
    parser.add_argument("--seed", type=int, default=42,
                         help="Semilla para reproducibilidad (default: 42)")
    parser.add_argument("--output", type=str, default="output/events.jsonl",
                         help="Ruta de salida en JSON Lines (default: output/events.jsonl)")
    args = parser.parse_args()

    events = generate(args.reservations, args.out_of_order_rate, args.seed)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    n_out_of_order = sum(1 for ev in events if ev.get("_simulation", {}).get("delivered_out_of_order"))
    print(f"Generados {len(events)} eventos para {args.reservations} reservas.")
    print(f"Eventos marcados como fuera de orden: {n_out_of_order}")
    print(f"Guardado en: {out_path}")


if __name__ == "__main__":
    main()
