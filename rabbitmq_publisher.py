"""
US-24 / TASK 3 - Publicar eventos simulados en RabbitMQ (vhost "reservas")

Coherente con lo documentado en el proyecto (HospedaPlus_Proceso_Explicado):
- Broker: RabbitMQ, vhost dedicado "reservas" para eventos del channel manager.
- Productor: aquí (simula a FastAPI/F2 publicando eventos reales).
- Cola durable + mensajes persistentes -> ningún evento se pierde (RNF-05).
- El consumidor real (fuera de alcance de esta historia) hace ACK y aplica
  los eventos de forma idempotente, manejando los que llegan fuera de orden.

Este script NO decide el orden de los eventos: solo los publica en el orden
en que vienen en el archivo de entrada (output/events.jsonl), que ya fue
construido por event_generator.py incluyendo los casos de desorden
simulado. Así, publicar es una simple inyección secuencial que preserva
el escenario de prueba generado.

Uso:
    python3 rabbitmq_publisher.py --input output/events.jsonl \
        --host localhost --port 5672 --vhost reservas \
        --user guest --password guest \
        --exchange channel_manager.events --queue reservation_events
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

try:
    import pika
except ImportError:
    pika = None


EXCHANGE_TYPE = "topic"  # permite routing keys jerárquicas: reservation.created, reservation.modified, ...


def _routing_key(event: dict) -> str:
    """event_type ya viene como 'reservation.created', etc. -> se usa directo como routing key."""
    return event["event_type"]


def build_connection_params(host: str, port: int, vhost: str, user: str, password: str) -> "pika.ConnectionParameters":
    credentials = pika.PlainCredentials(user, password)
    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=10,
    )


def publish_events(input_path: Path, host: str, port: int, vhost: str,
                    user: str, password: str, exchange: str, queue: str,
                    rate_per_second: float | None, dry_run: bool) -> int:
    events = [json.loads(line) for line in input_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    if dry_run:
        print(f"[dry-run] Se publicarían {len(events)} eventos. No se abre conexión a RabbitMQ.")
        for ev in events[:5]:
            print(f"  -> routing_key={_routing_key(ev)} reservation_id={ev['reservation_id']} "
                  f"version={ev['reservation_version']}")
        if len(events) > 5:
            print(f"  ... y {len(events) - 5} más.")
        return len(events)

    if pika is None:
        print("ERROR: falta la librería 'pika'. Instálala con: pip install pika", file=sys.stderr)
        return 0

    params = build_connection_params(host, port, vhost, user, password)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Exchange topic durable: sobrevive a un reinicio del broker.
    channel.exchange_declare(exchange=exchange, exchange_type=EXCHANGE_TYPE, durable=True)

    # Cola durable ligada al exchange con wildcard, para que el Consumer
    # reciba todos los reservation.* (creación, modificación, cancelación,
    # no-show, check-in, check-out).
    channel.queue_declare(queue=queue, durable=True)
    channel.queue_bind(exchange=exchange, queue=queue, routing_key="reservation.*")

    published = 0
    for ev in events:
        body = json.dumps(ev, ensure_ascii=False).encode("utf-8")
        channel.basic_publish(
            exchange=exchange,
            routing_key=_routing_key(ev),
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistente: sobrevive a un reinicio del broker (RNF-05)
                message_id=ev["event_id"],
            ),
        )
        published += 1
        if rate_per_second:
            time.sleep(1.0 / rate_per_second)

    connection.close()
    return published


def main():
    parser = argparse.ArgumentParser(description="Publica eventos simulados de F2 en RabbitMQ")
    parser.add_argument("--input", type=str, default="output/events.jsonl")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=5672)
    parser.add_argument("--vhost", type=str, default="reservas")
    parser.add_argument("--user", type=str, default="guest")
    parser.add_argument("--password", type=str, default="guest")
    parser.add_argument("--exchange", type=str, default="channel_manager.events")
    parser.add_argument("--queue", type=str, default="reservation_events")
    parser.add_argument("--rate", type=float, default=None,
                         help="Eventos por segundo a publicar (p.ej. 10 para simular el pico de F2). "
                              "Si se omite, publica tan rápido como pueda.")
    parser.add_argument("--dry-run", action="store_true",
                         help="No conecta a RabbitMQ: solo valida y resume lo que publicaría.")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: no existe {input_path}. Corre antes event_generator.py", file=sys.stderr)
        sys.exit(1)

    n = publish_events(
        input_path, args.host, args.port, args.vhost, args.user, args.password,
        args.exchange, args.queue, args.rate, args.dry_run,
    )
    print(f"Publicados {n} eventos en exchange='{args.exchange}' (vhost='{args.vhost}').")


if __name__ == "__main__":
    main()
