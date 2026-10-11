# Simulador de eventos del channel manager (F2) — HU-24

Implementa las 3 tareas de HU-24 (US-24, B0-90):

- **TASK 1** (`event_generator.py`): genera eventos `created`, `modified`,
  `cancelled`, `no_show`, `check_in`, `check_out` válidos según el esquema
  definido en US-12 (`schemas/`).
- **TASK 2** (incluida en `event_generator.py`, función `apply_out_of_order`):
  simula entrega fuera de orden — por defecto, una fracción configurable de
  reservas tiene su segundo evento (p.ej. `modified`) entregado antes que su
  `created`.
- **TASK 3** (`rabbitmq_publisher.py`): publica el lote generado en RabbitMQ,
  vhost `reservas`, exchange topic durable `channel_manager.events`, con
  mensajes persistentes (`delivery_mode=2`).

## Uso

```bash
pip install pika  # solo si vas a publicar a RabbitMQ (no hace falta para dry-run)

# 1. Generar eventos
python3 event_generator.py \
  --reservations 100 \
  --out-of-order-rate 0.1 \
  --seed 42 \
  --output output/events.jsonl

# 2. Revisar qué se publicaría, sin conexión real
python3 rabbitmq_publisher.py --input output/events.jsonl --dry-run

# 3. Publicar contra un RabbitMQ real (ajustar host/credenciales)
python3 rabbitmq_publisher.py \
  --input output/events.jsonl \
  --host localhost --port 5672 \
  --vhost reservas --user guest --password guest \
  --rate 10   # simula el pico de >=10 eventos/segundo que exige F2
```

## Validación manual (sin framework de tests)

```python
import json
events = [json.loads(l) for l in open("output/events.jsonl")]

# AC1: deben existir los 4 tipos exigidos por US-12 / HU-24
types = {e["event_type"] for e in events}
assert {"reservation.created", "reservation.modified",
        "reservation.cancelled", "reservation.no_show"} <= types

# AC2: debe haber al menos un caso de desorden
assert any(e.get("_simulation", {}).get("delivered_out_of_order") for e in events)
```

## Notas de diseño

- El campo `reservation_version` (1, 2, 3...) es el que usa el Consumer real
  para aplicar eventos de forma idempotente y detectar fuera de orden
  (CA-09 del enunciado): si llega version=2 sin haber visto version=1, sabe
  que debe esperar/reintentar en vez de aplicar el evento a ciegas.
- El campo `_simulation` es metadata **solo para pruebas/depuración** del
  simulador — no forma parte del contrato de evento real y el Consumer no
  debería depender de él. Si se prefiere un payload 100% "limpio" (idéntico
  al que mandaría un channel manager real), se puede quitar antes de
  publicar; lo dejé porque facilita verificar AC2 sin tener que re-derivar
  el orden correcto a mano.
- `--rate` en el publisher sirve para ensayar el requisito de `F2` de
  "≥10 eventos/segundo pico" de forma controlada en pruebas locales; en
  producción ese ritmo lo marca el volumen real de reservas, no el
  simulador.
