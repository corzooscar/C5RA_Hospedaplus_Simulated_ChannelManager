# Channel Manager Event Simulator (F2) — HU-24

Implement the 3 tasks of HU-24 (US-24, B0-90):

* **TASK 1 (`event_generator.py`):** Generates valid `created`, `modified`, `cancelled`, `no_show`, `check_in`, and `check_out` events according to the schema defined in US-12 (`schemas/`).
* **TASK 2 (included in `event_generator.py`, `apply_out_of_order` function):** Simulates out-of-order delivery — by default, a configurable fraction of reservations will have their second event (e.g., `modified`) delivered before their `created` event.
* **TASK 3 (`rabbitmq_publisher.py`):** Publishes the generated batch to RabbitMQ, vhost `reservas`, durable topic exchange `channel_manager.events`, using persistent messages (`delivery_mode=2`).

## Usage

```bash
pip install pika  # Only if you are going to publish to RabbitMQ (not required for dry-run)

# 1. Generate events
python3 event_generator.py \
  --reservations 100 \
  --out-of-order-rate 0.1 \
  --seed 42 \
  --output output/events.jsonl

# 2. Review what would be published, without a live connection
python3 rabbitmq_publisher.py --input output/events.jsonl --dry-run

# 3. Publish to a live RabbitMQ instance (adjust host/credentials)
python3 rabbitmq_publisher.py \
  --input output/events.jsonl \
  --host localhost --port 5672 \
  --vhost reservas --user guest --password guest \
  --rate 10   # Simulates the peak of >=10 events/second required by F2
```

## Manual Validation (Without a Testing Framework)

```python
import json
events = [json.loads(l) for l in open("output/events.jsonl")]

# AC1: The 4 required types by US-12 / HU-24 must exist
types = {e["event_type"] for e in events}
assert {"reservation.created", "reservation.modified",
        "reservation.cancelled", "reservation.no_show"} <= types

# AC2: There must be at least one case of out-of-order delivery
assert any(e.get("_simulation", {}).get("delivered_out_of_order") for e in events)
```

## Design Notes

* The `reservation_version` field (1, 2, 3...) is used by the live Consumer to apply events idempotently and detect out-of-order issues (CA-09 from the prompt): if `version=2` arrives without having seen `version=1`, it knows it must wait/retry instead of applying the event blindly.
* The `_simulation` field is metadata intended solely for simulator testing/debugging — it is not part of the actual event contract and the Consumer should not rely on it. If a 100% "clean" payload is preferred (identical to what a real channel manager would send), it can be removed before publishing; it was left in because it simplifies verifying AC2 without having to manually re-derive the correct order.
* The `--rate` parameter in the publisher allows testing the F2 requirement of "≥10 peak events/second" in a controlled manner during local tests; in production, this pace is driven by the actual volume of reservations, not the simulator.
