# generator.py
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

# ---- Config ----
BROKER = "localhost:9092"
TOPIC = "ecommerce-events"
DIRTY_RATE = 0.15  # 15% dos eventos saem "sujos" de propósito
EVENTS_PER_SECOND = 2

PRODUCTS = [f"PROD-{i:03d}" for i in range(1, 21)]
DIRTY_TYPES = [
    "missing_field",
    "wrong_type",
    "timestamp_future",
    "duplicate_id",
    "out_of_range",
    "sequence_violation",
    "unknown_event_type",
]

producer = Producer({"bootstrap.servers": BROKER})

# guarda sessões ativas pra simular jornada e permitir sequence_violation
active_sessions = {}
last_duplicate_event = None


def base_event(event_type, session_id, user_id, product_id):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "product_price": round(random.uniform(10, 500), 2),
        "quantity": random.randint(1, 5),
    }


def dirty_event(event):
    """Aplica um tipo de sujeira aleatório ao evento."""
    global last_duplicate_event
    dirty_type = random.choice(DIRTY_TYPES)

    if dirty_type == "missing_field":
        field = random.choice(["product_price", "quantity", "user_id"])
        event.pop(field, None)

    elif dirty_type == "wrong_type":
        event["quantity"] = random.choice(["dois", "N/A", None])

    elif dirty_type == "timestamp_future":
        future = datetime.now(timezone.utc) + timedelta(hours=random.randint(1, 48))
        event["timestamp"] = future.isoformat()

    elif dirty_type == "duplicate_id":
        if last_duplicate_event:
            return last_duplicate_event  # reenvia um evento já visto
        last_duplicate_event = event

    elif dirty_type == "out_of_range":
        field = random.choice(["product_price", "quantity"])
        event[field] = random.choice([-10, 0, -1])

    elif dirty_type == "sequence_violation":
        event["event_type"] = "purchase"  # compra sem add_to_cart prévio

    elif dirty_type == "unknown_event_type":
        event["event_type"] = "wishlist_add"  # fora do enum conhecido

    return event


def delivery_report(err, msg):
    if err is not None:
        print(f"Falha ao entregar mensagem: {err}")


def generate_loop():
    print(f"Gerando eventos em {TOPIC} (taxa de sujeira: {DIRTY_RATE*100:.0f}%)...")
    while True:
        session_id = random.choice(list(active_sessions.keys())) if active_sessions and random.random() < 0.6 else str(uuid.uuid4())
        user_id = active_sessions.get(session_id, f"user-{random.randint(1, 200)}")
        active_sessions[session_id] = user_id

        event_type = random.choices(
            ["click", "add_to_cart", "purchase", "cancellation"],
            weights=[0.5, 0.3, 0.15, 0.05],
        )[0]
        product_id = random.choice(PRODUCTS)

        event = base_event(event_type, session_id, user_id, product_id)

        if random.random() < DIRTY_RATE:
            event = dirty_event(event)

        producer.produce(
            TOPIC,
            key=session_id,
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)

        time.sleep(1 / EVENTS_PER_SECOND)


if __name__ == "__main__":
    generate_loop()