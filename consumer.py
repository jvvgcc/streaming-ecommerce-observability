# consumer.py
from alerts import check_and_alert
import json
import time
from collections import Counter
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer

# ---- Config ----
BROKER = "localhost:9092"
TOPIC = "ecommerce-events"
GROUP_ID = "obs-consumer"

PG_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "streaming_dw",
    "user": "streaming_user",
    "password": "streaming_pass",
}

VALID_EVENT_TYPES = {"click", "add_to_cart", "purchase", "cancellation"}
METRICS_WINDOW_SECONDS = 30  # a cada 30s, fecha uma janela de métricas

# rastreia sessões que já tiveram add_to_cart, pra validar sequência de negócio
sessions_with_cart = set()
seen_event_ids = set()


def validate_event(event: dict) -> list[str]:
    """Retorna lista de erros encontrados. Lista vazia = evento válido."""
    errors = []

    required_fields = [
        "event_id", "event_type", "timestamp", "user_id",
        "session_id", "product_id", "product_price", "quantity",
    ]
    for field in required_fields:
        if field not in event or event[field] is None:
            errors.append(f"campo_faltando:{field}")

    if errors:
        # sem os campos básicos, não dá pra validar o resto com segurança
        return errors

    if event["event_type"] not in VALID_EVENT_TYPES:
        errors.append(f"event_type_desconhecido:{event['event_type']}")

    if not isinstance(event["quantity"], int):
        errors.append(f"tipo_errado:quantity={event['quantity']!r}")
    elif event["quantity"] <= 0:
        errors.append(f"fora_do_range:quantity={event['quantity']}")

    if not isinstance(event["product_price"], (int, float)):
        errors.append(f"tipo_errado:product_price={event['product_price']!r}")
    elif event["product_price"] <= 0:
        errors.append(f"fora_do_range:product_price={event['product_price']}")

    try:
        event_time = datetime.fromisoformat(event["timestamp"])
        if event_time > datetime.now(timezone.utc):
            errors.append("timestamp_no_futuro")
    except (ValueError, TypeError):
        errors.append("timestamp_invalido")

    if event["event_id"] in seen_event_ids:
        errors.append("evento_duplicado")

    if event["event_type"] == "purchase" and event["session_id"] not in sessions_with_cart:
        errors.append("sequencia_invalida:purchase_sem_add_to_cart")

    return errors


def persist_valid(cur, event):
    cur.execute(
        """
        INSERT INTO streaming.eventos_validos
            (event_id, event_type, event_timestamp, user_id, session_id,
             product_id, product_price, quantity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            event["event_id"], event["event_type"], event["timestamp"],
            event["user_id"], event["session_id"], event["product_id"],
            event["product_price"], event["quantity"],
        ),
    )


def persist_quarantine(cur, raw_event, errors):
    cur.execute(
        """
        INSERT INTO streaming.eventos_quarentena (raw_event, validation_errors)
        VALUES (%s, %s)
        """,
        (json.dumps(raw_event), errors),
    )


def persist_metrics(cur, window_start, window_end, total, valid, invalid):
    error_rate = round((invalid / total) * 100, 2) if total else 0
    cur.execute(
        """
        INSERT INTO streaming.pipeline_metrics
            (window_start, window_end, total_events, valid_events, invalid_events, error_rate)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (window_start, window_end, total, valid, invalid, error_rate),
    )


def main():
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True

    window_start = datetime.now(timezone.utc)
    window_total = 0
    window_valid = 0
    window_invalid = 0
    error_counter = Counter()

    print(f"Consumindo e validando eventos de '{TOPIC}'...")

    try:
        with conn.cursor() as cur:
            while True:
                msg = consumer.poll(1.0)

                if msg is not None and not msg.error():
                    event = json.loads(msg.value())
                    errors = validate_event(event)

                    window_total += 1

                    if errors:
                        persist_quarantine(cur, event, errors)
                        window_invalid += 1
                        error_counter.update(e.split(":")[0] for e in errors)
                    else:
                        persist_valid(cur, event)
                        seen_event_ids.add(event["event_id"])
                        if event["event_type"] == "add_to_cart":
                            sessions_with_cart.add(event["session_id"])
                        window_valid += 1

                # fecha a janela de métricas a cada N segundos
                now = datetime.now(timezone.utc)
                if (now - window_start).total_seconds() >= METRICS_WINDOW_SECONDS:
                    if window_total > 0:
                        persist_metrics(cur, window_start, now, window_total, window_valid, window_invalid)
                        error_rate = round((window_invalid / window_total) * 100, 2)
                        print(
                            f"[{now.strftime('%H:%M:%S')}] janela fechada: "
                            f"{window_total} eventos | {window_valid} válidos | "
                            f"{window_invalid} inválidos | erro: {error_rate}% | "
                            f"top erros: {error_counter.most_common(3)}"
                        )
                        check_and_alert(
                            window_start, now, window_total, window_valid,
                            window_invalid, error_rate, error_counter.most_common(3),
                        )
                    window_start = now
                    window_total = window_valid = window_invalid = 0
                    error_counter.clear()

    except KeyboardInterrupt:
        print("\nEncerrando consumidor...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()