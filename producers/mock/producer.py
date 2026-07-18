#!/usr/bin/env python3
"""Mock producer — publishes synthetic metrics to metrics.raw every 2 seconds."""

from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import UTC, datetime

from kafka import KafkaProducer

PLANTS = ("plant-a", "plant-b", "plant-c")
EQUIPMENT = ("pump-01", "pump-02", "compressor-01", "turbine-01")
METRICS = ("temperature", "pressure", "flow_rate", "vibration")
UNITS = {
    "temperature": "C",
    "pressure": "bar",
    "flow_rate": "m3/h",
    "vibration": "mm/s",
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "metrics.raw")
INTERVAL_SEC = float(os.getenv("PUBLISH_INTERVAL_SEC", "2"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mock-producer")


def build_event() -> dict[str, object]:
    metric = random.choice(METRICS)
    return {
        "plant_id": random.choice(PLANTS),
        "equipment_id": random.choice(EQUIPMENT),
        "metric": metric,
        "value": round(random.uniform(10.0, 100.0), 2),
        "unit": UNITS[metric],
        "ts": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    log.info("publishing to %s every %ss via %s", TOPIC, INTERVAL_SEC, KAFKA_BOOTSTRAP)

    while True:
        event = build_event()
        future = producer.send(TOPIC, event)
        future.get(timeout=10)
        log.info("sent %s", event)
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
