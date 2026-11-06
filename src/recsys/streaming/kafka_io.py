"""Kafka topics and producer (host side; brokers at localhost:9092)."""

from __future__ import annotations

import logging
import os
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# 127.0.0.1, not localhost: the broker port is bound to IPv4 loopback only, and
# librdkafka trying [::1] first can fail metadata calls.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_HOST", "127.0.0.1:9092")
TOPIC, DLQ_TOPIC = "interactions", "interactions-dlq"
log = logging.getLogger(__name__)


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def recreate_topics() -> None:
    a = admin()
    existing = set(a.list_topics(timeout=10).topics)
    to_delete = [t for t in (TOPIC, DLQ_TOPIC) if t in existing]
    if to_delete:
        for f in a.delete_topics(to_delete).values():
            f.result()
        for _ in range(50):
            if not set(to_delete) & set(a.list_topics(timeout=10).topics):
                break
            time.sleep(0.2)
    for _ in range(50):  # deletion is asynchronous on the broker
        futures = a.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in (TOPIC, DLQ_TOPIC)])
        try:
            for f in futures.values():
                f.result()
            return
        except Exception as exc:
            if "already exists" not in str(exc) and "marked for deletion" not in str(exc):
                raise
            time.sleep(0.3)
    raise RuntimeError("could not recreate topics")


def producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 5, "acks": "all",
                     "enable.idempotence": True, "batch.num.messages": 10000})


def send(p: Producer, topic: str, value: bytes) -> None:
    """Produce with backpressure: when the local queue is full, serve delivery callbacks and retry."""
    while True:
        try:
            p.produce(topic, value)
            p.poll(0)
            return
        except BufferError:
            p.poll(0.05)


def topic_size(topic: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "recsys-inspect", "enable.auto.commit": False})
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
    c.close()
    return hi - lo
