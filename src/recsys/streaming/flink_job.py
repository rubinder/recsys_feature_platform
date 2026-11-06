"""PyFlink job: Kafka interactions -> streaming user features -> Feast feature server.

    parse + validate (pydantic contract) ──invalid──> Kafka `interactions-dlq`
      │ event-time timestamps + bounded-out-of-orderness watermarks
      ▼
    keyBy(user_id) -> UserFeatures (KeyedProcessFunction, UserStreamState in keyed state)
      │ late or out-of-order events ─────────────────> Kafka `interactions-dlq`
      ├─ history (event time < REPLAY_START): update state only (the backfill
      │  already holds these rows offline and Redis was materialized from it)
      └─ replay: push each feature row online via Feast /push (Redis), buffer
         rows and append them offline as atomic Parquet parts in the push
         source's batch directory; record stream progress (seq) in Redis

Parallelism is 1 so the progress marker is a total order the simulator can wait on.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, ProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from recsys.config import load_split
from recsys.contracts import InteractionEvent
from recsys.streaming.logic import UserStreamState

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "interactions")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "interactions-dlq")
FEATURE_SERVER = os.environ.get("FEATURE_SERVER_URL", "http://feature-server:6566")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
REPLAY_START = load_split().validation_end.timestamp()  # history before, live pushes from here
OUT_OF_ORDERNESS_S = 5
ONLINE_BATCH = 200       # online pushes are micro-batched...
QUIET_MS = 20            # ...and flushed after 20 ms without new events
OFFLINE_BATCH = 2000     # offline parts: 2000 rows or 10 s, whichever first
OFFLINE_MAX_AGE_S = 10
OFFLINE_DIR = os.environ.get("STREAM_OFFLINE_DIR", "/opt/recsys-data/user_stream")
PROGRESS_KEY = "recsys:stream:progress"
PUSH_SOURCE = "user_stream_push"

DLQ = OutputTag("dlq", Types.STRING())
EVENT = Types.TUPLE([Types.LONG(), Types.LONG(), Types.LONG(), Types.DOUBLE()])  # seq, user, item, ts
log = logging.getLogger("recsys.flink")


def _dlq(raw: str, reason: str) -> str:
    return json.dumps({"reason": reason, "raw": raw[:2000], "at": datetime.now(timezone.utc).isoformat()})


class Validate(ProcessFunction):
    """Contract check: malformed events go to the DLQ, valid ones continue as typed tuples."""

    def process_element(self, raw, ctx):
        try:
            e = InteractionEvent.model_validate_json(raw)
        except Exception as exc:  # pydantic ValidationError or bad JSON
            yield DLQ, _dlq(raw, f"contract: {str(exc).splitlines()[0][:300]}")
            return
        yield e.seq, e.user_id, e.item_id, e.timestamp.timestamp()


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[3] * 1000)


def _row_json(row: dict) -> dict:
    out = dict(row)
    # Fixed format: isoformat() drops ".000000" when microseconds are 0, and a
    # batch mixing both formats fails pandas' inferred-format parsing in Feast.
    out["event_timestamp"] = row["event_timestamp"].strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    return out


class UserFeatures(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        import httpx
        import redis

        self.state = runtime_context.get_state(ValueStateDescriptor("user_stream", Types.PICKLED_BYTE_ARRAY()))
        self.http = httpx.Client(base_url=FEATURE_SERVER, timeout=30)
        self.redis = redis.Redis.from_url(REDIS_URL)
        self.offline: list[dict] = []
        self.online: list[dict] = []
        self.offline_since = None
        self.max_seq = -1
        # Two independent processing-time timers: a 20 ms "quiet" timer that
        # flushes online rows / progress, and a 1 s maintenance timer that ages
        # out the offline buffer. Sharing one flag let the 1 s timer delay
        # online flushes (measured: ~1 s end-to-end per event).
        self.quiet_at = None
        self.maint_at = None
        self.pushed = 0

    def _push(self, rows: list[dict], to: str) -> None:
        df = {k: [r[k] for r in rows] for k in rows[0]}
        for attempt in range(5):
            try:
                resp = self.http.post("/push", json={"push_source_name": PUSH_SOURCE, "df": df, "to": to})
                resp.raise_for_status()
                return
            except Exception:
                if attempt == 4:
                    raise
                time.sleep(0.2 * 2**attempt)

    def _flush_offline(self) -> None:
        """Append-only part file, written to a temp name then renamed (readers never see partial files)."""
        if not self.offline:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        from recsys.streaming.schema import ARROW_SCHEMA

        rows, self.offline, self.offline_since = self.offline, [], None
        table = pa.Table.from_pylist([{k: v for k, v in r.items() if k != "_seq"} for r in rows], schema=ARROW_SCHEMA)
        name = f"part-live-{rows[0]['_seq']:09d}-{rows[-1]['_seq']:09d}"
        tmp = os.path.join(OFFLINE_DIR, f".{name}.tmp")
        pq.write_table(table, tmp)
        os.replace(tmp, os.path.join(OFFLINE_DIR, f"{name}.parquet"))

    def _flush_online(self) -> None:
        """Push buffered rows to Redis via Feast, then advance the progress marker past them."""
        if self.online:
            rows, self.online = self.online, []
            self._push(rows, "online")
            self.redis.set(PROGRESS_KEY, self.max_seq)

    def process_element(self, value, ctx):
        seq, user_id, item_id, ts = value
        wm = ctx.timer_service().current_watermark()
        # Keyed state = (UserStreamState, last seq applied). Kafka producers are
        # at-least-once, so a redelivered seq is dropped instead of applied twice.
        st, last_seq = self.state.value() or (UserStreamState(), -1)
        if seq <= last_seq:
            yield DLQ, _dlq(json.dumps(list(value)), f"duplicate: seq {seq} <= last applied {last_seq}")
            return
        if wm is not None and wm > 0 and ts * 1000 < wm - OUT_OF_ORDERNESS_S * 1000:
            yield DLQ, _dlq(json.dumps(list(value)), "late: behind watermark")
            return
        try:
            row = st.update(user_id, item_id, ts)
        except ValueError as exc:
            yield DLQ, _dlq(json.dumps(list(value)), f"out-of-order: {exc}")
            return
        self.state.update((st, seq))
        self.max_seq = max(self.max_seq, seq)

        if ts >= REPLAY_START:
            self.online.append(_row_json(row))
            self.offline.append({**row, "_seq": seq})
            self.offline_since = self.offline_since or time.monotonic()
            self.pushed += 1
            if len(self.online) >= ONLINE_BATCH:
                self._flush_online()
            if len(self.offline) >= OFFLINE_BATCH:
                self._flush_offline()
        elif self.max_seq % 5000 == 0:
            self.redis.set(PROGRESS_KEY, self.max_seq)

        now = ctx.timer_service().current_processing_time()
        if self.quiet_at is None:
            self.quiet_at = now + QUIET_MS
            ctx.timer_service().register_processing_time_timer(self.quiet_at)
        if self.offline and self.maint_at is None:
            self.maint_at = now + 1000
            ctx.timer_service().register_processing_time_timer(self.maint_at)

    def on_timer(self, timestamp, ctx):
        if self.quiet_at is not None and timestamp >= self.quiet_at:
            self.quiet_at = None
            if self.online:
                self._flush_online()
            elif self.max_seq >= 0:
                self.redis.set(PROGRESS_KEY, self.max_seq)
        if self.maint_at is not None and timestamp >= self.maint_at:
            self.maint_at = None
            if self.offline and time.monotonic() - self.offline_since > OFFLINE_MAX_AGE_S:
                self._flush_offline()
            if self.offline:
                self.maint_at = ctx.timer_service().current_processing_time() + 1000
                ctx.timer_service().register_processing_time_timer(self.maint_at)
        return iter(())

    def close(self) -> None:
        self._flush_online()
        self._flush_offline()
        self.http.close()


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    cfg = env.get_config()
    cfg.set_auto_watermark_interval(200)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("recsys-user-features")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder().set_topic(DLQ_TOPIC).set_value_serialization_schema(SimpleStringSchema()).build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-interactions")
    valid = raw.process(Validate(), output_type=EVENT).name("validate-contract")
    valid.get_side_output(DLQ).sink_to(dlq_sink).name("dlq-contract")

    timed = valid.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(OUT_OF_ORDERNESS_S)).with_timestamp_assigner(EventTime())
    )
    features = timed.key_by(lambda e: e[1], key_type=Types.LONG()).process(UserFeatures(), output_type=Types.STRING()).name(
        "user-stream-features"
    )
    features.get_side_output(DLQ).sink_to(dlq_sink).name("dlq-late")
    env.execute("recsys-user-stream-features")


if __name__ == "__main__":
    main()
