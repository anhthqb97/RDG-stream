"""RDG Stream PyFlink job — metrics.raw → validate → window → Cassandra."""

import json
import os
from datetime import datetime, timezone
from typing import Any

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingProcessingTimeWindows

JOB_NAME = "rdg-stream-job"
KAFKA_CONNECTOR_JAR = "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar"
KAFKA_CLIENTS_JAR = "file:///opt/flink/lib/kafka-clients-3.4.0.jar"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
RAW_TOPIC = "metrics.raw"
DLQ_TOPIC = "metrics.dlq"
CONSUMER_GROUP = "rdg-flink"
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = "rdg"
REQUIRED_FIELDS = ("plant_id", "equipment_id", "metric", "value", "unit", "ts")
VALID_TAG = "valid"
INVALID_TAG = "invalid"


def validate_record(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        record = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid json: {exc.msg}"

    if not isinstance(record, dict):
        return None, "payload must be a json object"

    missing = [field for field in REQUIRED_FIELDS if field not in record]
    if missing:
        return None, f"missing fields: {', '.join(missing)}"

    if not isinstance(record["value"], (int, float)):
        return None, "value must be numeric"

    if not isinstance(record["plant_id"], str) or not record["plant_id"]:
        return None, "plant_id must be a non-empty string"

    if not isinstance(record["equipment_id"], str) or not record["equipment_id"]:
        return None, "equipment_id must be a non-empty string"

    if not isinstance(record["metric"], str) or not record["metric"]:
        return None, "metric must be a non-empty string"

    if not isinstance(record["unit"], str) or not record["unit"]:
        return None, "unit must be a non-empty string"

    if not isinstance(record["ts"], str) or not record["ts"]:
        return None, "ts must be a non-empty string"

    return record, None


class SplitValidationFunction(MapFunction):
    def map(self, value: str) -> tuple[str, str]:
        record, reason = validate_record(value)
        if reason:
            return (
                INVALID_TAG,
                json.dumps(
                    {
                        "error": "validation_error",
                        "reason": reason,
                        "original": value,
                    }
                ),
            )
        return (VALID_TAG, json.dumps(record))


def apply_validation(raw_stream: DataStream) -> tuple[DataStream, DataStream]:
    tagged = raw_stream.map(
        SplitValidationFunction(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()]),
    )
    valid = tagged.filter(lambda item: item[0] == VALID_TAG).map(
        lambda item: json.loads(item[1]),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    invalid = tagged.filter(lambda item: item[0] == INVALID_TAG).map(
        lambda item: item[1],
        output_type=Types.STRING(),
    )
    return valid, invalid


def build_dlq_kafka_sink() -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def route_invalid_to_dlq(invalid_stream: DataStream) -> None:
    invalid_stream.sink_to(build_dlq_kafka_sink())


def key_by_plant_and_metric(stream: DataStream) -> DataStream:
    return stream.key_by(lambda record: (record["plant_id"], record["metric"]))


def apply_tumbling_window(keyed_stream: DataStream) -> DataStream:
    return keyed_stream.window(TumblingProcessingTimeWindows.of(Time.minutes(1)))


class MetricAggregateFunction(AggregateFunction):
    def create_accumulator(self) -> dict[str, float | int | str | None]:
        return {"sum": 0.0, "count": 0, "min": None, "max": None, "unit": None}

    def add(self, value: dict[str, Any], acc: dict[str, float | int | str | None]):
        metric_value = float(value["value"])
        acc["sum"] = float(acc["sum"]) + metric_value
        acc["count"] = int(acc["count"]) + 1
        acc["min"] = metric_value if acc["min"] is None else min(float(acc["min"]), metric_value)
        acc["max"] = metric_value if acc["max"] is None else max(float(acc["max"]), metric_value)
        acc["unit"] = value["unit"]
        return acc

    def get_result(
        self, acc: dict[str, float | int | str | None]
    ) -> dict[str, float | int | str | None]:
        return acc

    def merge(
        self,
        acc1: dict[str, float | int | str | None],
        acc2: dict[str, float | int | str | None],
    ) -> dict[str, float | int | str | None]:
        acc1["sum"] = float(acc1["sum"]) + float(acc2["sum"])
        acc1["count"] = int(acc1["count"]) + int(acc2["count"])
        if acc2["min"] is not None:
            acc1["min"] = (
                acc2["min"] if acc1["min"] is None else min(float(acc1["min"]), float(acc2["min"]))
            )
        if acc2["max"] is not None:
            acc1["max"] = (
                acc2["max"] if acc1["max"] is None else max(float(acc1["max"]), float(acc2["max"]))
            )
        if acc1["unit"] is None:
            acc1["unit"] = acc2["unit"]
        return acc1


class EnrichAggregateWindowFunction(ProcessWindowFunction):
    def process(self, key, context, aggregates):
        acc = aggregates[0]
        plant_id, metric = key
        count = int(acc["count"])
        avg = float(acc["sum"]) / count if count else 0.0
        yield {
            "plant_id": plant_id,
            "metric": metric,
            "avg": avg,
            "min": acc["min"],
            "max": acc["max"],
            "unit": acc["unit"],
            "window_end_ms": context.window().end,
        }


def compute_aggregates(windowed_stream: DataStream) -> DataStream:
    return windowed_stream.aggregate(
        MetricAggregateFunction(),
        EnrichAggregateWindowFunction(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )


class CassandraWriter(MapFunction):
    def open(self, runtime_context) -> None:
        from cassandra.cluster import Cluster

        self._cluster = Cluster([CASSANDRA_HOST])
        self._session = self._cluster.connect(CASSANDRA_KEYSPACE)

    def map(self, value: dict[str, Any]) -> dict[str, Any]:
        window_end = datetime.fromtimestamp(value["window_end_ms"] / 1000, tz=timezone.utc)
        bucket = window_end.date()
        self._session.execute(
            """
            INSERT INTO metrics_current (plant_id, metric, value, unit, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                value["plant_id"],
                value["metric"],
                value["avg"],
                value["unit"],
                window_end,
            ),
        )
        self._session.execute(
            """
            INSERT INTO metrics_ts (plant_id, metric, bucket, ts, value)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                value["plant_id"],
                value["metric"],
                bucket,
                window_end,
                value["avg"],
            ),
        )
        return value

    def close(self) -> None:
        self._session.shutdown()
        self._cluster.shutdown()


def sink_to_cassandra(aggregated_stream: DataStream) -> None:
    aggregated_stream.map(CassandraWriter(), output_type=Types.PICKLED_BYTE_ARRAY()).print()


CHECKPOINT_INTERVAL_MS = 60_000


def create_execution_environment() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.add_jars(KAFKA_CONNECTOR_JAR, KAFKA_CLIENTS_JAR)
    return env


def build_kafka_source(env: StreamExecutionEnvironment) -> DataStream:
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(RAW_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-metrics-raw")


def main() -> None:
    env = create_execution_environment()
    raw_stream = build_kafka_source(env)
    validated, invalid = apply_validation(raw_stream)
    route_invalid_to_dlq(invalid)
    keyed = key_by_plant_and_metric(validated)
    windowed = apply_tumbling_window(keyed)
    aggregated = compute_aggregates(windowed)
    sink_to_cassandra(aggregated)
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
