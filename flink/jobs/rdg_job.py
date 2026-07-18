"""RDG Stream PyFlink job — metrics.raw → validate → window → Cassandra."""

import json
import os
from typing import Any

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import DataStream, OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows

JOB_NAME = "rdg-stream-job"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
RAW_TOPIC = "metrics.raw"
DLQ_TOPIC = "metrics.dlq"
CONSUMER_GROUP = "rdg-flink"
REQUIRED_FIELDS = ("plant_id", "equipment_id", "metric", "value", "unit", "ts")
INVALID_TAG = OutputTag("invalid-records", Types.STRING())


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


class ValidateFunction(ProcessFunction):
    def process_element(self, value: str, ctx: ProcessFunction.Context):
        record, reason = validate_record(value)
        if reason:
            ctx.output(
                INVALID_TAG,
                json.dumps(
                    {
                        "error": "validation_error",
                        "reason": reason,
                        "original": value,
                    }
                ),
            )
            return
        yield record


def apply_validation(raw_stream: DataStream) -> tuple[DataStream, DataStream]:
    validated = raw_stream.process(ValidateFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
    invalid = validated.get_side_output(INVALID_TAG)
    return validated, invalid


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


def create_execution_environment() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
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
    apply_tumbling_window(keyed)
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
