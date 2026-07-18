"""RDG Stream PyFlink job — metrics.raw → validate → window → Cassandra."""

import os

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

JOB_NAME = "rdg-stream-job"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
RAW_TOPIC = "metrics.raw"
CONSUMER_GROUP = "rdg-flink"


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
    build_kafka_source(env)
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
