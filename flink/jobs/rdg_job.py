"""RDG Stream PyFlink job — metrics.raw → validate → window → Cassandra."""

from pyflink.datastream import StreamExecutionEnvironment

JOB_NAME = "rdg-stream-job"


def create_execution_environment() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    return env


def main() -> None:
    create_execution_environment()
    # Pipeline wired in TASK-028+
    # env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
