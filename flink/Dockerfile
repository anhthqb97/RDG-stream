FROM flink:1.19-scala_2.12-java11

USER root

RUN ARCH="$(dpkg --print-architecture)" \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev curl openjdk-11-jdk-headless build-essential \
    && ln -sf "/usr/lib/jvm/java-11-openjdk-${ARCH}" /usr/lib/jvm/java-11-openjdk \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk

COPY jobs/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.19
ARG FLINK_VERSION=1.19.3

RUN curl -fsSL \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      -o "/opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && curl -fsSL \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      -o "/opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && curl -fsSL \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar" \
      -o "/opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.jar"

USER flink
