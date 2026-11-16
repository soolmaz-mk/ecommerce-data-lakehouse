FROM apache/spark:3.5.3

# The base image runs as the unprivileged `spark` user, whose $HOME
# (/home/spark) doesn't actually exist -- fine for running Spark, but pip
# and Ivy both try to write a cache under $HOME and fail. Build as root;
# the final USER spark below restores the original runtime posture.
USER root

# S3A connector: talks to MinIO over the S3 API (see spark/conf/spark-defaults.conf).
RUN curl -fSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    curl -fSL -o /opt/spark/jars/iceberg-aws-bundle-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar

# Kafka + Avro connectors for jobs/bronze_ingestion, baked in rather than
# resolved via `spark-submit --packages` at runtime: that needs a writable
# Ivy cache (same $HOME problem as above) and internet access to Maven
# Central every time a job starts. Versions matched to spark-sql-kafka-0-10
# and spark-avro's own compile-scope dependencies for Spark 3.5.3 (checked
# against their POMs) -- everything else those connectors need (spark-sql,
# spark-tags, jsr305, the Avro core jars) is already bundled in this base
# image. Bump these if the Spark version above changes.
RUN curl -fSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fSL -o /opt/spark/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fSL -o /opt/spark/jars/spark-avro_2.12-3.5.3.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.3/spark-avro_2.12-3.5.3.jar && \
    curl -fSL -o /opt/spark/jars/xz-1.9.jar \
      https://repo1.maven.org/maven2/org/tukaani/xz/1.9/xz-1.9.jar

# Python dependencies for jobs/bronze_ingestion/config.py. Pinned to 2.8.1,
# not the latest: pydantic-settings 2.9+ dropped support for Python 3.8,
# which is what this base image ships.
RUN pip3 install --no-cache-dir pydantic-settings==2.8.1

# Bronze ingestion job code. Baked in at build time (immutable image, no
# code-mounting at deploy time) -- rebuild (`docker compose build`) after
# changing anything under jobs/.
COPY jobs /opt/spark-apps/jobs

USER spark
