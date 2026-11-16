# Spark configuration

This directory holds everything Spark needs to talk to MinIO over the S3A connector, plus the base image for the `jobs/bronze_ingestion` streaming job.

## How it works

The cluster is a plain Spark **standalone** cluster, not YARN/Kubernetes:
`spark-master` is the cluster manager, `spark-worker` is the (single) node
that actually runs tasks, and `bronze-ingestion` is a client that submits a
job to that cluster over `spark://spark-master:7077` and then acts as the
job's driver for as long as it runs. All three are the same Docker image
(this directory's `Dockerfile`) — they only differ in the command
`docker-compose.yaml` gives each container.

`bronze-ingestion` doesn't submit one job and exit. `jobs/bronze_ingestion/main.py`
starts one Spark Structured Streaming **query per Kafka topic**, all inside
the same driver process, and then blocks on
`spark.streams.awaitAnyTermination()` — so the container itself *is* the
long-running pipeline, not something that launches a job and returns. Each
query independently, on its own trigger (every `TRIGGER_INTERVAL`, 30s by
default):

1. Asks Kafka what's new since its last checkpoint.
2. Pulls that micro-batch, decodes each record's Avro payload (see
   `jobs/bronze_ingestion/README.md` for how), and writes it to MinIO as
   Parquet.
3. Writes a checkpoint recording how far it got, to
   `s3a://<bucket>/<checkpoint_path>/<topic>/` in MinIO.

That checkpoint is what makes the whole thing resumable rather than
stateless: if the container restarts for any reason — a redeploy, the
external Kafka cluster going down and `docker compose`'s
`restart: unless-stopped` cycling the container while it's unreachable,
whatever — each query resumes from its own last committed checkpoint
instead of re-reading from `KAFKA_STARTING_OFFSETS` again. The very first
run for a topic is the only time that setting matters.

Because every topic's query lives in the same driver/JVM and they're all
competing for the same worker's cores, `main.py` also puts each one in its
own Spark **fair-scheduler pool** (see "Concurrent topics and the FAIR
scheduler" in `jobs/bronze_ingestion/README.md`) — without that, a
high-volume topic's query can starve a low-volume one of any scheduling
time at all, since Spark's default is FIFO, not round-robin, across
concurrently running jobs in one SparkContext.

Failure is handled at two levels. Per-topic: `main.py` wraps each query's
startup in its own `try`/`except`, so one topic failing to decode or
reach the Schema Registry doesn't take the others down with it. Whole-job:
if *every* topic fails to start (for example, the whole external Kafka
cluster being unreachable), `main.py` raises and the process exits —
`docker-compose.yaml`'s `restart: unless-stopped` is what actually
retries it from there, not anything inside the Python code.

## Files

- `Dockerfile` — builds a custom Spark image on top of `apache/spark:3.5.3` with the S3A/Kafka/Avro jars and `jobs/bronze_ingestion`'s Python dependencies baked in (see [Jars](#jars) below). `spark-master`, `spark-worker`, and `bronze-ingestion` in the root `docker-compose.yaml` all build from this instead of using the base image directly — they only differ in the command each runs. Built with the **repo root** as context (not this directory), so it can `COPY jobs /opt/spark-apps/jobs`.
- `conf/spark-defaults.conf` — mounted read-only into `spark-master`, `spark-worker`, and `bronze-ingestion` at `/opt/spark/conf/spark-defaults.conf`. Applies to every job submitted to the cluster, so individual `spark-submit` calls don't need to repeat these settings.

## `spark-defaults.conf` explained

```
spark.hadoop.fs.s3a.endpoint            http://minio:9000
spark.hadoop.fs.s3a.path.style.access   true
spark.hadoop.fs.s3a.impl                org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.aws.credentials.provider com.amazonaws.auth.EnvironmentVariableCredentialsProvider
```
Points Spark's S3A client at MinIO instead of real AWS S3:
- `endpoint` — MinIO's S3 API (not the console port, `9001`). `minio` resolves via the shared `datalake_network` Docker network.
- `path.style.access=true` — MinIO doesn't support AWS's virtual-hosted-style bucket addressing (`bucket.minio:9000`), so force `minio:9000/bucket` instead.
- `ssl.enabled=false` — this stack doesn't terminate TLS in front of MinIO.
- `EnvironmentVariableCredentialsProvider` — reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from the `spark-master`/`spark-worker` container environment instead of a static key in this file. The root `docker-compose.yaml` sets those from `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` in `.env`, so credentials stay out of tracked files. Note this class lives in the AWS SDK (`com.amazonaws.auth`), not Hadoop's own `org.apache.hadoop.fs.s3a.auth` package — there's no Hadoop-native equivalent for plain env-var credentials.

```
spark.hadoop.fs.s3a.connection.timeout          5000
spark.hadoop.fs.s3a.connection.establish.timeout 5000
spark.hadoop.fs.s3a.attempts.maximum            3
spark.hadoop.fs.s3a.retry.limit                 3
spark.hadoop.fs.s3a.change.detection.mode       none
```
Without these, multi-partition writes to MinIO can deadlock indefinitely instead of failing — Hadoop's S3A client normally uses S3's per-object versioning to detect concurrent modification, but MinIO here isn't running with object versioning enabled, and a couple of executors hung forever waiting on a check that could never resolve. `change.detection.mode=none` disables that check, and the timeout/retry settings make sure a genuine connectivity problem surfaces as an error within seconds instead of hanging the job.

## Jars

Spark's S3A support needs two jars that aren't bundled with `apache/spark:3.5.3`:
- `hadoop-aws-3.3.4.jar` (matches the Hadoop 3.3.4 client baked into this Spark image — check with `docker run --rm apache/spark:3.5.3 ls /opt/spark/jars | grep hadoop-client` if you ever bump the Spark version)
- `aws-java-sdk-bundle-1.12.262.jar` (the AWS SDK version `hadoop-aws:3.3.4` was built against)

`jobs/bronze_ingestion` (Kafka + Avro decoding) needs five more, matched against `spark-sql-kafka-0-10`'s and `spark-avro`'s own compile-scope dependencies for Spark 3.5.3 (checked against their POMs on Maven Central — everything else those two connectors need, like `spark-sql` itself or the Avro core jars, is already bundled in the base image):
- `spark-sql-kafka-0-10_2.12-3.5.3.jar`, `spark-token-provider-kafka-0-10_2.12-3.5.3.jar`, `kafka-clients-3.4.1.jar`, `commons-pool2-2.11.1.jar` — the Kafka connector
- `spark-avro_2.12-3.5.3.jar`, `xz-1.9.jar` — the Avro connector

All seven are downloaded directly into `/opt/spark/jars/` at build time, which is already on Spark's default classpath — no `extraClassPath` config, `spark-submit --packages`, or separate init container needed. Baking them in also means jobs don't need internet access to Maven Central or a writable Ivy cache (`$HOME` for the image's `spark` user doesn't exist by default, which breaks Ivy's/pip's cache — build steps run as root specifically to work around this, then switch back to the `spark` user at the end). Rebuild the images (`docker compose build spark-master spark-worker bronze-ingestion`, or just `docker compose up --build`) whenever you change the jar list, versions, or `jobs/` itself.

## Python dependencies

`jobs/bronze_ingestion/config.py` needs `pydantic-settings`, installed via `pip3` at build time — pinned to `2.8.1`, not the latest, because `pydantic-settings` 2.9+ dropped support for Python 3.8, which is what this base image ships.

## Adding more S3A / Spark settings

Any `spark.*` key added to `spark-defaults.conf` takes effect on the next `docker compose restart spark-master spark-worker bronze-ingestion` — no image rebuild needed, since it's a read-only bind mount.
