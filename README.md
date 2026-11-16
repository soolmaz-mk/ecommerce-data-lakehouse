# FinalProject — Data Lake Stack

Docker Compose stack for a local data lake: S3-compatible object storage (MinIO) and a Spark cluster (master + worker) for processing it, wired together via Spark's S3A connector, plus a bronze-layer Structured Streaming job that ingests the project's Kafka topics into MinIO as Parquet.

## Services

| Service            | Image                          | Ports                         | Notes                                                                                                                      |
| ------------------ | ------------------------------ | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `minio`            | `minio/minio`                  | 9000 (S3 API), 9001 (console) | Data persisted in the `minio_data` named volume                                                                            |
| `minio-init`       | `minio/mc`                     | —                             | One-off: creates the `bronze` and `warehouse` buckets. Pinned to a specific `RELEASE.*` tag, not `:latest`                               |
| `spark-master`     | built from `spark/Dockerfile`  | 7077 (cluster), 8080 (UI)     | Custom image bakes in the S3A/Kafka/Avro jars — see [`spark/README.md`](spark/README.md)                                   |
| `spark-worker`     | built from `spark/Dockerfile`  | 8081 (UI)                     | Connects to `spark://spark-master:7077`                                                                                    |
| `bronze-ingestion` | built from `spark/Dockerfile`  | —                             | Runs `jobs/bronze_ingestion` via `spark-submit` — see [`jobs/bronze_ingestion/README.md`](jobs/bronze_ingestion/README.md) |
| `clickhouse`       | `clickhouse/clickhouse-server` | 8123 (HTTP), 9004 (Native)    | OLAP database for analytics. Native port remapped to 9004 on the host — minio already uses 9000. Persisted in `clickhouse_data` and `clickhouse_logs` volumes |
| `metabase`         | `metabase/metabase`            | 3000 (configurable via `MB_PORT`) | BI dashboard. Data persisted in the `metabase_data` named volume. The ClickHouse driver is bundled in core since Metabase v54, so no plugin jar is needed |
| `metabase-init`    | `python:3.12-slim`             | —                                 | Profile-gated (`provision`): builds the ClickHouse connection, 17 questions and 3 dashboards from `metabase/questions/*.sql`. The 7 monitoring questions are excluded by default — see [`metabase/README.md`](metabase/README.md). Idempotent |
| `iceberg-rest`     | built from `iceberg-rest/Dockerfile` | 8181 (REST catalog)     | Iceberg REST catalog for the silver layer. Backed by `postgres-iceberg` — see [Iceberg catalog](#iceberg-catalog)      |
| `postgres-iceberg` | `postgres:16-alpine`           | —                             | The Iceberg catalog's metadata store. Dedicated instance, separate from `postgres-airflow`. Persisted in `iceberg_postgres_data` |

All services share the `datalake_network` bridge network. `spark-master`, `spark-worker`, and `bronze-ingestion` all build from the same image (`spark/Dockerfile`, built with the repo root as context so it can bake in `jobs/`) — they only differ in the command each runs.

## Requirements

- Docker
- Docker Compose

## Setup

1. Create your `.env` file and set real credentials (the stack refuses to start if `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` aren't set):
   ```
   cp .env.example .env
   ```
2. Start the stack:
   ```
   docker compose up -d --build
   ```
3. Check everything is healthy:
   ```
   docker compose ps
   ```

## Access

- MinIO console: http://localhost:9001
- MinIO S3 API: http://localhost:9000
- Spark master UI: http://localhost:8080
- ClickHouse HTTP interface: http://localhost:8123
- ClickHouse native protocol (e.g. for `clickhouse-client`): localhost:9004
- Metabase: http://localhost:3000 (or `$MB_PORT` if overridden)
- Iceberg REST catalog: http://localhost:8181/v1/config

## Iceberg catalog

The silver layer's Iceberg tables are cataloged by `iceberg-rest`
(`apache/iceberg-rest-fixture`), backed by a dedicated PostgreSQL instance.
Two things about this deployment are deliberate:

- **The image's default catalog store is unusable here.** Upstream's Dockerfile
  sets `CATALOG_URI=jdbc:sqlite:/tmp/iceberg_catalog.db` — SQLite, inside the
  container, under `/tmp`. Left at that default, every table definition is lost
  the moment the container is recreated, while the data files stay behind in
  MinIO, orphaned: a silent and total metadata loss. `CATALOG_URI` therefore
  points at `postgres-iceberg` instead.
- **The catalog gets its own postgres**, not `postgres-airflow`. The catalog is
  the system of record for every silver table; coupling its lifecycle to the
  orchestrator's metadata DB means an Airflow reset can take every table
  definition with it.

`iceberg-rest` is **built, not pulled** (`iceberg-rest/Dockerfile`). The upstream
image bundles only the SQLite JDBC driver — shipping a PostgreSQL driver is still
an open upstream request ([apache/iceberg#14802](https://github.com/apache/iceberg/issues/14802)) —
so `CATALOG_URI=jdbc:postgresql://…` fails at startup with `No suitable driver
found`. The image adds the driver jar (verified by SHA-1, same bake-at-build-time
approach as `spark/Dockerfile`) and overrides `CMD` to put it on the classpath,
because upstream's `entrypoint.sh` hardcodes `CLASSPATH="iceberg-rest-adapter.jar"`
and would otherwise never load it. It is pinned to `1.10.1` rather than the
untagged `latest` the stack used before, per the pinning rule in
[Operational notes](#operational-notes).

Note that the image sets `CATALOG_JDBC_STRICT__MODE=true`, so namespaces must be
created explicitly before tables can be created in them.

## Silver layer (Kimball star)

The silver layer turns the raw bronze Parquet into a conformed Kimball star
(`silver` namespace) of **3 facts** (`fact_sales`, `fact_orders`,
`fact_clickstream`) and **8 dimensions** (`dim_date`, `dim_user`, `dim_product`,
`dim_category`, `dim_payment_method`, `dim_order_status`, `dim_event_type`,
`dim_device`). **See [`jobs/silver/README.md`](jobs/silver/README.md) for how the
layer works, with diagrams.** Code: [`jobs/silver/`](jobs/silver/); design rationale (data
profiling, grain decisions, money arithmetic, SCD strategy) in the working
notes. `fact_returns`/`dim_return_reason` are intentionally out of scope — the
`returns_refunds` topic is empty and has no registered schema.

**Engine: Polars + PyIceberg, no Spark.** Silver leaves Spark entirely
(`polars`/`pyiceberg` need Python ≥3.10, the Spark image ships 3.8). Each Airflow
task runs in a dedicated, isolated venv at `/opt/venvs/silver` (built by the
top-level [`Dockerfile`](Dockerfile) from
[`jobs/silver/requirements.txt`](jobs/silver/requirements.txt)) via
`ExternalPythonOperator` — no JVM, no `spark-submit`, no Docker socket. The
venv's dependency tree is kept fully separate from Airflow's own constraints.

Key design points, all in code under `jobs/silver/`:

- **Dimensions are full rebuilds; facts are incremental** over a look-back
  window (`SILVER_LOOKBACK_DAYS`, default 3). Bronze partitions some sources by a
  *business* date (`users` by `signup_date`, `product_price_history` by
  `valid_from`), so "today's partition" ≠ "what arrived today" — rebuilding the
  tiny dims each run sidesteps that trap entirely. Surrogate keys are stable
  hashes so a rebuild never re-numbers keys and orphans fact FKs.
- **Point-in-time SCD2 joins** use Polars `join_asof` (`transforms/facts.py`):
  each order line/event is attached to the dimension version valid at
  transaction time, not the current one.
- **`dim_product`'s price timeline is rebuilt** from `product_price_history` via
  `LEAD` — the source's `valid_to`/`is_current` are unusable (every row claims
  `is_current=true`). Products with no price history are seeded from
  `products.price` at epoch so they never drop out of the point-in-time join.
- **Every dimension carries `-1` (Unknown/N/A) and `-2` (Late-arriving)
  members** — facts never store a NULL FK.
- **Write-Audit-Publish** (`wap.py`): each run writes to a per-run Iceberg audit
  branch, the DQ gate (`transforms/dq.py`: unique grain, no NULL FKs, measure
  sanity, order-total reconciliation, SCD2 invariants) runs against the branch,
  and only if green is `main` fast-forwarded onto it
  (`set_current_snapshot(ref_name=…)`). Readers never see a half-loaded star.

DAGs: [`dags/silver_load.py`](dags/silver_load.py) (`@hourly`,
`max_active_runs=1`, tasks pinned to the `silver_pool` so Polars can't OOM the
scheduler) and [`dags/silver_maintenance.py`](dags/silver_maintenance.py)
(`@weekly` compaction + snapshot expiry).

## Gold layer (One Big Table on ClickHouse)

dbt flattens the silver star into four wide OBT tables in ClickHouse —
`obt_sales`, `obt_orders`, `obt_clickstream`, `obt_returns` — so analytical
queries need no joins. Project: [`dbt_project/`](dbt_project/); orchestration:
[`dags/gold_dbt_dag.py`](dags/gold_dbt_dag.py) (`@daily`, gated behind an
`ExternalTaskSensor` on `silver_load`'s `publish` task so it never reads a
half-published star).

ClickHouse reads the silver tables through its **Iceberg table engine** — the
`on-run-start` hooks in `dbt_project.yml` register one external table per silver
table against `s3://warehouse/silver/...`. The dbt models are deliberately
written as flat `SELECT`s with no CTEs: ClickHouse's SQL analyzer mis-resolves
aliases in CTEs over external Iceberg tables. That constraint applies to the
Iceberg-backed models only — queries against the materialised OBT tables use
CTEs freely.

## Presentation layer (Metabase)

Five business dashboards plus a real-time pipeline monitor, **provisioned from
code** rather than clicked together — 24 SQL files and a stdlib-only script.
See [`metabase/README.md`](metabase/README.md) for deployment, the dashboard
inventory, and the quantified list of upstream data gaps that limit two of the
required reports.

```bash
docker compose exec clickhouse bash \
  /docker-entrypoint-initdb.d/10-realtime-monitoring.sh   # once, for the bonus layer
docker compose --profile provision run --rm metabase-init # dashboards
```

The **bonus real-time monitor** does not read the lakehouse at all. ClickHouse
Kafka-engine tables consume `transactional.orders` and `behavioral.events`
directly (Confluent Avro, via the schema registry) into rolling 48-hour
MergeTree tables — see
[`clickhouse/init/10-realtime-monitoring.sh`](clickhouse/init/10-realtime-monitoring.sh).
Going through bronze → silver → gold would mean reporting the pipeline's health
using the pipeline, which reports nothing at the moment the pipeline stops. It
uses its own Kafka consumer group, so it never competes with bronze ingestion.

Its headline metric is **data freshness, not container liveness** — the one
signal that would have caught this project's 14-hour silent ingestion outage,
where the container stayed `Up` with green healthchecks while nothing moved.

## Configuration

Environment variables (see `.env.example`):

- `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` — admin credentials for the ClickHouse server. Required — compose fails fast at startup if either is unset.
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` — MinIO admin credentials, passed to Spark's S3A client as `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars in `docker-compose.yaml`. Required — compose fails fast at startup if either is unset, rather than silently running with blank credentials.
- `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` — resources given to the Spark worker's JVM (required)
- `SPARK_WORKER_CPU_LIMIT`, `SPARK_WORKER_MEMORY_LIMIT` — optional hard Docker limits for the `spark-worker` container, default `3` / `3G`. Keep these above the JVM settings above so the container isn't OOM-killed under load.
- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPICS`, `SCHEMA_REGISTRY_URL` — required by the `bronze-ingestion` service; see [`jobs/bronze_ingestion/README.md`](jobs/bronze_ingestion/README.md) for the full variable list and what they mean.

Custom Spark config lives in `spark/conf/spark-defaults.conf`, including the S3A settings that let Spark read/write `s3a://bronze/...` paths in MinIO.

The `hadoop-aws`/`aws-java-sdk-bundle` (S3A) and `spark-sql-kafka-0-10`/`spark-avro` (bronze ingestion) jars aren't committed to the repo — `spark/Dockerfile` downloads them into `/opt/spark/jars/` at image build time, so `spark-master`/`spark-worker`/`bronze-ingestion` need to be built (`docker compose up --build`) rather than just pulled. The bronze ingestion job's own Python code (`jobs/`) is baked into the same image, so a code change also needs a rebuild.

See [`spark/README.md`](spark/README.md) for a line-by-line breakdown of the Spark/S3A config and jar setup, and [`jobs/bronze_ingestion/README.md`](jobs/bronze_ingestion/README.md) for the ingestion job itself.

## Operational notes

- Images are pinned to explicit versions (no `:latest`), and every service carries `deploy.resources` CPU/memory limits — see `docker-compose.yaml`.
- Required secrets (`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPICS`, `SCHEMA_REGISTRY_URL`) use `${VAR:?...}` so `docker compose up` fails immediately with a clear error if `.env` is missing or incomplete, instead of running with blank/default values.
- MinIO's data directory is a Docker-managed named volume (`minio_data`), not a host bind mount — besides being the more portable default, a bind mount hits a `flock()` incompatibility with MinIO's erasure-coding scanner under Docker Desktop's virtiofs/gRPC-FUSE on macOS ("resource deadlock avoided" errors unrelated to any actual disk contention).
- Not in scope here: TLS termination, MinIO/Spark HA, and public network exposure — this hardens the compose stack itself for a single-host deployment. Put a reverse proxy in front of ports 9000/9001/8080 if this is reachable from outside a trusted network.
