# Spark job that compacts Iceberg files and manifests.
# PyIceberg cannot do this, so it runs on the Spark cluster instead.
import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("iceberg_maintenance")

TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024

ORPHAN_RETENTION_HOURS = 72


def _catalog_conf(catalog: str) -> dict:
    region = os.environ.get("AWS_REGION", "us-east-1")
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "rest",
        f"spark.sql.catalog.{catalog}.uri": os.environ.get(
            "ICEBERG_REST_URI", "http://iceberg-rest:8181"
        ),
        f"spark.sql.catalog.{catalog}.warehouse": "s3://%s/"
        % os.environ.get("WAREHOUSE_BUCKET", "warehouse"),
        f"spark.sql.catalog.{catalog}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{catalog}.s3.endpoint": os.environ.get(
            "MINIO_ENDPOINT", "http://minio:9000"
        ),
        f"spark.sql.catalog.{catalog}.s3.path-style-access": "true",
        f"spark.sql.catalog.{catalog}.s3.region": region,
        f"spark.sql.catalog.{catalog}.client.region": region,
        "spark.executorEnv.AWS_REGION": region,
    }


def build_session(catalog: str):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("iceberg-maintenance")
    for key, value in _catalog_conf(catalog).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _scalar(rows, *names):
    if not rows:
        return None
    row = rows[0].asDict()
    for n in names:
        if n in row:
            return row[n]
    return row


def rewrite_data_files(spark, table: str) -> dict:
    rows = spark.sql(
        f"""
        CALL {table.split('.')[0]}.system.rewrite_data_files(
            table => '{table.split('.', 1)[1]}',
            strategy => 'binpack',
            options => map('target-file-size-bytes','{TARGET_FILE_SIZE_BYTES}')
        )
        """
    ).collect()
    return {"rewrite_data_files": _scalar(rows, "rewritten_data_files_count")}


def rewrite_manifests(spark, table: str) -> dict:
    rows = spark.sql(
        f"""
        CALL {table.split('.')[0]}.system.rewrite_manifests(
            table => '{table.split('.', 1)[1]}'
        )
        """
    ).collect()
    return {"rewrite_manifests": _scalar(rows, "rewritten_manifests_count")}


def remove_orphan_files(spark, table: str) -> dict:
    rows = spark.sql(
        f"""
        CALL {table.split('.')[0]}.system.remove_orphan_files(
            table => '{table.split('.', 1)[1]}',
            older_than => TIMESTAMPADD(HOUR, -{ORPHAN_RETENTION_HOURS}, CURRENT_TIMESTAMP)
        )
        """
    ).collect()
    return {"remove_orphan_files": len(rows)}


ACTIONS = {
    "rewrite_data_files": rewrite_data_files,
    "rewrite_manifests": rewrite_manifests,
    "remove_orphan_files": remove_orphan_files,
}

DEFAULT_TABLES = [
    "dim_date", "dim_user", "dim_product", "dim_category", "dim_payment_method",
    "dim_order_status", "dim_event_type", "dim_device", "dim_return_reason",
    "fact_orders", "fact_sales", "fact_clickstream", "fact_returns",
]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Spark-side Iceberg maintenance")
    p.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", "iceberg"),
                   help="Spark catalog name to register the REST catalog under")
    p.add_argument("--namespace", default=os.environ.get("SILVER_NAMESPACE", "silver"))
    p.add_argument("--tables", default="", help="comma-separated; default = all silver tables")
    p.add_argument("--actions", default="rewrite_data_files,rewrite_manifests",
                   help=f"comma-separated subset of {list(ACTIONS)}. "
                        "remove_orphan_files is DESTRUCTIVE and opt-in.")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] or DEFAULT_TABLES
    actions = [a.strip() for a in args.actions.split(",") if a.strip()]

    unknown = [a for a in actions if a not in ACTIONS]
    if unknown:
        logger.error("unknown action(s) %s; valid: %s", unknown, list(ACTIONS))
        return 2

    spark = build_session(args.catalog)
    failures = 0
    for name in tables:
        fq = f"{args.catalog}.{args.namespace}.{name}"
        for action in actions:
            try:
                result = ACTIONS[action](spark, fq)
                logger.info("%s %s -> %s", name, action, result)
            except Exception as exc:
                failures += 1
                logger.error("%s %s FAILED: %s", name, action, exc)
    spark.stop()
    logger.info("done: %d table(s), %d action(s), %d failure(s)",
                len(tables), len(actions), failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
