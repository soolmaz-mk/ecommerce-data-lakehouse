import logging

logger = logging.getLogger(__name__)


def expire_snapshots(spark, table_name):

    logger.info(
        f"Expire snapshots started: {table_name}"
    )


    spark.sql(
        f"""
        CALL rest.system.expire_snapshots(
            table => '{table_name}',
            older_than =>
            TIMESTAMP '2026-07-01 00:00:00'
        )
        """
    )


    logger.info(
        f"Expire snapshots finished: {table_name}"
    )
