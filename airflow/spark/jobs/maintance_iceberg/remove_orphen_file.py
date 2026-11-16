import logging

logger = logging.getLogger(__name__)


def remove_orphan_files(spark, table_name):

    logger.info(
        f"Remove orphan files started: {table_name}"
    )


    spark.sql(
        f"""
        CALL rest.system.remove_orphan_files(
            table => '{table_name}'
        )
        """
    )


    logger.info(
        f"Remove orphan files finished: {table_name}"
    )
