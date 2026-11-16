import logging

logger = logging.getLogger(__name__)


def rewrite_data_files(spark, table_name):

    logger.info(
        f"Rewrite data files started: {table_name}"
    )


    spark.sql(
        f"""
        CALL rest.system.rewrite_data_files(
            table => '{table_name}'
        )
        """
    )


    logger.info(
        f"Rewrite data files finished: {table_name}"
    )
