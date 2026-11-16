import logging

logger = logging.getLogger(__name__)


def rewrite_manifests(spark, table_name):

    logger.info(
        f"Rewrite manifests started: {table_name}"
    )


    spark.sql(
        f"""
        CALL rest.system.rewrite_manifests(
            table => '{table_name}'
        )
        """
    )


    logger.info(
        f"Rewrite manifests finished: {table_name}"
    )
