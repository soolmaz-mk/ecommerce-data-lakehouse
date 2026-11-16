import argparse
import logging


from spark_session import get_spark


from config_loader import (
    load_config,
    get_table_config,
    is_operation_enabled
)


from rewrite_data_files import rewrite_data_files
from rewrite_manifests import rewrite_manifests
from expire_snapshots import expire_snapshots
from remove_orphan_files import remove_orphan_files



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


logger = logging.getLogger(__name__)



def parse_args():

    parser = argparse.ArgumentParser(
        description="Iceberg Maintenance Job"
    )


    parser.add_argument(
        "--table",
        required=True,
        help="Iceberg table name"
    )


    parser.add_argument(
        "--config",
        required=False,
        default="/opt/spark/config/maintenance.yml",
        help="Maintenance config path"
    )


    return parser.parse_args()




def run_maintenance(
        spark,
        table_name,
        table_config
):


    



    if is_operation_enabled(
        table_config,
        "rewrite_data_files"
    ):

        logger.info(
            f"Running rewrite_data_files on {table_name}"
        )

        rewrite_data_files(
            spark,
            table_name,
            table_config["operations"]["rewrite_data_files"]
        )



    if is_operation_enabled(
        table_config,
        "rewrite_manifests"
    ):

        logger.info(
            f"Running rewrite_manifests on {table_name}"
        )


        rewrite_manifests(
            spark,
            table_name
        )



    if is_operation_enabled(
        table_config,
        "expire_snapshots"
    ):

        logger.info(
            f"Running expire_snapshots on {table_name}"
        )


        expire_snapshots(
            spark,
            table_name,
            table_config["operations"]["expire_snapshots"]
        )



    if is_operation_enabled(
        table_config,
        "remove_orphan_files"
    ):

        logger.info(
            f"Running remove_orphan_files on {table_name}"
        )


        remove_orphan_files(
            spark,
            table_name,
            table_config["operations"]["remove_orphan_files"]
        )





def main():

    args = parse_args()


    table_name = args.table



    logger.info(
        f"Starting Iceberg maintenance for {table_name}"
    )


    config = load_config(
        args.config
    )



    table_config = get_table_config(
        config,
        table_name
    )



    spark = get_spark()



    try:

        run_maintenance(
            spark,
            table_name,
            table_config
        )


        logger.info(
            f"Maintenance completed successfully: {table_name}"
        )


    except Exception as e:

        logger.exception(
            f"Maintenance failed for {table_name}"
        )

        raise e



    finally:

        spark.stop()



if __name__ == "__main__":

    main()
