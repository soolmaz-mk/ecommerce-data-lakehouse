import yaml
import logging


logger = logging.getLogger(__name__)


def load_config(
    path="/opt/spark/config/maintenance.yml"
):

    logger.info(
        f"Loading config file: {path}"
    )


    with open(path, "r") as file:

        config = yaml.safe_load(file)


    logger.info(
        "Configuration loaded successfully"
    )


    return config

def get_table_config(
    config,
    table_name
):

    tables = (
        config
        ["maintenance"]
        ["tables"]
    )


    for table in tables:

        if table["name"] == table_name:

            return table


    raise ValueError(
        f"Table configuration not found: {table_name}"
    )


def is_operation_enabled(
    table_config,
    operation
):

    return (
        table_config
        ["operations"]
        [operation]
        ["enabled"]
    )
