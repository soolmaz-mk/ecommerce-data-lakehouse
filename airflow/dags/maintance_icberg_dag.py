from airflow.sdk import dag, task, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum


TEHRAN_TZ = pendulum.timezone("Asia/Tehran")

dag_config = {
	dag_id: "iceberg_maintenance",
    schedule: "0 2 * * *",
    start_date: pendulum.datetime(2026, 1, 1, tz=TEHRAN_TZ ),
    catchup: False}



@dag(**dag_config)
def maintenance():
    @task
    def get_tables_name() -> list:
        return Variable.get(
            "silver_tables",
            deserialize_json=True
        )
        
    tables_name = get_tables_name()

    run_maintenance_job = SparkSubmitOperator.partial(

        task_id="run_maintenance_job",

        application=
        "/opt/spark/jobs/maintenance_iceberg.py",

        conn_id="spark_default",

        trigger_rule="none_failed"

    ).expand(

        application_args=
        tables_name.map(
            lambda table:
            [
                "--table",
                table
            ]
        )

    )


    tables_name >> run_maintenance_job



maintenance()