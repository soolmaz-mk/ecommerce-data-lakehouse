from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Define the path to your dbt project inside the Airflow container
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

with DAG(
    dag_id='gold_dbt_load',
    default_args=default_args,
    schedule='15 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'dbt', 'clickhouse'],
) as dag:

    # 1. Sensor to wait for the Silver layer DAG to finish successfully.
    # silver_load is @hourly (logical date :00) while this DAG fires at :15, so
    # the sensor has to look 15 minutes back -- without execution_delta it pokes
    # for a silver_load run at :15 that never exists and times out.
    wait_for_silver_layer = ExternalTaskSensor(
        task_id='wait_for_silver_layer',
        external_dag_id='silver_load',
        external_task_id='publish',
        execution_delta=timedelta(minutes=15),
        mode='reschedule',
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    # 2. Empty operators as visual branching/convergence points (Best Practice for Graph readability)
    start_dbt_models = EmptyOperator(task_id='start_dbt_models')
    end_dbt_models = EmptyOperator(task_id='end_dbt_models')

    # 3. Individual tasks for each OBT model (Breaking the Black Box for better observability)
    run_obt_sales = BashOperator(
        task_id='run_obt_sales',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select obt_sales"
    )

    run_obt_orders = BashOperator(
        task_id='run_obt_orders',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select obt_orders"
    )

    run_obt_returns = BashOperator(
        task_id='run_obt_returns',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select obt_returns"
    )

    run_obt_clickstream = BashOperator(
        task_id='run_obt_clickstream',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select obt_clickstream"
    )

    # 4. Overall data quality test for the entire Gold layer
    test_dbt_models = BashOperator(
        task_id='test_dbt_models',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test"
    )

    # 5. Define the pipeline execution flow (Sequential to prevent Memory Crash)
    wait_for_silver_layer >> start_dbt_models
    
    # Run tasks sequentially (one by one) to save RAM and CPU resources
    start_dbt_models >> run_obt_sales >> run_obt_orders >> run_obt_returns >> run_obt_clickstream >> test_dbt_models
    
    # End the DAG gracefully
    test_dbt_models >> end_dbt_models
