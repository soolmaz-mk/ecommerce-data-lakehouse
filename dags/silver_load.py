# Airflow DAG that loads bronze into the silver star, every hour.
# Order: stage -> dimensions -> facts -> quality checks -> publish.
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import ExternalPythonOperator
except ImportError:
    from airflow.operators.python import ExternalPythonOperator

from jobs.silver import tasks

VENV = "/opt/venvs/silver/bin/python"
POOL = "silver_pool"

RUN_ID = "{{ run_id }}"
NODASH = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S') }}"
ISTART = "{{ data_interval_start.isoformat() }}"
IEND = "{{ data_interval_end.isoformat() }}"

default_args = {
    "owner": "silver",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


def _op(dag, task_id, callable_, op_kwargs=None, trigger_rule="all_success"):
    return ExternalPythonOperator(
        task_id=task_id,
        python=VENV,
        python_callable=callable_,
        op_kwargs=op_kwargs or {},
        expect_airflow=False,
        pool=POOL,
        trigger_rule=trigger_rule,
        dag=dag,
    )


with DAG(
    dag_id="silver_load",
    description="Bronze -> Silver star (Polars + Iceberg WAP)",
    default_args=default_args,
    start_date=datetime(2026, 7, 16),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "iceberg", "polars", "kimball"],
) as dag:

    ensure_tables = _op(dag, "ensure_tables", tasks.ensure_tables)
    seed_dim_date = _op(dag, "seed_dim_date", tasks.seed_dim_date)
    open_branches = _op(dag, "open_audit_branches", tasks.open_audit_branches,
                        {"run_id": RUN_ID})

    stage_users = _op(dag, "stage_users", tasks.stage_users, {"interval_nodash": NODASH})
    stage_products = _op(dag, "stage_products", tasks.stage_products, {"interval_nodash": NODASH})
    stage_categories = _op(dag, "stage_categories", tasks.stage_categories, {"interval_nodash": NODASH})
    stage_price_history = _op(dag, "stage_price_history", tasks.stage_price_history, {"interval_nodash": NODASH})
    win = {"interval_nodash": NODASH, "interval_start": ISTART, "interval_end": IEND}
    stage_orders = _op(dag, "stage_orders", tasks.stage_orders, win)
    stage_order_items = _op(dag, "stage_order_items", tasks.stage_order_items,
                            {"interval_nodash": NODASH})
    stage_events = _op(dag, "stage_events", tasks.stage_events, win)
    stage_returns = _op(dag, "stage_returns", tasks.stage_returns, {"interval_nodash": NODASH})
    stages = [stage_users, stage_products, stage_categories, stage_price_history,
              stage_orders, stage_order_items, stage_events, stage_returns]

    dk = {"run_id": RUN_ID, "interval_nodash": NODASH}
    load_dim_user = _op(dag, "load_dim_user", tasks.load_dim_user, dk)
    load_dim_product = _op(dag, "load_dim_product", tasks.load_dim_product, dk)
    load_dim_category = _op(dag, "load_dim_category", tasks.load_dim_category, dk)
    load_dim_payment = _op(dag, "load_dim_payment_method", tasks.load_dim_payment_method, dk)
    load_dim_status = _op(dag, "load_dim_order_status", tasks.load_dim_order_status, dk)
    load_dim_event_type = _op(dag, "load_dim_event_type", tasks.load_dim_event_type, dk)
    load_dim_device = _op(dag, "load_dim_device", tasks.load_dim_device, dk)
    load_dim_return_reason = _op(dag, "load_dim_return_reason", tasks.load_dim_return_reason, dk)
    dims = [load_dim_user, load_dim_product, load_dim_category, load_dim_payment,
            load_dim_status, load_dim_event_type, load_dim_device,
            load_dim_return_reason]

    load_fact_orders = _op(dag, "load_fact_orders", tasks.load_fact_orders, dk)
    load_fact_sales = _op(dag, "load_fact_sales", tasks.load_fact_sales, dk)
    load_fact_clickstream = _op(dag, "load_fact_clickstream", tasks.load_fact_clickstream, dk)
    load_fact_returns = _op(dag, "load_fact_returns", tasks.load_fact_returns, dk)
    facts = [load_fact_orders, load_fact_sales, load_fact_clickstream, load_fact_returns]

    run_dq = _op(dag, "run_dq", tasks.run_dq, {"run_id": RUN_ID})
    publish = _op(dag, "publish", tasks.publish, {"run_id": RUN_ID})
    drop_branches = _op(dag, "drop_audit_branches", tasks.drop_audit_branches,
                        {"run_id": RUN_ID}, trigger_rule="one_failed")

    ensure_tables >> seed_dim_date >> open_branches >> stages

    stage_users >> load_dim_user
    [stage_products, stage_price_history] >> load_dim_product
    stage_categories >> load_dim_category
    open_branches >> [load_dim_payment, load_dim_status, load_dim_event_type,
                      load_dim_device, load_dim_return_reason]

    for d in dims:
        d >> facts
    [stage_orders, stage_order_items] >> load_fact_orders
    [stage_orders, stage_order_items] >> load_fact_sales
    stage_events >> load_fact_clickstream
    stage_returns >> load_fact_returns

    facts >> run_dq >> publish

    for t in [*stages, *dims, *facts, open_branches, run_dq, publish]:
        t >> drop_branches
