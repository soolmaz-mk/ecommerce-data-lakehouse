# Entry points that Airflow calls. Each one only imports and calls runner.
# Keep them as plain functions - see PART 12.2 in hosseincpl.md for why.
from __future__ import annotations


def ensure_tables() -> dict:
    from jobs.silver.runner import ensure_tables as _fn
    return _fn()


def seed_dim_date() -> dict:
    from jobs.silver.runner import seed_dim_date as _fn
    return _fn()


def open_audit_branches(run_id: str) -> dict:
    from jobs.silver.runner import open_audit_branches as _fn
    return _fn(run_id)


def publish(run_id: str) -> dict:
    from jobs.silver.runner import publish as _fn
    return _fn(run_id)


def drop_audit_branches(run_id: str) -> dict:
    from jobs.silver.runner import drop_audit_branches as _fn
    return _fn(run_id)


def stage_users(interval_nodash: str) -> dict:
    from jobs.silver.runner import stage_users as _fn
    return _fn(interval_nodash)


def stage_products(interval_nodash: str) -> dict:
    from jobs.silver.runner import stage_products as _fn
    return _fn(interval_nodash)


def stage_categories(interval_nodash: str) -> dict:
    from jobs.silver.runner import stage_categories as _fn
    return _fn(interval_nodash)


def stage_price_history(interval_nodash: str) -> dict:
    from jobs.silver.runner import stage_price_history as _fn
    return _fn(interval_nodash)


def stage_returns(interval_nodash: str) -> dict:
    from jobs.silver.runner import stage_returns as _fn
    return _fn(interval_nodash)


def stage_orders(interval_nodash: str, interval_start: str, interval_end: str) -> dict:
    from jobs.silver.runner import stage_orders as _fn
    return _fn(interval_nodash, interval_start, interval_end)


def stage_order_items(interval_nodash: str) -> dict:
    from jobs.silver.runner import stage_order_items as _fn
    return _fn(interval_nodash)


def stage_events(interval_nodash: str, interval_start: str, interval_end: str) -> dict:
    from jobs.silver.runner import stage_events as _fn
    return _fn(interval_nodash, interval_start, interval_end)


def load_dim_user(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.runner import load_dim_user as _fn
    return _fn(run_id, interval_nodash)


def load_dim_product(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.runner import load_dim_product as _fn
    return _fn(run_id, interval_nodash)


def load_dim_category(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.runner import load_dim_category as _fn
    return _fn(run_id, interval_nodash)


def load_dim_payment_method(run_id: str, interval_nodash: str = "") -> dict:
    from jobs.silver.runner import load_dim_payment_method as _fn
    return _fn(run_id, interval_nodash)


def load_dim_order_status(run_id: str, interval_nodash: str = "") -> dict:
    from jobs.silver.runner import load_dim_order_status as _fn
    return _fn(run_id, interval_nodash)


def load_dim_event_type(run_id: str, interval_nodash: str = "") -> dict:
    from jobs.silver.runner import load_dim_event_type as _fn
    return _fn(run_id, interval_nodash)


def load_dim_device(run_id: str, interval_nodash: str = "") -> dict:
    from jobs.silver.runner import load_dim_device as _fn
    return _fn(run_id, interval_nodash)


def load_dim_return_reason(run_id: str, interval_nodash: str = "") -> dict:
    from jobs.silver.runner import load_dim_return_reason as _fn
    return _fn(run_id, interval_nodash)


def load_fact_orders(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.runner import load_fact_orders as _fn
    return _fn(run_id, interval_nodash)


def load_fact_sales(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.runner import load_fact_sales as _fn
    return _fn(run_id, interval_nodash)


def load_fact_clickstream(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.runner import load_fact_clickstream as _fn
    return _fn(run_id, interval_nodash)


def load_fact_returns(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.runner import load_fact_returns as _fn
    return _fn(run_id, interval_nodash)


def run_dq(run_id: str) -> dict:
    from jobs.silver.runner import run_dq as _fn
    return _fn(run_id)


def run_maintenance() -> dict:
    from jobs.silver.runner import run_maintenance as _fn
    return _fn()
