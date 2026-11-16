# The real work behind every Airflow task, one function per task.
# It connects the classes: read, clean, build, write, check, publish.
from __future__ import annotations

import datetime as dt


def _window(interval_start: str, interval_end: str, lookback: int) -> tuple:
    lo = dt.datetime.fromisoformat(interval_start).date() - dt.timedelta(days=lookback)
    hi = dt.datetime.fromisoformat(interval_end).date()
    return lo, hi


def _context():
    from jobs.silver.catalog import IcebergCatalog
    from jobs.silver.config import load_config
    cfg = load_config()
    return cfg, IcebergCatalog(cfg)


def ensure_tables() -> dict:
    _, catalog = _context()
    created = catalog.ensure_tables()
    print(f"[ensure_tables] created={created or 'none (all present)'}", flush=True)
    return {"created": created}


def seed_dim_date() -> dict:
    from jobs.silver.transforms.dims import DimensionBuilder
    cfg, catalog = _context()
    table = catalog.load("dim_date")
    if table.scan(limit=1).to_arrow().num_rows > 0:
        print("[seed_dim_date] already seeded -> skip", flush=True)
        return {"seeded": 0}
    df = DimensionBuilder(cfg).date()
    catalog.append(table, df)
    print(f"[seed_dim_date] seeded {df.height} rows", flush=True)
    return {"seeded": df.height}


def open_audit_branches(run_id: str) -> dict:
    from jobs.silver.wap import WapManager
    _, catalog = _context()
    wap = WapManager(run_id)
    opened = [n for n in catalog.registry if wap.open(catalog.load(n))]
    print(f"[open_audit_branches] branch={wap.branch} opened_on={opened}", flush=True)
    return {"branch": wap.branch, "opened": opened}


def publish(run_id: str) -> dict:
    from jobs.silver.wap import WapManager
    _, catalog = _context()
    wap = WapManager(run_id)
    published = []
    for name in catalog.registry:
        table = catalog.load(name)
        if wap.has_branch(table):
            wap.publish(table)
            published.append(name)
    print(f"[publish] fast-forwarded main on {published}", flush=True)
    return {"branch": wap.branch, "published": published}


def drop_audit_branches(run_id: str) -> dict:
    from jobs.silver.wap import WapManager
    _, catalog = _context()
    wap = WapManager(run_id)
    dropped = []
    for name in catalog.registry:
        table = catalog.load(name)
        if wap.has_branch(table):
            wap.drop(table)
            dropped.append(name)
    print(f"[drop_audit_branches] dropped {dropped}", flush=True)
    raise RuntimeError(
        f"silver_load run failed; rolled back audit branch {wap.branch!r} on "
        f"{len(dropped)} table(s): {dropped or 'none'}. See the upstream task "
        f"logs for the original error."
    )


def _read_dim(catalog, cfg, name: str, run_id: str):
    from jobs.silver.io import IcebergReader
    from jobs.silver.wap import WapManager
    table = catalog.load(name)
    return IcebergReader(cfg).read(table, WapManager(run_id).snapshot_id(table))


def _stage(source: str, interval_nodash: str, staged, quarantine) -> dict:
    from jobs.silver.io import StagingStore
    from jobs.silver.config import load_config
    store = StagingStore(load_config())
    store.write_staged(source, interval_nodash, staged)
    if quarantine.height:
        store.write_quarantine(source, interval_nodash, quarantine)
    print(f"[stage_{source}] good={staged.height} quarantined={quarantine.height}", flush=True)
    return {"source": source, "good": staged.height, "quarantined": quarantine.height}


def _staging(cfg):
    from jobs.silver.transforms.staging import StagingTransforms
    return StagingTransforms(cfg)


def stage_users(interval_nodash: str) -> dict:
    cfg, _ = _context()
    good, bad = _staging(cfg).users()
    return _stage("users", interval_nodash, good, bad)


def stage_products(interval_nodash: str) -> dict:
    cfg, _ = _context()
    good, bad = _staging(cfg).products()
    return _stage("products", interval_nodash, good, bad)


def stage_categories(interval_nodash: str) -> dict:
    cfg, _ = _context()
    good, bad = _staging(cfg).categories()
    return _stage("categories", interval_nodash, good, bad)


def stage_price_history(interval_nodash: str) -> dict:
    cfg, _ = _context()
    good, bad = _staging(cfg).price_history()
    return _stage("price_history", interval_nodash, good, bad)


def stage_returns(interval_nodash: str) -> dict:
    cfg, _ = _context()
    good, bad = _staging(cfg).returns()
    return _stage("returns", interval_nodash, good, bad)


def stage_order_items(interval_nodash: str) -> dict:
    cfg, _ = _context()
    good, bad = _staging(cfg).order_items()
    return _stage("order_items", interval_nodash, good, bad)


def stage_orders(interval_nodash: str, interval_start: str, interval_end: str) -> dict:
    cfg, _ = _context()
    lo, hi = _window(interval_start, interval_end, cfg.lookback_days)
    good, bad = _staging(cfg).orders(lo, hi)
    return _stage("orders", interval_nodash, good, bad)


def stage_events(interval_nodash: str, interval_start: str, interval_end: str) -> dict:
    cfg, _ = _context()
    lo, hi = _window(interval_start, interval_end, cfg.lookback_days)
    good, bad = _staging(cfg).events(lo, hi)
    return _stage("events", interval_nodash, good, bad)


def _write_dim(name: str, run_id: str, df) -> dict:
    from jobs.silver.wap import WapManager
    _, catalog = _context()
    table = catalog.load(name)
    catalog.overwrite(table, df, branch=WapManager(run_id).effective_branch(table))
    print(f"[load_{name}] rows={df.height}", flush=True)
    return {"table": name, "rows": df.height}


def _write_fact(name: str, run_id: str, df) -> dict:
    from jobs.silver.wap import WapManager
    _, catalog = _context()
    table = catalog.load(name)
    catalog.dynamic_overwrite(table, df, branch=WapManager(run_id).effective_branch(table))
    print(f"[load_{name}] rows={df.height}", flush=True)
    return {"table": name, "rows": df.height}


def _dims(cfg):
    from jobs.silver.transforms.dims import DimensionBuilder
    return DimensionBuilder(cfg)


def _staged(cfg, source: str, interval_nodash: str):
    from jobs.silver.io import StagingStore
    return StagingStore(cfg).read_staged(source, interval_nodash)


def load_dim_user(run_id: str, interval_nodash: str) -> dict:
    cfg, _ = _context()
    users = _staged(cfg, "users", interval_nodash)
    return _write_dim("dim_user", run_id, _dims(cfg).user(users))


def load_dim_product(run_id: str, interval_nodash: str) -> dict:
    cfg, _ = _context()
    ph = _staged(cfg, "price_history", interval_nodash)
    prods = _staged(cfg, "products", interval_nodash)
    return _write_dim("dim_product", run_id, _dims(cfg).product(ph, prods))


def load_dim_category(run_id: str, interval_nodash: str) -> dict:
    cfg, _ = _context()
    cats = _staged(cfg, "categories", interval_nodash)
    return _write_dim("dim_category", run_id, _dims(cfg).category(cats))


def load_dim_payment_method(run_id: str, interval_nodash: str = "") -> dict:
    cfg, _ = _context()
    return _write_dim("dim_payment_method", run_id, _dims(cfg).payment_method())


def load_dim_order_status(run_id: str, interval_nodash: str = "") -> dict:
    cfg, _ = _context()
    return _write_dim("dim_order_status", run_id, _dims(cfg).order_status())


def load_dim_event_type(run_id: str, interval_nodash: str = "") -> dict:
    cfg, _ = _context()
    return _write_dim("dim_event_type", run_id, _dims(cfg).event_type())


def load_dim_device(run_id: str, interval_nodash: str = "") -> dict:
    cfg, _ = _context()
    return _write_dim("dim_device", run_id, _dims(cfg).device())


def load_dim_return_reason(run_id: str, interval_nodash: str = "") -> dict:
    cfg, _ = _context()
    return _write_dim("dim_return_reason", run_id, _dims(cfg).return_reason())


def _facts():
    from jobs.silver.transforms.facts import FactBuilder
    return FactBuilder()


def load_fact_orders(run_id: str, interval_nodash: str) -> dict:
    cfg, catalog = _context()
    orders = _staged(cfg, "orders", interval_nodash)
    items = _staged(cfg, "order_items", interval_nodash)
    df = _facts().orders(
        orders, items,
        _read_dim(catalog, cfg, "dim_user", run_id),
        _read_dim(catalog, cfg, "dim_payment_method", run_id),
        _read_dim(catalog, cfg, "dim_order_status", run_id),
    )
    return _write_fact("fact_orders", run_id, df)


def load_fact_sales(run_id: str, interval_nodash: str) -> dict:
    cfg, catalog = _context()
    orders = _staged(cfg, "orders", interval_nodash)
    items = _staged(cfg, "order_items", interval_nodash)
    df = _facts().sales(
        items, orders,
        _read_dim(catalog, cfg, "dim_user", run_id),
        _read_dim(catalog, cfg, "dim_product", run_id),
        _read_dim(catalog, cfg, "dim_payment_method", run_id),
    )
    return _write_fact("fact_sales", run_id, df)


def load_fact_clickstream(run_id: str, interval_nodash: str) -> dict:
    cfg, catalog = _context()
    events = _staged(cfg, "events", interval_nodash)
    df = _facts().clickstream(
        events,
        _read_dim(catalog, cfg, "dim_user", run_id),
        _read_dim(catalog, cfg, "dim_product", run_id),
        _read_dim(catalog, cfg, "dim_event_type", run_id),
        _read_dim(catalog, cfg, "dim_device", run_id),
    )
    return _write_fact("fact_clickstream", run_id, df)


def load_fact_returns(run_id: str, interval_nodash: str) -> dict:
    from jobs.silver.wap import WapManager
    cfg, catalog = _context()
    returns = _staged(cfg, "returns", interval_nodash)
    df = _facts().returns(
        returns,
        _read_dim(catalog, cfg, "dim_user", run_id),
        _read_dim(catalog, cfg, "dim_product", run_id),
        _read_dim(catalog, cfg, "dim_return_reason", run_id),
    )
    table = catalog.load("fact_returns")
    catalog.overwrite(table, df, branch=WapManager(run_id).effective_branch(table))
    print(f"[load_fact_returns] rows={df.height}", flush=True)
    return {"table": "fact_returns", "rows": df.height}


def run_dq(run_id: str) -> dict:
    from jobs.silver.transforms.dq import DataQualityGate
    from jobs.silver.wap import WapManager
    cfg, catalog = _context()
    wap = WapManager(run_id)
    branch = wap.branch if wap.has_branch(catalog.load("fact_sales")) else None
    report = DataQualityGate(cfg, branch, catalog).run_all()
    print(f"[run_dq] PASSED branch={branch}: {report}", flush=True)
    return report


def run_maintenance() -> dict:
    from jobs.silver.maintenance import Maintenance
    cfg, catalog = _context()
    return Maintenance(cfg, catalog).run()
