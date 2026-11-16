# Weekly cleanup: deletes old snapshots and leftover audit branches.
# Branches are removed first, because a branch keeps its snapshot alive.
from __future__ import annotations

from datetime import datetime, timezone

from jobs.silver.catalog import IcebergCatalog
from jobs.silver.config import SilverConfig, load_config
from jobs.silver.wap import WapManager


class Maintenance:

    def __init__(self, cfg: SilverConfig | None = None, catalog: IcebergCatalog | None = None):
        self.cfg = cfg or load_config()
        self.catalog = catalog or IcebergCatalog(self.cfg)

    def sweep_stale_branches(self, table, now: datetime) -> list[str]:
        cutoff_ms = int((now - self.cfg.stale_branch_age).timestamp() * 1000)
        dropped = []
        prefix = WapManager.BRANCH_PREFIX
        for ref_name in [r for r in list(table.metadata.refs) if r.startswith(prefix)]:
            ref = table.metadata.refs.get(ref_name)
            snapshot = table.snapshot_by_id(ref.snapshot_id) if ref else None
            if snapshot is not None and snapshot.timestamp_ms >= cutoff_ms:
                continue
            table.manage_snapshots().remove_branch(ref_name).commit()
            table.refresh()
            dropped.append(ref_name)
        return dropped

    def expire_snapshots(self, table, cutoff: datetime) -> None:
        table.maintenance.expire_snapshots().older_than(cutoff).commit()
        table.refresh()

    def run(self) -> dict:
        now = datetime.now(timezone.utc)
        cutoff = now - self.cfg.snapshot_retention

        report: dict[str, dict] = {}
        for name in self.catalog.registry:
            if not self.catalog.exists(name):
                continue
            table = self.catalog.load(name)

            before = len(table.metadata.snapshots)
            dropped = self.sweep_stale_branches(table, now)
            self.expire_snapshots(table, cutoff)
            after = len(table.metadata.snapshots)

            report[name] = {
                "stale_branches_dropped": dropped,
                "snapshots_before": before,
                "snapshots_after": after,
                "snapshots_expired": before - after,
            }

        expired = sum(e["snapshots_expired"] for e in report.values())
        branches = sum(len(e["stale_branches_dropped"]) for e in report.values())
        print(
            f"[run_maintenance] cutoff={cutoff.isoformat()} tables={len(report)} "
            f"snapshots_expired={expired} branches_dropped={branches}",
            flush=True,
        )
        return report
