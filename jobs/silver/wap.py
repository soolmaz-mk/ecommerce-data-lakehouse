# Write-Audit-Publish: makes a temporary branch for each run.
# The run writes there first, and main only moves if the checks pass.
from __future__ import annotations

from pyiceberg.table import Table

MAIN_BRANCH = "main"


class WapManager:

    BRANCH_PREFIX = "audit_"
    UNSAFE_CHARS = (":", "+", ".")

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.branch = self.branch_name(run_id)

    @classmethod
    def branch_name(cls, run_id: str) -> str:
        safe = run_id
        for ch in cls.UNSAFE_CHARS:
            safe = safe.replace(ch, "_")
        return cls.BRANCH_PREFIX + safe

    def has_branch(self, table: Table) -> bool:
        return self.branch in table.metadata.refs

    def snapshot_id(self, table: Table) -> int | None:
        ref = table.metadata.refs.get(self.branch)
        return ref.snapshot_id if ref else None

    def effective_branch(self, table: Table) -> str:
        return self.branch if self.has_branch(table) else MAIN_BRANCH

    def open(self, table: Table) -> bool:
        tip = table.metadata.current_snapshot_id
        if tip is None:
            return False
        if self.has_branch(table):
            table.manage_snapshots().remove_branch(self.branch).commit()
            table.refresh()
        table.manage_snapshots().create_branch(tip, self.branch).commit()
        table.refresh()
        return True

    def publish(self, table: Table) -> None:
        table.manage_snapshots().set_current_snapshot(ref_name=self.branch).commit()
        table.refresh()
        if self.has_branch(table):
            table.manage_snapshots().remove_branch(self.branch).commit()

    def drop(self, table: Table) -> None:
        table.refresh()
        if self.has_branch(table):
            table.manage_snapshots().remove_branch(self.branch).commit()
