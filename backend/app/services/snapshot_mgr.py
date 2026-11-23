"""分区快照领域服务：通用骨架之外，叠加回填回执确认后的分区状态。

快照端和回填端会并发记录完成状态；列表里以回填回执、实际写入和
实际生效日期为准，让下游一眼看出哪些分区已完整提交。
"""
from __future__ import annotations

from typing import Any

from app.fusion_engine import fusion_engine
from app.services.base import BaseModuleService
from app.store import store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "快照分区"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["分区编号", "目标宽表", "分区键值", "记录行数", "Schema摘要", "状态标记", "依赖分区", "创建时间",
                   "回填回执", "回填生效", "待核行数", "实际生效日期"]
    REQUIRED_FIELDS = ["分区编号", "目标宽表", "分区键值"]
    STATUS_ORDER = ["创建中", "可用", "标记中", "已过期"]
    ACTION_RULES = {"创建快照": "可用", "标记版本": "标记中", "清理过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def _engine_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for snap in fusion_engine._snapshots.values():
            if not snap.backfill_receipt_code:
                continue
            row: dict[str, Any] = {"id": snap.partition_id}
            row.update(snap.to_record())
            status = snap.status_mark if snap.status_mark in self.STATUS_ORDER else "创建中"
            row["status"] = status
            row["pending"] = status != "可用"
            row["abnormal"] = False
            rows.append(row)
        return rows

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        rows = self._engine_rows() + list(store.rows(self.MODULE))
        filters = kwargs.get("filters") or {}
        for field, expected in filters.items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        keyword = kwargs.get("keyword")
        if keyword:
            rows = [
                row for row in rows
                if keyword.casefold() in str(row.get(self.LIST_FIELDS[0], "")).casefold()
            ]
        status = kwargs.get("status")
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        page = int(kwargs.get("page", 1))
        size = int(kwargs.get("size", 20))
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        for row in self._engine_rows():
            if row["id"] == entry_id:
                return row
        return store.find(self.MODULE, entry_id)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        engine_rows = self._engine_rows()
        snapshot["backfill_partitions"] = len(engine_rows)
        snapshot["readable_partitions"] = [
            row["分区键值"] for row in engine_rows if row["回填生效"] == "是"
        ]
        snapshot["pending_partitions"] = [
            row["分区键值"] for row in engine_rows if row["回填生效"] != "是"
        ]
        return snapshot
