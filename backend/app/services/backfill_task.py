"""数据回填领域服务：断点续传之外，补齐按日期批量补写与分区回执。

通用资源骨架负责查询/登记/动作；批量回填的任务和回执由融合引擎统一持有，
这里把它们叠加进列表，值班同事能在同一个模块里看到逐日计数。
"""
from __future__ import annotations

from typing import Any

from app.fusion_engine import fusion_engine
from app.services.base import BaseModuleService
from app.store import store


class BackfillTaskService(BaseModuleService):
    MODULE = "backfill_task"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "目标宽表", "回填区间", "并行分区", "依赖检查", "断点位置", "已处理行", "剩余行数",
                   "补写日期", "提交次数", "回填分区回执", "批次状态"]
    REQUIRED_FIELDS = ["任务编号", "目标宽表", "回填区间"]
    STATUS_ORDER = ["待回填", "回填中", "已完成", "回填失败"]
    ACTION_RULES = {"启动回填": "回填中", "暂停回填": "已完成", "标记失败": "回填失败"}
    NEGATIVE_ACTIONS = []

    STATUS_MAP = {
        "待回填": "待回填",
        "回填中": "回填中",
        "回执待交接": "回填中",
        "部分待核": "回填中",
        "已完成": "已完成",
        "回填失败": "回填失败",
    }

    def _engine_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for task in fusion_engine._backfills.values():
            if not task.partition_dates and not task.receipt_codes:
                continue
            record = task.to_record()
            row: dict[str, Any] = {"id": task.task_id}
            row.update(record)
            status = self.STATUS_MAP.get(task.batch_status, "回填中")
            row["status"] = status
            row["pending"] = status != "已完成"
            row["abnormal"] = status == "回填失败"
            rows.append(row)
        return rows

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        engine_rows = self._engine_rows()
        rows = engine_rows + list(store.rows(self.MODULE))
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
        receipts = fusion_engine.list_backfill_receipts()
        snapshot["partition_receipts"] = len(receipts)
        snapshot["effective_dates"] = [
            receipt["分区键值"] for receipt in receipts if receipt["实际生效"] == "是"
        ]
        snapshot["pending_dates"] = [
            receipt["分区键值"] for receipt in receipts if receipt["实际生效"] != "是"
        ]
        return snapshot
