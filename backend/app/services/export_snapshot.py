"""快照导出领域服务：取数入口按回填分区回执和实际写入证据选择可读分区。

列表仍是导出计划；批量取数走 /api/backfill-batch/export，
由融合引擎在返回订单行时带上业务日期和入账凭据。
"""
from __future__ import annotations

from typing import Any

from app.fusion_engine import fusion_engine
from app.services.base import BaseModuleService


class ExportSnapshotService(BaseModuleService):
    MODULE = "export_snapshot"
    ENTITY = "导出计划"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["计划编号", "目标宽表", "导出格式", "分区筛选", "字段筛选", "导出路径", "行数限制", "导出状态",
                   "可读分区", "跳过分区", "实际取数行"]
    REQUIRED_FIELDS = ["计划编号", "目标宽表", "导出格式"]
    STATUS_ORDER = ["待导出", "导出中", "已完成", "导出失败"]
    ACTION_RULES = {"启动导出": "导出中", "取消导出": "已完成", "重试导出": "导出失败"}
    NEGATIVE_ACTIONS = []

    def _engine_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for plan in fusion_engine._exports.values():
            if not plan.partition_filter:
                continue
            dates = [part.strip() for part in plan.partition_filter.split(",") if part.strip()]
            export = fusion_engine.export_readable_partitions(
                export_id=plan.plan_id, dates=dates, materialize_missing=False)
            row: dict[str, Any] = {"id": plan.plan_id}
            row.update({
                "计划编号": plan.plan_code,
                "目标宽表": plan.target_wide_table,
                "导出格式": plan.export_format,
                "分区筛选": plan.partition_filter,
                "字段筛选": ",".join(plan.field_filter),
                "导出路径": plan.export_path,
                "行数限制": str(plan.row_limit),
                "导出状态": plan.export_status,
                "可读分区": ",".join(export["readable_dates"]),
                "跳过分区": ",".join(item["分区键值"] for item in export["skipped_dates"]),
                "实际取数行": str(export["exported_rows"]),
            })
            row["status"] = plan.export_status
            row["pending"] = plan.export_status != "已完成"
            row["abnormal"] = bool(export["skipped_dates"])
            rows.append(row)
        return rows

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        from app.store import store

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
        from app.store import store

        return store.find(self.MODULE, entry_id)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        plans = self._engine_rows()
        snapshot["plans_with_partition_filter"] = len(plans)
        snapshot["readable_dates"] = [
            row["可读分区"] for row in plans if row["可读分区"]
        ]
        return snapshot
