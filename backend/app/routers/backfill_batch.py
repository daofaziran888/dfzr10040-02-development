"""按日期批量回填接口：批量补写、分区回执、核验、重试与快照取数。

这些接口刻意和通用资源路由分开：批量回填是跨业务日的复合动作，
需要把补写明细、计数、回执和实际写入证据一起返回给分区快照与下游取数。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.fusion_engine import fusion_engine

router = APIRouter(prefix="/api/backfill-batch", tags=["按日期批量回填"])


class BatchSubmitPayload(BaseModel):
    target_wide_table: str
    dates: list[str] = Field(default_factory=list)
    rows_by_date: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    deliver_receipt_dates: list[str] | None = None
    task_code: str | None = None


class TaskQuery(BaseModel):
    task_code: str | None = None
    target_wide_table: str = ""
    dates: list[str] = Field(default_factory=list)


class RetryPayload(BaseModel):
    task_code: str
    partition_key_value: str


class ConfirmPayload(BaseModel):
    target_wide_table: str
    dates: list[str] = Field(default_factory=list)
    snapshot_statuses: dict[str, str] = Field(default_factory=dict)
    task_code: str | None = None


class ExportPayload(BaseModel):
    plan_id: int | None = None
    target_wide_table: str = ""
    dates: list[str] = Field(default_factory=list)
    task_code: str | None = None


class CommitPayload(BaseModel):
    target_wide_table: str
    partition_key_value: str
    order_id: str
    amount: float
    credential_code: str = ""


class ExportPlanPayload(BaseModel):
    target_wide_table: str
    dates: list[str] = Field(default_factory=list)
    export_format: str = "parquet"


def _find_task_by_dates(target_wide_table: str, dates: list[str]) -> Any:
    """同一宽表 + 同一批业务日视为同一批次，避免重复建任务。"""
    wanted = sorted(dates)
    for task in fusion_engine._backfills.values():
        if task.target_wide_table == target_wide_table and sorted(task.partition_dates) == wanted:
            return task
    return None


@router.get("/scenario")
def scenario() -> dict[str, Any]:
    """值班场景总览：逐日回执、可读日期、待核日期一次看全。"""
    task_code = ""
    for task in fusion_engine._backfills.values():
        if task.receipt_codes:
            task_code = task.task_code
    receipts = fusion_engine.list_backfill_receipts(task_code or None)
    pending = fusion_engine.pending_backfill_partitions(task_code or None)
    return {
        "ok": True,
        "task_code": task_code,
        "receipts": receipts,
        "pending": pending["dates"],
        "readable_dates": [r["分区键值"] for r in receipts if r["实际生效"] == "是"],
    }


@router.post("/tasks")
def create_task(payload: BatchSubmitPayload) -> dict[str, Any]:
    existing = _find_task_by_dates(payload.target_wide_table, payload.dates)
    if existing is None:
        task = fusion_engine.create_backfill_task(
            target_wide_table=payload.target_wide_table,
            backfill_range=(
                f"{min(payload.dates)}~{max(payload.dates)}" if payload.dates else ""
            ),
            partition_dates=payload.dates,
        )
    else:
        task = existing
    return {"ok": True, "task_code": task.task_code, "dates": task.partition_dates,
            "submitted_count": task.submitted_count}


@router.post("/submit")
def submit(payload: BatchSubmitPayload) -> dict[str, Any]:
    task = None
    if payload.task_code:
        task = fusion_engine._find_backfill_by_code(payload.task_code)
    if task is None:
        task = _find_task_by_dates(payload.target_wide_table, payload.dates)
    if task is None:
        task = fusion_engine.create_backfill_task(
            target_wide_table=payload.target_wide_table,
            backfill_range=(
                f"{min(payload.dates)}~{max(payload.dates)}" if payload.dates else ""
            ),
            partition_dates=payload.dates,
        )
    if not task.partition_dates:
        task.partition_dates = list(payload.dates)
    return fusion_engine.submit_batch_backfill(
        task.task_id, payload.rows_by_date,
        deliver_receipt_dates=payload.deliver_receipt_dates,
    )


@router.get("/receipts")
def receipts(task_code: str | None = None) -> dict[str, Any]:
    return {"ok": True, "items": fusion_engine.list_backfill_receipts(task_code)}


@router.get("/pending")
def pending(task_code: str | None = None) -> dict[str, Any]:
    return fusion_engine.pending_backfill_partitions(task_code)


@router.post("/verify")
def verify(payload: TaskQuery) -> dict[str, Any]:
    return fusion_engine.verify_backfill_receipts(
        payload.task_code, payload.target_wide_table, payload.dates)


@router.post("/retry")
def retry(payload: RetryPayload) -> dict[str, Any]:
    return fusion_engine.retry_backfill_partition(
        payload.task_code, payload.partition_key_value)


@router.post("/confirm")
def confirm(payload: ConfirmPayload) -> dict[str, Any]:
    return fusion_engine.confirm_snapshot_from_backfill(
        target_wide_table=payload.target_wide_table,
        dates=payload.dates,
        snapshot_statuses=payload.snapshot_statuses,
        task_code=payload.task_code,
    )


@router.post("/commit")
def commit(payload: CommitPayload) -> dict[str, Any]:
    record = fusion_engine.record_partition_commit(
        target_wide_table=payload.target_wide_table,
        partition_key_value=payload.partition_key_value,
        order_id=payload.order_id,
        amount=payload.amount,
        credential_code=payload.credential_code,
    )
    return {"ok": True, "record": record.to_record()}


@router.post("/export-plans")
def create_export_plan(payload: ExportPlanPayload) -> dict[str, Any]:
    plan = fusion_engine.create_export_plan(
        target_wide_table=payload.target_wide_table,
        export_format=payload.export_format,
        partition_filter=",".join(payload.dates),
    )
    return {"ok": True, "plan_id": plan.plan_id, "plan": plan.__dict__}


@router.post("/export")
def export(payload: ExportPayload) -> dict[str, Any]:
    return fusion_engine.export_readable_partitions(
        export_id=payload.plan_id,
        target_wide_table=payload.target_wide_table,
        dates=payload.dates,
        task_code=payload.task_code,
    )


@router.get("/summary")
def summary(task_code: str) -> dict[str, Any]:
    return fusion_engine.backfill_batch_snapshot(task_code)
