"""熔炉引擎：跨模块数据融合流水线编排器。

负责执行数据源 -> Schema 映射 -> 宽表定义 -> 融合作业的完整链路，
并管理冲突仲裁、质量门禁、分区快照、水位追踪与增量归并。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.models import (
    BackfillPartitionReceipt,
    BackfillTask,
    ColdStoreStrategy,
    CrossValidatePlan,
    DataDriftRecord,
    ExportPlan,
    FusionJob,
    FusionMonitorMetric,
    LineageEdge,
    MergeConflictRule,
    MetricAggrRule,
    PartitionRule,
    PartitionCommitRecord,
    QualityGateRule,
    SchemaMapping,
    SnapshotPartition,
    SourceDef,
    WatermarkRecord,
    WideTableDef,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_code(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"


class FusionEngine:
    """熔炉引擎：编排跨模块数据融合流水线。"""

    def __init__(self) -> None:
        self._sources: dict[int, SourceDef] = {}
        self._schemas: dict[int, SchemaMapping] = {}
        self._wide_tables: dict[int, WideTableDef] = {}
        self._fusion_jobs: dict[int, FusionJob] = {}
        self._conflict_rules: dict[int, MergeConflictRule] = {}
        self._quality_gates: dict[int, QualityGateRule] = {}
        self._snapshots: dict[int, SnapshotPartition] = {}
        self._backfills: dict[int, BackfillTask] = {}
        self._cold_stores: dict[int, ColdStoreStrategy] = {}
        self._lineages: dict[int, LineageEdge] = {}
        self._metrics: dict[int, MetricAggrRule] = {}
        self._partitions: dict[int, PartitionRule] = {}
        self._watermarks: dict[int, WatermarkRecord] = {}
        self._exports: dict[int, ExportPlan] = {}
        self._cross_validates: dict[int, CrossValidatePlan] = {}
        self._drifts: dict[int, DataDriftRecord] = {}
        self._monitors: dict[int, FusionMonitorMetric] = {}
        self._partition_commits: dict[int, PartitionCommitRecord] = {}
        self._backfill_receipts: dict[int, BackfillPartitionReceipt] = {}
        # 实际写入证据：key=(目标宽表, 分区键值)，按订单号去重，重试只补缺不重复落行。
        self._backfill_writes: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
        self._next_id: int = 1

    def _next_seq(self) -> int:
        sid = self._next_id
        self._next_id += 1
        return sid

    # ---- 1. source_def -> schema_map -> wide_table_def -> fusion_job ----

    def register_source(self, source_code: str, source_name: str, source_type: str,
                        connection_string: str, auth_method: str = "jdbc",
                        charset: str = "UTF-8", timezone: str = "Asia/Shanghai") -> SourceDef:
        sid = self._next_seq()
        source = SourceDef(
            source_id=sid, source_code=source_code, source_name=source_name,
            source_type=source_type, connection_string=connection_string,
            auth_method=auth_method, charset=charset, timezone=timezone,
            access_status="待验证",
        )
        self._sources[sid] = source
        return source

    def create_schema_mapping(self, source_table: str, target_wide_table: str,
                              field_mapping_json: str, type_conversion: str = "",
                              missing_policy: str = "skip") -> SchemaMapping:
        sid = self._next_seq()
        mapping = SchemaMapping(
            mapping_id=sid, plan_code=_generate_code("SCHE", sid),
            source_table=source_table, target_wide_table=target_wide_table,
            field_mapping_json=field_mapping_json,
            type_conversion_rules=type_conversion,
            missing_value_policy=missing_policy,
            mapping_order="ASC", plan_status="草稿",
        )
        self._schemas[sid] = mapping
        return mapping

    def define_wide_table(self, table_code: str, table_name: str,
                          field_list: list[str], primary_key: str = "id",
                          partition_key: str = "dt", ttl_days: int = 90) -> WideTableDef:
        sid = self._next_seq()
        wt = WideTableDef(
            table_id=sid, table_code=table_code, table_name=table_name,
            field_list=field_list, primary_key=primary_key,
            partition_key=partition_key, ttl_days=ttl_days,
            source_count=0, last_updated=_now_iso(),
        )
        self._wide_tables[sid] = wt
        return wt

    def create_fusion_job(self, target_wide_table: str, participant_sources: list[str],
                          join_key: str, fusion_window: str = "1d",
                          watermark_field: str = "update_time") -> FusionJob:
        sid = self._next_seq()
        job = FusionJob(
            job_id=sid, job_code=_generate_code("FUSI", sid),
            target_wide_table=target_wide_table,
            participant_sources=participant_sources,
            join_key=join_key, fusion_window=fusion_window,
            watermark_field=watermark_field,
            output_rows=0, skipped_rows=0,
        )
        self._fusion_jobs[sid] = job
        return job

    # ---- 2. fusion_job -> merge_conflict -> quality_gate -> snapshot_mgr ----

    def create_conflict_rule(self, target_wide_table: str, conflict_fields: list[str],
                             arbitration_strategy: str = "source_priority",
                             priority_source: str = "", time_baseline: str = "") -> MergeConflictRule:
        sid = self._next_seq()
        rule = MergeConflictRule(
            rule_id=sid, rule_code=_generate_code("MERG", sid),
            target_wide_table=target_wide_table,
            conflict_fields=conflict_fields,
            arbitration_strategy=arbitration_strategy,
            priority_source=priority_source,
            time_baseline=time_baseline or _now_iso(),
            conflict_count=0, rule_status="草稿",
        )
        self._conflict_rules[sid] = rule
        return rule

    def create_quality_gate(self, target_wide_table: str, check_type: str,
                            threshold_condition: str, trigger_action: str = "block",
                            block_level: str = "warning") -> QualityGateRule:
        sid = self._next_seq()
        gate = QualityGateRule(
            rule_id=sid, rule_code=_generate_code("QUAL", sid),
            target_wide_table=target_wide_table,
            check_type=check_type,
            threshold_condition=threshold_condition,
            trigger_action=trigger_action,
            block_level=block_level,
            checked_rows=0, last_triggered="",
        )
        self._quality_gates[sid] = gate
        return gate

    def create_snapshot(self, target_wide_table: str, partition_key_value: str,
                        dependency_partitions: list[str] | None = None,
                        schema_digest: str = "") -> SnapshotPartition:
        sid = self._next_seq()
        snap = SnapshotPartition(
            partition_id=sid,
            partition_code=_generate_code("SNAP", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            record_count=0,
            schema_digest=schema_digest or f"SHA256-{uuid.uuid4().hex[:8]}",
            status_mark="创建中",
            dependency_partitions=dependency_partitions or [],
            created_time=_now_iso(),
        )
        self._snapshots[sid] = snap
        return snap

    # ---- 3. fusion_job -> watermark_track -> delta_merge ----

    def create_watermark(self, data_source: str, current_watermark: str,
                         data_origin: str = "fusion_job") -> WatermarkRecord:
        sid = self._next_seq()
        wm = WatermarkRecord(
            record_id=sid, record_code=_generate_code("WATE", sid),
            data_source=data_source,
            current_watermark=current_watermark,
            previous_watermark="",
            advance_interval="5m",
            delay_minutes=0,
            data_origin=data_origin,
            last_advance=_now_iso(),
        )
        self._watermarks[sid] = wm
        return wm

    def advance_watermark(self, record_id: int, new_watermark: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(record_id)
        if wm is None:
            return None
        wm.previous_watermark = wm.current_watermark
        wm.current_watermark = new_watermark
        wm.last_advance = _now_iso()
        return wm

    # ---- 4. snapshot_mgr -> backfill_task -> cold_store ----

    def create_backfill_task(self, target_wide_table: str, backfill_range: str,
                             parallel_partitions: int = 4, dependency_check: str = "",
                             checkpoint_position: str = "",
                             partition_dates: list[str] | None = None) -> BackfillTask:
        sid = self._next_seq()
        task = BackfillTask(
            task_id=sid, task_code=_generate_code("BACK", sid),
            target_wide_table=target_wide_table,
            backfill_range=backfill_range,
            parallel_partitions=parallel_partitions,
            dependency_check=dependency_check,
            checkpoint_position=checkpoint_position,
            processed_rows=0, remaining_rows=0,
            partition_dates=list(partition_dates or []),
        )
        self._backfills[sid] = task
        return task

    # ---- 4.1 按日期批量回填与分区回执 ----

    def record_partition_commit(self, target_wide_table: str, partition_key_value: str,
                                order_id: str, amount: float | str,
                                credential_code: str = "", committed: bool = True,
                                source: str = "partition_table") -> PartitionCommitRecord:
        """登记分区表侧的入账凭据。

        行身份是（目标宽表、分区日期、订单号）：同一个订单号出现在不同业务日时，
        按所在日期分别入账，不做跨日去重。
        """
        order_key = str(order_id)
        for record in self._partition_commits.values():
            if (record.target_wide_table == target_wide_table
                    and record.partition_key_value == partition_key_value
                    and record.order_id == order_key):
                record.amount = float(amount)
                record.committed = committed
                record.source = source
                if credential_code:
                    record.credential_code = credential_code
                return record

        sid = self._next_seq()
        record = PartitionCommitRecord(
            commit_id=sid, commit_code=_generate_code("PCOM", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            order_id=order_key,
            amount=float(amount),
            credential_code=credential_code or f"CRED-{sid:04d}",
            committed=committed,
            source=source,
            created_time=_now_iso(),
        )
        self._partition_commits[sid] = record
        return record

    def submit_batch_backfill(self, task_id: int,
                              rows_by_date: dict[str, list[dict[str, Any]]],
                              deliver_receipt_dates: list[str] | None = None) -> dict[str, Any]:
        """按业务日批量补写：一次提交、逐日处理、逐日留下分区回执。

        - 同一批业务日只允许提交一次，重复提交会被拒绝；
        - 实际写入按（宽表、日期、订单号）去重，重试只补缺，不重复计入；
        - 每个日期的新增行以分区提交记录核定，实际写入与入账凭据逐单对齐；
        - deliver_receipt_dates 用来模拟回执交接中断：未列出的日期实际已写入，
          但回执没有交付到下游，状态停在“待核/已核验”。
        """
        task = self._backfills.get(task_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {task_id} 不存在"}
        if task.submitted_count >= 1:
            return {
                "ok": False,
                "message": "同一批业务日只允许提交一次，请对未生效分区执行重试",
                "duplicate_submission": True,
                "submitted_count": task.submitted_count,
            }

        dates = [date for date in (task.partition_dates or sorted(rows_by_date)) if date]
        if not dates:
            return {"ok": False, "message": "批量回填至少需要一个业务日"}

        deliver = set(dates if deliver_receipt_dates is None else deliver_receipt_dates)
        per_date: list[dict[str, Any]] = []
        submitted_total = 0

        for date in dates:
            rows = self._normalize_backfill_rows(rows_by_date.get(date, []))
            if rows:
                self._write_backfill_partition(task.target_wide_table, date, rows)
            receipt = self._upsert_backfill_receipt(
                task, date, submitted_rows=len(rows), deliver=date in deliver)
            self._refresh_backfill_receipt(receipt)
            submitted_total += len(rows)
            per_date.append(receipt.to_record())

        task.submitted_count += 1
        task.processed_rows += submitted_total
        task.checkpoint_position = ""
        task.receipt_codes = [receipt["回执编号"] for receipt in per_date]
        created = [self._find_receipt(task.task_code, date) for date in dates]
        task.remaining_rows = sum(receipt.pending_rows for receipt in created if receipt)
        if all(receipt.effective and receipt.delivered for receipt in created if receipt):
            task.batch_status = "已完成"
        elif any(receipt.effective for receipt in created if receipt):
            task.batch_status = "部分待核"
        else:
            task.batch_status = "待核"

        return {
            "ok": True,
            "message": "批量回填写入完成，分区回执逐日生成",
            "task_code": task.task_code,
            "submitted_count": task.submitted_count,
            "submitted_rows": submitted_total,
            "dates": dates,
            "receipts": per_date,
            "batch_status": task.batch_status,
        }

    def list_backfill_receipts(self, task_code: str | None = None) -> list[dict[str, Any]]:
        """列出回填分区回执；只传日期时按任务编号过滤。"""
        receipts = list(self._backfill_receipts.values())
        if task_code:
            receipts = [r for r in receipts if r.task_code == task_code]
        receipts.sort(key=lambda r: (r.partition_key_value, r.receipt_code))
        return [r.to_record() for r in receipts]

    def verify_backfill_receipts(self, task_code: str | None = None,
                                 target_wide_table: str = "",
                                 dates: list[str] | None = None) -> dict[str, Any]:
        """核验缺回执或待核日期的实际写入，回填回执里标清已提交和待核行数。

        分区表已有入账凭据或回填端已有实际写入、但回执缺失的日期，
        会先补出一条待核回执，再按逐单比对结果核定行数。
        """
        table = target_wide_table
        if not table and task_code:
            task = self._find_backfill_by_code(task_code)
            table = task.target_wide_table if task else ""
        candidate_tables = {table} if table else {
            record.target_wide_table for record in self._partition_commits.values()
        } | {key[0] for key in self._backfill_writes}
        candidate_dates: set[str] = set(dates or [])
        if not candidate_dates:
            candidate_dates = {
                record.partition_key_value for record in self._partition_commits.values()
            } | {key[1] for key in self._backfill_writes}
        for candidate_table in candidate_tables:
            for date in sorted(candidate_dates):
                if self._find_receipt(task_code, date, candidate_table) is not None:
                    continue
                evidence = self._partition_evidence(candidate_table, date)
                if evidence["committed_rows"] == 0 and evidence["actual_rows"] == 0:
                    continue
                # 缺回执：实际写入尚未交付，补出的回执按待核定。
                self._upsert_backfill_receipt(
                    None, date, submitted_rows=evidence["actual_rows"],
                    deliver=False, target_wide_table=candidate_table)

        receipts = list(self._backfill_receipts.values())
        if task_code:
            receipts = [r for r in receipts if r.task_code == task_code]
        if table:
            receipts = [r for r in receipts if r.target_wide_table == table]
        if dates:
            wanted = set(dates)
            receipts = [r for r in receipts if r.partition_key_value in wanted]
        verified: list[dict[str, Any]] = []
        for receipt in receipts:
            self._refresh_backfill_receipt(receipt)
            verified.append(receipt.to_record())
        return {
            "ok": True,
            "task_code": task_code or "",
            "verified": len(verified),
            "receipts": verified,
            "pending_rows": sum(
                int(item["待核行"]) for item in verified),
        }

    def retry_backfill_partition(self, task_code: str, partition_key_value: str) -> dict[str, Any]:
        """重试单个业务日：幂等重放实际写入，重新核定并交付回执。

        不会增加提交次数，也不会把同一订单重复计入：
        已存在的订单行只做覆盖核对，新增行数保持不变。
        """
        receipt = self._find_receipt(task_code, partition_key_value)
        if receipt is None:
            return {"ok": False, "message": f"业务日 {partition_key_value} 没有回填分区回执"}

        attempted = [
            {"order_id": line.get("order_id", ""), "amount": line.get("amount", 0)}
            for line in receipt.detail
        ]
        before = len(self._backfill_writes.get(
            (receipt.target_wide_table, receipt.partition_key_value), {}))
        duplicates = 0
        for line in attempted:
            key = (receipt.target_wide_table, receipt.partition_key_value)
            writes = self._backfill_writes.setdefault(key, {})
            if str(line["order_id"]) in writes:
                duplicates += 1
            writes[str(line["order_id"])] = {
                "order_id": str(line["order_id"]),
                "amount": float(line["amount"]),
            }
        after = len(self._backfill_writes.get(
            (receipt.target_wide_table, receipt.partition_key_value), {}))

        receipt.retry_count += 1
        receipt.delivered = True
        self._refresh_backfill_receipt(receipt)

        task = self._find_backfill_by_code(task_code)
        if task is not None:
            task.batch_status = (
                "已完成" if all(
                    (r := self._find_receipt(task_code, date)) is not None
                    and r.effective
                    and r.delivered
                    for date in task.partition_dates
                ) else "部分待核"
            )

        return {
            "ok": True,
            "message": "分区重试完成，回执已交付，未重复计入",
            "task_code": task_code,
            "partition_key_value": partition_key_value,
            "submitted_rows": receipt.submitted_rows,
            "committed_rows": receipt.committed_rows,
            "pending_rows": receipt.pending_rows,
            "actual_rows": receipt.actual_rows,
            "rows_added": after - before,
            "duplicates_skipped": duplicates,
            "effective": receipt.effective,
            "status": receipt.status,
            "receipt": receipt.to_record(),
        }

    def pending_backfill_partitions(self, task_code: str | None = None) -> dict[str, Any]:
        """仍待核的业务日及行数，交给值班同事续办。

        已按实际写入核验通过、能够供下游取数的日期不会再出现在这里，
        保证同一业务日不会同时作为“已交付”和“待补发”。
        """
        receipts = list(self._backfill_receipts.values())
        if task_code:
            receipts = [r for r in receipts if r.task_code == task_code]
        latest: dict[tuple[str, str], BackfillPartitionReceipt] = {}
        for receipt in receipts:
            latest[(receipt.target_wide_table, receipt.partition_key_value)] = receipt

        pending: list[dict[str, Any]] = []
        for receipt in latest.values():
            self._refresh_backfill_receipt(receipt)
            if receipt.effective:
                continue
            pending.append({
                "任务编号": receipt.task_code,
                "目标宽表": receipt.target_wide_table,
                "分区键值": receipt.partition_key_value,
                "已提交行": receipt.committed_rows,
                "待核行": receipt.pending_rows,
                "实际写入行": receipt.actual_rows,
                "回执编号": receipt.receipt_code,
                "说明": receipt.note or "实际写入与分区提交记录不一致，需值班核验",
            })
        pending.sort(key=lambda item: (item["目标宽表"], item["分区键值"]))
        return {"ok": True, "count": len(pending), "dates": pending}

    def confirm_snapshot_from_backfill(
        self, target_wide_table: str, dates: list[str] | None = None,
        snapshot_statuses: dict[str, str] | None = None,
        task_code: str | None = None,
    ) -> dict[str, Any]:
        """分区快照并发请求多个业务日时，用回填回执和实际写入确认分区状态。

        两个端各自记录完成状态时会冲突：快照端可能已经标了“已完成”，
        回填端回执却因交接中断仍停在待核。这里以分区提交记录和实际写入为准，
        只有完全对得上的日期才标记为可用并交给下游。
        """
        if dates is None:
            dates = []
            for receipt in self._backfill_receipts.values():
                if receipt.target_wide_table == target_wide_table:
                    dates.append(receipt.partition_key_value)
        dates = sorted(set(dates))

        results: list[dict[str, Any]] = []
        effective_dates: list[str] = []
        pending_dates: list[dict[str, Any]] = []
        conflict_dates: list[str] = []
        statuses = snapshot_statuses or {}

        for date in dates:
            receipt = self._find_receipt(task_code, date, target_wide_table)
            if receipt is None:
                # 缺回执先按实际写入补一条待核回执，再参与状态核定。
                self.verify_backfill_receipts(
                    task_code=task_code, target_wide_table=target_wide_table, dates=[date])
                receipt = self._find_receipt(task_code, date, target_wide_table)
            if receipt is None:
                results.append({
                    "分区键值": date,
                    "回填回执状态": "缺回执",
                    "快照记录状态": statuses.get(date, "未记录"),
                    "状态冲突": statuses.get(date) in ("已完成", "可用", "已提交"),
                    "回填生效": False,
                    "核定行数": 0,
                    "待核行": 0,
                    "结论": "缺回执且无实际写入，不能交给下游",
                })
                pending_dates.append({"分区键值": date, "待核行": 0})
                continue

            self._refresh_backfill_receipt(receipt)
            claimed = statuses.get(date)
            snapshot_complete = claimed in ("已完成", "可用", "已提交")
            backfill_complete = receipt.delivered and receipt.status == "已提交"
            conflict = claimed is not None and snapshot_complete != backfill_complete
            if conflict:
                conflict_dates.append(date)

            snap = self._find_snapshot(target_wide_table, date)
            if snap is None:
                snap = self.create_snapshot(target_wide_table, date)
            snap.record_count = receipt.committed_rows
            snap.status_mark = "可用" if receipt.effective else "创建中"
            snap.backfill_receipt_code = receipt.receipt_code
            snap.backfill_effective = receipt.effective
            snap.backfill_pending_rows = receipt.pending_rows
            # 实际生效日期按整张宽表统计，避免只传单个日期时看不出本次回填生效范围。
            snap.backfill_effective_dates = sorted({
                candidate.partition_key_value
                for candidate in self._backfill_receipts.values()
                if candidate.target_wide_table == target_wide_table
                and (not task_code or candidate.task_code == task_code)
                and candidate.effective
            })

            if receipt.effective:
                effective_dates.append(date)
            else:
                pending_dates.append({
                    "分区键值": date,
                    "待核行": receipt.pending_rows,
                })

            results.append({
                "分区键值": date,
                "分区编号": snap.partition_code,
                "回填回执状态": receipt.status,
                "回执已交付": receipt.delivered,
                "快照记录状态": claimed or "未记录",
                "状态冲突": conflict,
                "回填生效": receipt.effective,
                "核定行数": receipt.committed_rows,
                "待核行": receipt.pending_rows,
                "实际写入行": receipt.actual_rows,
                "结论": (
                    "分区已完整提交，可交给下游"
                    if receipt.effective else "存在待核行，继续留在值班清单"
                ),
            })

        return {
            "ok": True,
            "target_wide_table": target_wide_table,
            "dates": results,
            "effective_dates": effective_dates,
            "pending_dates": pending_dates,
            "conflict_dates": conflict_dates,
            "conflict": len(conflict_dates) > 0,
        }

    # ---- 4.2 从分区快照按回执取数 ----

    def export_readable_partitions(self, export_id: int | None = None,
                                   target_wide_table: str = "",
                                   dates: list[str] | None = None,
                                   task_code: str | None = None,
                                   materialize_missing: bool = True) -> dict[str, Any]:
        """快照导出取数入口：按回填分区回执和实际写入证据选择可读分区。

        只有实际写入与分区提交记录逐单对齐的日期才会被读出；
        缺回执的日期会先核验实际写入，仍未对齐的日期跳过并留下待核行数。
        返回的订单行同时带业务日期，能看出这次回填实际生效的日期。
        materialize_missing=False 时用于只读预览：缺回执的日期只核验、不补登回执。
        """
        plan = self._exports.get(export_id) if export_id is not None else None
        if export_id is not None and plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}

        table = target_wide_table or (plan.target_wide_table if plan else "")
        requested = list(dates or [])
        if not requested and plan is not None and plan.partition_filter:
            requested = [
                part.strip() for part in plan.partition_filter.split(",") if part.strip()
            ]
        if not requested:
            requested = sorted({
                receipt.partition_key_value
                for receipt in self._backfill_receipts.values()
                if receipt.target_wide_table == table
            })

        rows: list[dict[str, Any]] = []
        readable_dates: list[str] = []
        skipped: list[dict[str, Any]] = []
        receipts_used: list[dict[str, Any]] = []
        per_date_counts: dict[str, int] = {}

        for date in sorted(set(requested)):
            receipt = self._find_receipt(task_code, date, table)
            if receipt is None and materialize_missing:
                # 缺回执的日期先核验实际写入，补出的回执仍按待核定，不会被当成已交付。
                self.verify_backfill_receipts(
                    task_code=task_code, target_wide_table=table, dates=[date])
                receipt = self._find_receipt(task_code, date, table)
            if receipt is None:
                skipped.append({"分区键值": date, "待核行": 0, "原因": "没有回填回执或实际写入"})
                continue

            self._refresh_backfill_receipt(receipt)
            receipts_used.append(receipt.to_record())
            if not receipt.effective:
                skipped.append({
                    "分区键值": date,
                    "待核行": receipt.pending_rows,
                    "原因": "实际写入与分区提交记录未对齐，回执仍待核",
                })
                continue

            readable_dates.append(date)
            writes = self._backfill_writes.get((table, date), {})
            day_rows: list[dict[str, Any]] = []
            for order_id, write in sorted(writes.items()):
                commit = self._find_commit(table, date, order_id)
                row = {
                    "日期": date,
                    "订单号": order_id,
                    "金额": write["amount"],
                    "入账凭据": commit.credential_code if commit else "",
                    "order_id": order_id,
                    "amount": write["amount"],
                }
                day_rows.append(row)
            per_date_counts[date] = len(day_rows)
            rows.extend(day_rows)

        if plan is not None:
            plan.export_status = "已完成"
        return {
            "ok": True,
            "plan_code": plan.plan_code if plan else "",
            "target_wide_table": table,
            "requested_dates": sorted(set(requested)),
            "readable_dates": readable_dates,
            "effective_dates": readable_dates,
            "skipped_dates": skipped,
            "rows": rows,
            "exported_rows": len(rows),
            "by_date": per_date_counts,
            "receipts": receipts_used,
        }


    def create_cold_store_strategy(self, target_wide_table: str, hot_data_days: int,
                                   cold_storage_type: str = "parquet",
                                   compression_method: str = "zstd",
                                   migration_window: str = "02:00-06:00") -> ColdStoreStrategy:
        sid = self._next_seq()
        cs = ColdStoreStrategy(
            strategy_id=sid, strategy_code=_generate_code("COLD", sid),
            target_wide_table=target_wide_table,
            hot_data_days=hot_data_days,
            cold_storage_type=cold_storage_type,
            migration_window=migration_window,
            compression_method=compression_method,
            migrated_rows=0, last_migration="",
        )
        self._cold_stores[sid] = cs
        return cs

    # ---- 5. wide_table_def -> lineage_edge -> metric_aggr ----

    def create_lineage_edge(self, upstream_node: str, downstream_node: str,
                            related_fields: list[str], transform_function: str = "",
                            edge_type: str = "direct", dependency_level: int = 1) -> LineageEdge:
        sid = self._next_seq()
        edge = LineageEdge(
            edge_id=sid, edge_code=_generate_code("LINE", sid),
            upstream_node=upstream_node, downstream_node=downstream_node,
            related_fields=related_fields,
            transform_function=transform_function,
            edge_type=edge_type,
            dependency_level=dependency_level,
            last_traced=_now_iso(),
        )
        self._lineages[sid] = edge
        return edge

    def create_metric_aggr_rule(self, target_wide_table: str, agg_function: str,
                                group_dimensions: list[str], time_granularity: str = "1d",
                                precompute_window: str = "7d",
                                storage_table: str = "") -> MetricAggrRule:
        sid = self._next_seq()
        rule = MetricAggrRule(
            rule_id=sid, rule_code=_generate_code("METR", sid),
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
            time_granularity=time_granularity,
            precompute_window=precompute_window,
            storage_table=storage_table or f"aggr_{target_wide_table}",
            last_computed="",
        )
        self._metrics[sid] = rule
        return rule

    # ---- 6. partition_rule -> cold_store -> archive ----

    def create_partition_rule(self, target_wide_table: str, partition_type: str,
                              partition_field: str, retention_days: int = 30,
                              archive_policy: str = "move_to_cold") -> PartitionRule:
        sid = self._next_seq()
        rule = PartitionRule(
            rule_id=sid, strategy_code=_generate_code("PART", sid),
            target_wide_table=target_wide_table,
            partition_type=partition_type,
            partition_field=partition_field,
            retention_days=retention_days,
            archive_policy=archive_policy,
            partition_count=0,
            last_adjusted=_now_iso(),
        )
        self._partitions[sid] = rule
        return rule

    # ---- 7. snapshot_mgr -> export_snapshot ----

    def create_export_plan(self, target_wide_table: str, export_format: str = "parquet",
                           partition_filter: str = "", field_filter: list[str] | None = None,
                           export_path: str = "/exports", row_limit: int = 0) -> ExportPlan:
        sid = self._next_seq()
        plan = ExportPlan(
            plan_id=sid, plan_code=_generate_code("EXPO", sid),
            target_wide_table=target_wide_table,
            export_format=export_format,
            partition_filter=partition_filter,
            field_filter=field_filter or [],
            export_path=export_path,
            row_limit=row_limit,
            export_status="待导出",
        )
        self._exports[sid] = plan
        return plan

    # ---- 8. cross_validate -> data_drift -> fusion_monitor ----

    def create_cross_validate_plan(self, primary_wide_table: str,
                                   reference_wide_table: str, join_key: str,
                                   validate_fields: list[str],
                                   deviation_threshold: str = "0.05") -> CrossValidatePlan:
        sid = self._next_seq()
        plan = CrossValidatePlan(
            plan_id=sid, plan_code=_generate_code("CROS", sid),
            primary_wide_table=primary_wide_table,
            reference_wide_table=reference_wide_table,
            join_key=join_key,
            validate_fields=validate_fields,
            deviation_threshold=deviation_threshold,
            last_result="",
            validate_status="待校验",
        )
        self._cross_validates[sid] = plan
        return plan

    def create_drift_record(self, wide_table_name: str, drift_type: str,
                            historical_baseline: str, current_distribution: str,
                            kl_divergence: str = "0.0",
                            drift_direction: str = "none") -> DataDriftRecord:
        sid = self._next_seq()
        record = DataDriftRecord(
            record_id=sid, record_code=_generate_code("DATA", sid),
            wide_table_name=wide_table_name,
            drift_type=drift_type,
            historical_baseline=historical_baseline,
            current_distribution=current_distribution,
            kl_divergence=kl_divergence,
            drift_direction=drift_direction,
            detected_at=_now_iso(),
        )
        self._drifts[sid] = record
        return record

    def create_fusion_monitor(self, fusion_job: str, monitor_dimension: str,
                              collection_interval: str = "60s",
                              alert_threshold: str = "",
                              alert_channel: str = "webhook",
                              silence_window: str = "") -> FusionMonitorMetric:
        sid = self._next_seq()
        metric = FusionMonitorMetric(
            metric_id=sid, metric_code=_generate_code("FUSI", sid),
            fusion_job=fusion_job,
            monitor_dimension=monitor_dimension,
            collection_interval=collection_interval,
            alert_threshold=alert_threshold,
            alert_channel=alert_channel,
            silence_window=silence_window,
            last_sample=_now_iso(),
        )
        self._monitors[sid] = metric
        return metric

    # ---- 复合融合操作 ----

    def execute_fusion_pipeline(self, job_id: int) -> dict[str, Any]:
        """执行完整融合流水线：数据源读取 -> 映射转换 -> 冲突仲裁 -> 质量检查 -> 产出快照。

        返回值包含融合记录、冲突日志、质量报告和快照信息。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        raw_records: list[dict[str, Any]] = []
        for source_code in job.participant_sources:
            source = self._find_source_by_code(source_code)
            if source is None:
                continue
            source_records = self._simulate_source_read(source)
            for rec in source_records:
                rec["_source"] = source_code
                rec["_fusion_key"] = f"{source_code}#{rec.get(job.join_key, '')}"
                rec["_timestamp"] = _now_iso()
                rec["_version"] = 1
            raw_records.extend(source_records)

        mapped_records = self._apply_schema_mappings(raw_records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(raw_records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(raw_records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = conflict_result

        quality_result = self._apply_quality_gates(
            conflict_result.get("resolved", mapped_records),
            job.target_wide_table,
        )
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(conflict_result.get("resolved", mapped_records))
            result["snapshot"] = {"partition_code": snap.partition_code, "record_count": snap.record_count}

        return result

    def execute_delta_merge(self, job_id: int, delta_records: list[dict[str, Any]],
                            watermark_record_id: int = 0) -> dict[str, Any]:
        """执行增量归并：将增量数据合并到融合作业的输出中。"""
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        existing = {self._make_merge_key(r, job.join_key): r for r in job.merged_records}
        inserted = 0
        updated = 0
        deleted = 0

        for delta in delta_records:
            key = self._make_merge_key(delta, job.join_key)
            op = delta.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(delta)
                updated += 1
            else:
                existing[key] = dict(delta)
                inserted += 1

        job.merged_records = list(existing.values())

        if watermark_record_id > 0:
            wm = self._watermarks.get(watermark_record_id)
            if wm is not None:
                wm.previous_watermark = wm.current_watermark
                wm.current_watermark = _now_iso()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after_merge": len(job.merged_records),
        }

    def execute_backfill_with_checkpoint(self, backfill_id: int,
                                         source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行带断点续传的数据回填。

        从 checkpoint_position 处继续处理，失败时保留断点可恢复。
        """
        task = self._backfills.get(backfill_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}

        start_idx = 0
        if task.checkpoint_position:
            try:
                start_idx = int(task.checkpoint_position)
            except (ValueError, TypeError):
                start_idx = 0

        batch_size = max(task.parallel_partitions * 100, 100)
        end_idx = min(start_idx + batch_size, len(source_records))
        batch = source_records[start_idx:end_idx]

        processed = len(batch)
        task.processed_rows += processed
        task.remaining_rows = max(0, len(source_records) - end_idx)

        if end_idx < len(source_records):
            task.checkpoint_position = str(end_idx)
            complete = False
        else:
            task.checkpoint_position = ""
            complete = True

        return {
            "ok": True,
            "batch_processed": processed,
            "total_processed": task.processed_rows,
            "remaining": task.remaining_rows,
            "checkpoint": task.checkpoint_position,
            "complete": complete,
        }

    def migrate_to_cold_store(self, strategy_id: int,
                              hot_partition_cutoff: str) -> dict[str, Any]:
        """执行冷热分层迁移：将超过热数据保留期的分区迁移到冷存储。"""
        strategy = self._cold_stores.get(strategy_id)
        if strategy is None:
            return {"ok": False, "message": f"冷热分层策略 {strategy_id} 不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == strategy.target_wide_table
            and snap.partition_key_value < hot_partition_cutoff
        ]
        migrated = 0
        for snap in eligible_snaps:
            migrated += snap.record_count
            snap.status_mark = "已迁移至冷存储"

        strategy.migrated_rows += migrated
        strategy.last_migration = _now_iso()

        return {
            "ok": True,
            "migrated_partitions": len(eligible_snaps),
            "migrated_rows": migrated,
            "strategy_code": strategy.strategy_code,
        }

    def execute_export_from_snapshot(self, export_id: int,
                                     snapshot_id: int) -> dict[str, Any]:
        """从分区快照执行数据导出。"""
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}

        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        records = fusion_job.merged_records if fusion_job else []

        if plan.field_filter:
            records = [
                {k: v for k, v in rec.items() if k in plan.field_filter}
                for rec in records
            ]

        if plan.row_limit > 0 and len(records) > plan.row_limit:
            records = records[:plan.row_limit]

        plan.export_status = "已完成"

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "exported_rows": len(records),
            "format": plan.export_format,
            "path": plan.export_path,
            "records": records,
        }

    def execute_cross_validation(self, plan_id: int) -> dict[str, Any]:
        """执行跨宽表交叉校验，检测数据一致性偏差。"""
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_job = self._find_fusion_job_by_table(plan.primary_wide_table)
        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)

        primary_records = primary_job.merged_records if primary_job else []
        ref_records = ref_job.merged_records if ref_job else []

        primary_by_key = {str(r.get(plan.join_key, "")): r for r in primary_records}
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        drift_detected = False

        for key in set(primary_by_key.keys()) & set(ref_by_key.keys()):
            p = primary_by_key[key]
            r = ref_by_key[key]
            for field in plan.validate_fields:
                pv = str(p.get(field, ""))
                rv = str(r.get(field, ""))
                if pv != rv:
                    mismatches.append({
                        "key": key, "field": field,
                        "primary_value": pv, "reference_value": rv,
                    })
                    drift_detected = True

        plan.validate_status = "偏差告警" if mismatches else "一致"
        plan.last_result = json.dumps({
            "matched_keys": len(set(primary_by_key.keys()) & set(ref_by_key.keys())),
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
        }, ensure_ascii=False)

        result: dict[str, Any] = {
            "ok": True,
            "plan_code": plan.plan_code,
            "validate_status": plan.validate_status,
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
            "mismatches": mismatches[:20],
        }

        if drift_detected:
            drift_record = self.create_drift_record(
                wide_table_name=plan.primary_wide_table,
                drift_type="schema_mismatch",
                historical_baseline="baseline_" + _now_iso(),
                current_distribution="current_" + _now_iso(),
            )
            result["drift_record_code"] = drift_record.record_code

        return result

    def reconcile_partition_archive(self, partition_rule_id: int,
                                    cold_strategy_id: int) -> dict[str, Any]:
        """根据分区策略和冷热分层策略执行归档。

        将超出保留期的分区数据从冷存储转移到归档。
        """
        part_rule = self._partitions.get(partition_rule_id)
        cold_strat = self._cold_stores.get(cold_strategy_id)

        if part_rule is None or cold_strat is None:
            return {"ok": False, "message": "分区策略或冷热分层策略不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == part_rule.target_wide_table
            and snap.status_mark == "已迁移至冷存储"
        ]

        archived = 0
        for snap in eligible_snaps:
            snap.status_mark = "已归档"
            archived += snap.record_count

        return {
            "ok": True,
            "archived_partitions": len(eligible_snaps),
            "archived_rows": archived,
            "archive_policy": part_rule.archive_policy,
        }

    def reconcile_watermark_with_fusion(self, job_id: int) -> dict[str, Any]:
        """协调水位线与融合作业的推进状态。

        水位延迟时推进受限，融合作业未完成时增量归并暂停。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        related_watermarks = [
            wm for wm in self._watermarks.values()
            if wm.data_source in job.participant_sources
        ]

        stalled = [wm for wm in related_watermarks if wm.delay_minutes > 30]
        advancing = [wm for wm in related_watermarks if wm.delay_minutes <= 30]

        return {
            "ok": True,
            "job_code": job.job_code,
            "total_watermarks": len(related_watermarks),
            "advancing": len(advancing),
            "stalled": len(stalled),
            "can_proceed": len(stalled) == 0,
        }

    def build_wide_table_from_fusion(self, wide_table_id: int, fusion_job_id: int) -> dict[str, Any]:
        """从融合作业结果构建宽表定义，产生数据血缘边。"""
        wt = self._wide_tables.get(wide_table_id)
        job = self._fusion_jobs.get(fusion_job_id)

        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        if job is None:
            return {"ok": False, "message": f"融合作业 {fusion_job_id} 不存在"}

        wt.source_count = len(job.participant_sources)
        wt.last_updated = _now_iso()

        lineages: list[str] = []
        for source_code in job.participant_sources:
            edge = self.create_lineage_edge(
                upstream_node=source_code,
                downstream_node=wt.table_name,
                related_fields=wt.field_list,
                edge_type="fusion_map",
                dependency_level=1,
            )
            lineages.append(edge.edge_code)

        return {
            "ok": True,
            "table_code": wt.table_code,
            "source_count": wt.source_count,
            "lineage_edges": lineages,
            "last_updated": wt.last_updated,
        }

    def compute_metric_from_lineage(self, lineage_edge_id: int,
                                    target_wide_table: str,
                                    agg_function: str,
                                    group_dimensions: list[str]) -> dict[str, Any]:
        """基于数据血缘边计算指标聚合规则。"""
        edge = self._lineages.get(lineage_edge_id)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {lineage_edge_id} 不存在"}

        rule = self.create_metric_aggr_rule(
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
        )
        rule.last_computed = _now_iso()

        return {
            "ok": True,
            "metric_rule_code": rule.rule_code,
            "from_lineage": edge.edge_code,
            "agg_function": agg_function,
            "group_dimensions": group_dimensions,
        }

    # ---- 辅助方法 ----

    @staticmethod
    def _normalize_backfill_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """把补写明细统一成 {order_id, amount}，订单号取订单号或 order_id。"""
        normalized: list[dict[str, Any]] = []
        for row in rows:
            order_id = row.get("order_id", row.get("订单号", row.get("order", "")))
            amount = row.get("amount", row.get("金额", 0))
            order_id = str(order_id).strip()
            if not order_id:
                continue
            normalized.append({"order_id": order_id, "amount": float(amount)})
        return normalized

    def _write_backfill_partition(self, target_wide_table: str, date: str,
                                  rows: list[dict[str, Any]]) -> int:
        """把补写明细落到实际写入证据里；同一订单号只保留一行。"""
        writes = self._backfill_writes.setdefault((target_wide_table, date), {})
        before = len(writes)
        for row in rows:
            writes[row["order_id"]] = dict(row)
        return len(writes) - before

    def _partition_evidence(self, target_wide_table: str, date: str) -> dict[str, Any]:
        """分区提交记录与实际写入证据的逐单比对结果。"""
        commits = {
            record.order_id: record
            for record in self._partition_commits.values()
            if record.target_wide_table == target_wide_table
            and record.partition_key_value == date
            and record.committed
        }
        writes = self._backfill_writes.get((target_wide_table, date), {})
        matched = [
            order_id for order_id in writes
            if order_id in commits and float(commits[order_id].amount) == float(writes[order_id]["amount"])
        ]
        missing_commit = [order_id for order_id in writes if order_id not in commits]
        mismatched = [
            order_id for order_id in writes
            if order_id in commits and float(commits[order_id].amount) != float(writes[order_id]["amount"])
        ]
        return {
            "committed_rows": len(commits),
            "actual_rows": len(writes),
            "matched_rows": len(matched),
            "missing_commit_rows": len(missing_commit),
            "mismatched_rows": len(mismatched),
            "matched": matched,
            "missing_commit": missing_commit,
            "mismatched": mismatched,
        }

    def _find_commit(self, target_wide_table: str, date: str,
                     order_id: str) -> PartitionCommitRecord | None:
        for record in self._partition_commits.values():
            if (record.target_wide_table == target_wide_table
                    and record.partition_key_value == date
                    and record.order_id == str(order_id)
                    and record.committed):
                return record
        return None

    def _find_snapshot(self, target_wide_table: str, date: str) -> SnapshotPartition | None:
        for snap in self._snapshots.values():
            if snap.target_wide_table == target_wide_table and snap.partition_key_value == date:
                return snap
        return None

    def _find_backfill_by_code(self, task_code: str) -> BackfillTask | None:
        for task in self._backfills.values():
            if task.task_code == task_code:
                return task
        return None

    def _find_receipt(self, task_code: str | None, date: str,
                      target_wide_table: str = "") -> BackfillPartitionReceipt | None:
        candidates = [
            receipt for receipt in self._backfill_receipts.values()
            if receipt.partition_key_value == date
            and (not task_code or receipt.task_code == task_code)
            and (not target_wide_table or receipt.target_wide_table == target_wide_table)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda receipt: receipt.receipt_id)

    def _upsert_backfill_receipt(self, task: BackfillTask | None, date: str,
                                 submitted_rows: int, deliver: bool,
                                 target_wide_table: str = "") -> BackfillPartitionReceipt:
        table = target_wide_table or (task.target_wide_table if task else "")
        task_code = task.task_code if task else ""
        existing = self._find_receipt(task_code or None, date, table)
        if existing is not None:
            existing.submitted_rows = max(existing.submitted_rows, submitted_rows)
            existing.delivered = existing.delivered or deliver
            return existing

        sid = self._next_seq()
        receipt = BackfillPartitionReceipt(
            receipt_id=sid,
            receipt_code=_generate_code("BRCP", sid),
            task_code=task_code,
            target_wide_table=table,
            partition_key_value=date,
            submitted_rows=submitted_rows,
            issued_time=_now_iso(),
            delivered=deliver,
        )
        self._backfill_receipts[sid] = receipt
        return receipt

    def _refresh_backfill_receipt(self, receipt: BackfillPartitionReceipt) -> None:
        """用分区提交记录和实际写入证据重新核定回执的已提交/待核行数。

        新增行以分区提交记录为准；实际写入逐单核对：
        - 回执成功交付且逐单一致的行记入“已提交”；
        - 实际已写入但回执没交付、写入缺少入账凭据或金额对不上的行记入“待核”；
        - 分区表已入账但回填端还没写出来的行同样记入“待核”。
        """
        table, date = receipt.target_wide_table, receipt.partition_key_value
        evidence = self._partition_evidence(table, date)
        writes = self._backfill_writes.get((table, date), {})
        commits = {
            record.order_id: record
            for record in self._partition_commits.values()
            if record.target_wide_table == table
            and record.partition_key_value == date
            and record.committed
        }

        detail: list[dict[str, Any]] = []
        seen: set[str] = set()
        for order_id, write in writes.items():
            seen.add(order_id)
            commit = commits.get(order_id)
            matched = (
                commit is not None
                and float(commit.amount) == float(write["amount"])
            )
            detail.append({
                "order_id": order_id,
                "amount": float(write["amount"]),
                "committed": bool(matched and receipt.delivered),
                "written": True,
                "credential": commit.credential_code if commit else "",
            })
        for order_id, commit in commits.items():
            if order_id in seen:
                continue
            detail.append({
                "order_id": order_id,
                "amount": float(commit.amount),
                "committed": False,
                "written": False,
                "credential": commit.credential_code,
            })

        receipt.detail = detail
        receipt.actual_rows = evidence["actual_rows"]
        receipt.committed_rows = sum(1 for line in detail if line["committed"])
        receipt.pending_rows = len(detail) - receipt.committed_rows
        receipt.verified_time = _now_iso()

        if receipt.committed_rows > 0 and receipt.pending_rows == 0:
            receipt.status = "已提交"
            receipt.effective = True
            receipt.note = "实际写入与分区提交记录逐单一致"
        elif not writes and not commits:
            receipt.status = "待提交"
            receipt.effective = False
            receipt.note = "没有实际写入"
        else:
            receipt.status = "待核"
            receipt.effective = False
            if not receipt.delivered:
                receipt.note = "回执交接中断，实际写入已落盘但未交付下游"
            elif receipt.pending_rows:
                receipt.note = "实际写入与分区提交记录未对齐，需值班核验"
            else:
                receipt.note = "回执未交付"

    def add_backfill_rows(self, target_wide_table: str, date: str,
                          order_id: str, amount: float | str) -> PartitionCommitRecord:
        """便捷入口：同时登记分区提交记录和实际写入，订单号按所在日期入账。"""
        record = self.record_partition_commit(
            target_wide_table=target_wide_table,
            partition_key_value=date,
            order_id=order_id,
            amount=amount,
        )
        self._write_backfill_partition(
            target_wide_table, date,
            [{"order_id": str(order_id), "amount": float(amount)}],
        )
        return record

    def backfill_batch_snapshot(self, task_code: str) -> dict[str, Any]:
        """批量回填的逐日计数与交付证据汇总。"""
        task = self._find_backfill_by_code(task_code)
        receipts = [
            receipt for receipt in self._backfill_receipts.values()
            if receipt.task_code == task_code
        ]
        receipts.sort(key=lambda receipt: receipt.partition_key_value)
        per_date = []
        for receipt in receipts:
            self._refresh_backfill_receipt(receipt)
            per_date.append({
                "日期": receipt.partition_key_value,
                "已提交行": receipt.committed_rows,
                "待核行": receipt.pending_rows,
                "实际写入行": receipt.actual_rows,
                "回执状态": receipt.status,
                "实际生效": receipt.effective,
                "明细": [line["order_id"] for line in receipt.detail if line["committed"]],
            })
        return {
            "ok": task is not None,
            "task_code": task_code,
            "target_wide_table": task.target_wide_table if task else "",
            "submitted_count": task.submitted_count if task else 0,
            "dates": per_date,
            "total_committed": sum(item["已提交行"] for item in per_date),
            "total_pending": sum(item["待核行"] for item in per_date),
        }

    def _find_source_by_code(self, source_code: str) -> SourceDef | None:
        for source in self._sources.values():
            if source.source_code == source_code:
                return source
        return None

    def _find_fusion_job_by_table(self, wide_table_name: str) -> FusionJob | None:
        for job in self._fusion_jobs.values():
            if job.target_wide_table == wide_table_name:
                return job
        return None

    def _simulate_source_read(self, source: SourceDef) -> list[dict[str, Any]]:
        """模拟从数据源读取样本记录。"""
        return [
            {"id": f"{source.source_code}-rec-1", "name": f"{source.source_name}-rec1",
             "value": 100.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-2", "name": f"{source.source_name}-rec2",
             "value": 200.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-3", "name": f"{source.source_name}-rec3",
             "value": 300.0, "update_time": _now_iso()},
        ]

    def _apply_schema_mappings(self, records: list[dict[str, Any]],
                               target_wide_table: str) -> list[dict[str, Any]]:
        """应用 Schema 映射：对关联到目标宽表的映射方案进行字段转换。"""
        relevant = [
            m for m in self._schemas.values()
            if m.target_wide_table == target_wide_table
        ]
        if not relevant:
            return list(records)

        mapped: list[dict[str, Any]] = []
        for rec in records:
            source_table = rec.get("_source", "")
            applicable = [m for m in relevant if m.source_table == source_table]
            if applicable:
                new_rec = dict(rec)
                for mapping in applicable:
                    try:
                        field_map = json.loads(mapping.field_mapping_json)
                        for src_field, tgt_field in field_map.items():
                            if src_field in new_rec:
                                new_rec[tgt_field] = new_rec.pop(src_field)
                    except (json.JSONDecodeError, KeyError):
                        pass
                mapped.append(new_rec)
            else:
                mapped.append(rec)
        return mapped

    def _apply_conflict_resolution(self, records: list[dict[str, Any]],
                                   target_wide_table: str) -> dict[str, Any]:
        """应用冲突仲裁规则到融合记录。"""
        rules = [
            r for r in self._conflict_rules.values()
            if r.target_wide_table == target_wide_table and r.rule_status == "生效中"
        ]
        if not rules:
            return {"applied": False, "reason": "无生效的冲突规则", "resolved": records}

        resolved = list(records)
        all_conflicts: list[dict[str, Any]] = []
        for rule in rules:
            resolved, conflicts = rule.resolve(resolved)
            rule.conflict_count = len(conflicts)
            all_conflicts.extend(conflicts)

        return {
            "applied": True,
            "rules_used": len(rules),
            "conflicts_found": len(all_conflicts),
            "conflicts": all_conflicts[:50],
            "resolved": resolved,
        }

    def _apply_quality_gates(self, records: list[dict[str, Any]],
                             target_wide_table: str) -> dict[str, Any]:
        """应用质量门禁检查。"""
        gates = [
            g for g in self._quality_gates.values()
            if g.target_wide_table == target_wide_table
        ]
        if not gates:
            return {"passed": True, "gates_checked": 0, "violations": []}

        violations: list[dict[str, Any]] = []
        for gate in gates:
            gate.checked_rows = len(records)
            if gate.check_type == "row_count_min" and len(records) < int(gate.threshold_condition):
                violations.append({"rule_code": gate.rule_code, "check": "row_count_min",
                                   "actual": len(records), "threshold": gate.threshold_condition})
            elif gate.check_type == "null_check":
                null_count = sum(
                    1 for rec in records
                    if rec.get(gate.threshold_condition) is None
                )
                if null_count > 0:
                    violations.append({"rule_code": gate.rule_code, "check": "null_check",
                                       "null_count": null_count})

        passed = len(violations) == 0
        return {"passed": passed, "gates_checked": len(gates), "violations": violations}

    @staticmethod
    def _make_merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出引擎全局数据质量快照。"""
        return {
            "sources": len(self._sources),
            "schemas": len(self._schemas),
            "wide_tables": len(self._wide_tables),
            "fusion_jobs": len(self._fusion_jobs),
            "conflict_rules": len(self._conflict_rules),
            "quality_gates": len(self._quality_gates),
            "snapshots": len(self._snapshots),
            "backfills": len(self._backfills),
            "cold_stores": len(self._cold_stores),
            "lineages": len(self._lineages),
            "metrics": len(self._metrics),
            "partitions": len(self._partitions),
            "watermarks": len(self._watermarks),
            "exports": len(self._exports),
            "cross_validates": len(self._cross_validates),
            "drifts": len(self._drifts),
            "monitors": len(self._monitors),
        }


# 全局单例供测试和跨模块消费
fusion_engine = FusionEngine()