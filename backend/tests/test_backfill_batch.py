"""按日期批量回填、分区回执与快照取数测试。

复现值班场景：同一宽表三个业务日一次提交，周一/周二已入账，
周三两单的入账凭据已记在分区表但回执交接中断；快照端与回填端
并发记录完成状态时以回执和实际写入为准。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine

WIDE = "WT_ORDER_BATCH"
RUNNING = "20261116"  # 周一
LOCKED = "20261117"   # 周二
INTERRUPTED = "20261118"  # 周三


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


def rows_by_date() -> dict[str, list[dict]]:
    return {
        RUNNING: [
            {"order_id": "K", "amount": 11},
            {"order_id": "A-16", "amount": 101},
        ],
        LOCKED: [
            {"order_id": "K", "amount": 22},
            {"order_id": "A-17", "amount": 102},
        ],
        INTERRUPTED: [
            {"order_id": "K", "amount": 33},
            {"order_id": "A-18", "amount": 103},
        ],
    }


def prepare(engine: FusionEngine, deliver=(RUNNING, LOCKED)):
    """周一/周二已入账并交付；周三凭据已在分区表，但回执交接中断。"""
    task = engine.create_backfill_task(
        target_wide_table=WIDE,
        backfill_range=f"{RUNNING}~{INTERRUPTED}",
        partition_dates=[RUNNING, LOCKED, INTERRUPTED],
    )
    rows = rows_by_date()
    for date, day_rows in rows.items():
        for row in day_rows:
            engine.record_partition_commit(
                WIDE, date, row["order_id"], row["amount"],
                credential_code=f"CRED-{date}-{row['order_id']}",
            )
    submitted = engine.submit_batch_backfill(
        task.task_id, rows, deliver_receipt_dates=list(deliver),
    )
    return task, submitted


class TestBatchBackfill:
    def test_only_one_submission_per_batch(self, engine: FusionEngine) -> None:
        task, submitted = prepare(engine)
        assert submitted["ok"] is True
        assert submitted["submitted_count"] == 1
        assert submitted["submitted_rows"] == 6

        again = engine.submit_batch_backfill(task.task_id, rows_by_date())
        assert again["ok"] is False
        assert again["duplicate_submission"] is True
        assert task.submitted_count == 1

    def test_per_date_counts_match_detail(self, engine: FusionEngine) -> None:
        _, submitted = prepare(engine)
        summary = engine.backfill_batch_snapshot(submitted["task_code"])
        assert len(summary["dates"]) == 3
        for item in summary["dates"]:
            assert item["实际写入行"] == 2
            assert len(item["明细"]) == (2 if item["实际生效"] else 0)

    def test_same_order_number_booked_per_date(self, engine: FusionEngine) -> None:
        _, submitted = prepare(engine)
        export = engine.export_readable_partitions(
            target_wide_table=WIDE, dates=[RUNNING, LOCKED])
        k_rows = [row for row in export["rows"] if row["订单号"] == "K"]
        assert len(k_rows) == 2
        assert sorted(row["金额"] for row in k_rows) == [11, 22]
        assert {row["日期"] for row in k_rows} == {RUNNING, LOCKED}

    def test_interrupted_receipt_stays_pending(self, engine: FusionEngine) -> None:
        _, submitted = prepare(engine)
        by_date = {item["分区键值"]: item for item in submitted["receipts"]}
        assert by_date[RUNNING]["回执状态"] == "已提交"
        assert by_date[INTERRUPTED]["回执状态"] == "待核"
        assert by_date[INTERRUPTED]["已提交行"] == "0"
        assert by_date[INTERRUPTED]["待核行"] == "2"

    def test_pending_list_excludes_delivered(self, engine: FusionEngine) -> None:
        _, submitted = prepare(engine)
        pending = engine.pending_backfill_partitions(submitted["task_code"])
        assert pending["count"] == 1
        only = pending["dates"][0]
        assert only["分区键值"] == INTERRUPTED
        assert only["待核行"] == 2

    def test_snapshot_conflict_and_effective_dates(self, engine: FusionEngine) -> None:
        _, submitted = prepare(engine)
        confirmed = engine.confirm_snapshot_from_backfill(
            WIDE,
            dates=[RUNNING, LOCKED, INTERRUPTED],
            snapshot_statuses={RUNNING: "已完成", LOCKED: "已完成", INTERRUPTED: "已完成"},
            task_code=submitted["task_code"],
        )
        assert confirmed["effective_dates"] == [RUNNING, LOCKED]
        assert [item["分区键值"] for item in confirmed["pending_dates"]] == [INTERRUPTED]
        assert confirmed["conflict_dates"] == [INTERRUPTED]

        snaps = {
            snap.partition_key_value: snap
            for snap in engine._snapshots.values()
            if snap.target_wide_table == WIDE
        }
        assert snaps[RUNNING].status_mark == "可用"
        assert snaps[RUNNING].backfill_effective_dates == [RUNNING, LOCKED]
        assert snaps[INTERRUPTED].status_mark == "创建中"
        assert snaps[INTERRUPTED].backfill_pending_rows == 2

    def test_export_skips_unverified_date(self, engine: FusionEngine) -> None:
        _, submitted = prepare(engine)
        plan = engine.create_export_plan(
            target_wide_table=WIDE, export_format="parquet",
            partition_filter=f"{RUNNING},{LOCKED},{INTERRUPTED}",
        )
        export = engine.export_readable_partitions(plan.plan_id)
        assert export["readable_dates"] == [RUNNING, LOCKED]
        assert export["exported_rows"] == 4
        assert [item["分区键值"] for item in export["skipped_dates"]] == [INTERRUPTED]
        assert export["skipped_dates"][0]["待核行"] == 2
        assert all(row["日期"] in (RUNNING, LOCKED) for row in export["rows"])

    def test_retry_is_idempotent_and_completes(self, engine: FusionEngine) -> None:
        _, submitted = prepare(engine)
        first = engine.retry_backfill_partition(submitted["task_code"], INTERRUPTED)
        assert first["ok"] is True
        assert first["rows_added"] == 0
        assert first["duplicates_skipped"] == 2
        assert first["effective"] is True
        assert first["committed_rows"] == 2
        assert first["pending_rows"] == 0

        second = engine.retry_backfill_partition(submitted["task_code"], INTERRUPTED)
        assert second["rows_added"] == 0

        summary = engine.backfill_batch_snapshot(submitted["task_code"])
        assert summary["total_committed"] == 6
        assert summary["total_pending"] == 0
        assert all(item["实际写入行"] == 2 for item in summary["dates"])
        assert engine.pending_backfill_partitions(submitted["task_code"])["count"] == 0

        reconciled = engine.confirm_snapshot_from_backfill(
            WIDE, dates=[RUNNING, LOCKED, INTERRUPTED],
            snapshot_statuses={RUNNING: "已完成", LOCKED: "已完成", INTERRUPTED: "已完成"},
            task_code=submitted["task_code"],
        )
        assert reconciled["effective_dates"] == [RUNNING, LOCKED, INTERRUPTED]
        assert reconciled["conflict_dates"] == []

        export = engine.export_readable_partitions(
            target_wide_table=WIDE, dates=[RUNNING, LOCKED, INTERRUPTED])
        assert export["exported_rows"] == 6
        assert sorted(row["金额"] for row in export["rows"] if row["订单号"] == "K") == [11, 22, 33]
