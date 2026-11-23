"""验证补齐：缺回执日期的实际写入核验、只读取数不落回执。

覆盖“缺回执的日期须核验实际写入”和“取数入口不得凭空补出已交付回执”。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine

WIDE = "WT_ORDER_RECEIPT"


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


def test_missing_receipt_is_verified_and_stays_pending(engine: FusionEngine) -> None:
    """分区表已入账、实际写入已落盘，但回执丢失：核验后标成待核而不是已提交。"""
    date = "20261118"
    for order_id, amount in (("K", 33), ("A-18", 103)):
        engine.record_partition_commit(WIDE, date, order_id, amount,
                                       credential_code=f"CRED-{order_id}")
        engine._write_backfill_partition(
            WIDE, date, [{"order_id": order_id, "amount": float(amount)}])

    # 没有任何回执时导出：缺回执日期只核验实际写入，不进入可读分区。
    export = engine.export_readable_partitions(target_wide_table=WIDE, dates=[date])
    assert export["readable_dates"] == []
    assert export["skipped_dates"][0]["待核行"] == 2

    verified = engine.verify_backfill_receipts(target_wide_table=WIDE, dates=[date])
    assert verified["verified"] == 1
    receipt = verified["receipts"][0]
    assert receipt["回执状态"] == "待核"
    assert receipt["已提交行"] == "0"
    assert receipt["待核行"] == "2"

    # 只读取数（materialize_missing=False）不会补登回执。
    fresh = FusionEngine()
    fresh.record_partition_commit(WIDE, date, "K", 33)
    fresh._write_backfill_partition(WIDE, date, [{"order_id": "K", "amount": 33.0}])
    preview = fresh.export_readable_partitions(
        target_wide_table=WIDE, dates=[date], materialize_missing=False)
    assert preview["readable_dates"] == []
    assert fresh.list_backfill_receipts() == []


def test_write_without_credential_is_pending(engine: FusionEngine) -> None:
    """实际写入缺少入账凭据的行必须待核，不能算作已提交。"""
    date = "20261118"
    engine.record_partition_commit(WIDE, date, "K", 33)
    engine._write_backfill_partition(WIDE, date, [
        {"order_id": "K", "amount": 33.0},
        {"order_id": "X-9", "amount": 9.0},
    ])
    receipt = engine._upsert_backfill_receipt(None, date, submitted_rows=2, deliver=True,
                                              target_wide_table=WIDE)
    engine._refresh_backfill_receipt(receipt)
    assert receipt.committed_rows == 1
    assert receipt.pending_rows == 1
    assert receipt.status == "待核"
    assert receipt.effective is False
    assert {line["order_id"] for line in receipt.detail if not line["committed"]} == {"X-9"}


def test_amount_mismatch_is_pending(engine: FusionEngine) -> None:
    """金额与分区提交记录不一致的行不能交付。"""
    date = "20261118"
    engine.record_partition_commit(WIDE, date, "K", 33)
    engine._write_backfill_partition(WIDE, date, [{"order_id": "K", "amount": 34.0}])
    receipt = engine._upsert_backfill_receipt(None, date, submitted_rows=1, deliver=True,
                                              target_wide_table=WIDE)
    engine._refresh_backfill_receipt(receipt)
    assert receipt.committed_rows == 0
    assert receipt.pending_rows == 1
    assert receipt.effective is False
