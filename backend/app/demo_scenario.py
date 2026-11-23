"""值班演示场景：同一宽表三个业务日一次提交，周三回执交接中断。

只在服务启动时装载到全局融合引擎，供页面和接口演示；测试使用独立
FusionEngine 实例，不受这里的示例数据影响。
"""
from __future__ import annotations

from typing import Any

from app.fusion_engine import fusion_engine

WIDE_TABLE = "订单宽表_值班回填"
MONDAY = "20261116"
TUESDAY = "20261117"
WEDNESDAY = "20261118"
DATES = [MONDAY, TUESDAY, WEDNESDAY]

# 每个业务日补两行，各日都有订单 K，金额依次为 11、22、33。
DEMO_ROWS: dict[str, list[dict[str, Any]]] = {
    MONDAY: [
        {"order_id": "K", "amount": 11},
        {"order_id": "A-1016", "amount": 101},
    ],
    TUESDAY: [
        {"order_id": "K", "amount": 22},
        {"order_id": "A-1017", "amount": 102},
    ],
    WEDNESDAY: [
        {"order_id": "K", "amount": 33},
        {"order_id": "A-1018", "amount": 103},
    ],
}


def load_demo_scenario() -> dict[str, Any]:
    """装载值班场景：周一/周二已入账，周三凭据在分区表但回执中断。"""
    if getattr(load_demo_scenario, "_loaded", False):
        return {"ok": True, "skipped": True}

    # 1. 分区提交记录：三个业务日逐单入账（周三两单的凭据已在分区表）。
    for date, rows in DEMO_ROWS.items():
        for row in rows:
            fusion_engine.record_partition_commit(
                target_wide_table=WIDE_TABLE,
                partition_key_value=date,
                order_id=row["order_id"],
                amount=row["amount"],
                credential_code=f"CRED-{date}-{row['order_id']}",
            )

    # 2. 值班一次提交三个业务日：周一/周二回执交付，周三回执交接中断。
    task = fusion_engine.create_backfill_task(
        target_wide_table=WIDE_TABLE,
        backfill_range=f"{MONDAY}~{WEDNESDAY}",
        partition_dates=DATES,
    )
    submitted = fusion_engine.submit_batch_backfill(
        task.task_id, DEMO_ROWS,
        deliver_receipt_dates=[MONDAY, TUESDAY],
    )

    # 3. 快照端并发把三个日期都记成已完成，与回填端状态冲突，按回执核定。
    confirmed = fusion_engine.confirm_snapshot_from_backfill(
        target_wide_table=WIDE_TABLE,
        dates=DATES,
        snapshot_statuses={date: "已完成" for date in DATES},
        task_code=task.task_code,
    )

    # 4. 回填端未恢复时，从快照导出申请三个业务日，只能读到已完整提交的分区。
    plan = fusion_engine.create_export_plan(
        target_wide_table=WIDE_TABLE,
        export_format="parquet",
        partition_filter=",".join(DATES),
    )

    load_demo_scenario._loaded = True
    return {
        "ok": True,
        "task_code": task.task_code,
        "submitted": submitted,
        "confirmed": confirmed,
        "export_plan_id": plan.plan_id,
    }
