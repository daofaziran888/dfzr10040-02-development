"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

按日期批量回填是跨业务日的复合动作，单独挂在固定前缀下，
避免被 /api/{module} 的动态路径抢先匹配。
"""
from __future__ import annotations

from app.routers import backfill_batch, resource

ROUTERS = [backfill_batch, resource]
