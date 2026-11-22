"""Schema演进服务：演进记录即字段版本记录，承接版本交接核对。"""
from __future__ import annotations

from typing import Any

from app.services import field_version as fv
from app.services.base import BaseModuleService


class SchemaEvoService(BaseModuleService):
    MODULE = "schema_evo"
    ENTITY = "演进记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "数据流", "变更列", "列变更前", "列变更后", "兼容模式", "下游影响", "演进时间"]
    REQUIRED_FIELDS = ["记录编号", "数据流", "变更列"]
    STATUS_ORDER = ["已捕获", "兼容通过", "存在风险", "已阻断"]
    ACTION_RULES = {"提交校验": "兼容通过", "标记风险": "存在风险", "阻断演进": "已阻断"}
    NEGATIVE_ACTIONS = []

    def audit_version_handoff(self, source: str, table: str) -> dict[str, Any]:
        """核对某个 源端@影响表 的版本交接全貌。

        回答：已有批次能否继续、哪些数据以旧版产出、新版何时开始被转换消费。
        """
        return fv.audit_key(fv.version_key(source, table))
