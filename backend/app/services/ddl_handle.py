"""DDL适配服务：DDL变更以可核验的字段版本数据级联生效到解析与演进。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services import field_version as fv
from app.services.base import BaseModuleService
from app.store import store


class DdlHandleService(BaseModuleService):
    MODULE = "ddl_handle"
    ENTITY = "DDL适配"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["适配编号", "源端", "DDL类型", "影响表", "兼容策略", "自动应用", "人工确认", "处理状态"]
    REQUIRED_FIELDS = ["适配编号", "源端", "DDL类型"]
    STATUS_ORDER = ["待处理", "适配中", "已应用", "需人工"]
    ACTION_RULES = {"自动应用": "适配中", "人工确认": "已应用", "忽略变更": "需人工"}
    NEGATIVE_ACTIONS = ["忽略变更"]

    # ---------- 字段版本交接 ----------

    def _entry_key(self, entry: dict[str, Any]) -> tuple[str, str, str]:
        source = str(entry.get("源端", ""))
        table = str(entry.get("影响表", ""))
        return source, table, fv.version_key(source, table)

    def _ddl_ref(self, entry: dict[str, Any]) -> str:
        """同一DDL事件的幂等键：重新投递/中断恢复都凭它找到已确认版本。"""
        return str(entry.get("适配编号") or f"DDL适配#{entry.get('id')}")

    def _rule_for(self, source: str) -> dict[str, Any] | None:
        for bp in store.rows("binlog_parse"):
            if str(bp.get("源端", "")) == source:
                return bp
        return None

    def _derive_columns(self, base_cols: str, ddl_type: str, version_no: int) -> str:
        """在最近一次已确认列清单上派生新列清单（演示环境按DDL类型确定性推导）。"""
        cols = [c for c in str(base_cols or "").split(",") if c]
        upper = ddl_type.upper()
        if "ADD" in upper and "COLUMN" in upper:
            cols.append(f"ext_col_v{version_no}")
        elif "DROP" in upper and "COLUMN" in upper and cols:
            cols = cols[:-1]
        return ",".join(cols)

    def _next_envelope(self, entry: dict[str, Any], source: str, table: str, key: str) -> dict[str, Any]:
        rule = self._rule_for(source)
        existing = fv.records_for(key)
        staged_nos = list(fv.staged(rule, key)) if rule is not None else []
        next_no = max([int(r["版本号"]) for r in existing] + [int(n) for n in staged_nos] + [0]) + 1
        if existing:
            latest = max(existing, key=lambda r: int(r["版本号"]))
            base_cols = str(latest.get("列清单") or fv.base_columns(source, table))
        else:
            base_cols = fv.base_columns(source, table)
        columns = self._derive_columns(base_cols, str(entry.get("DDL类型", "")), next_no)
        return fv.make_envelope(
            source, table, next_no, columns,
            self._ddl_ref(entry), f"DDL变更-{entry.get('DDL类型', '')}",
        )

    def stage_field_mapping(self, entry_id: int) -> dict[str, Any] | None:
        """只写映射侧：把版本信封登载到解析规则（登载≠生效，指针不动）。

        幂等：同一来源适配已登载就直接复用；演进侧已登记则按记录信封对齐。
        单独暴露用于故障演练（模拟"只写入映射就中断"）与恢复核对。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        source, table, key = self._entry_key(entry)
        rule = self._rule_for(source)
        if rule is None:
            return None
        ref = self._ddl_ref(entry)
        existing = fv.staged_by_ref(rule, key, ref)
        if existing is not None:
            return existing
        record = fv.find_record_by_ref(key, ref)
        env = fv.record_envelope(record) if record is not None else self._next_envelope(entry, source, table, key)
        fv.stage_envelope(rule, env)
        return env

    def register_evolution_record(self, entry_id: int) -> dict[str, Any] | None:
        """只登记演进记录侧：同一来源适配已确认的记录直接复用，不重复产生版本。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        source, table, key = self._entry_key(entry)
        ref = self._ddl_ref(entry)
        found = fv.find_record_by_ref(key, ref)
        if found is not None:
            return found
        rule = self._rule_for(source)
        env = fv.staged_by_ref(rule, key, ref) if rule is not None else None
        if env is None:
            env = self._next_envelope(entry, source, table, key)
        existing = fv.records_for(key)
        if existing:
            latest = max(existing, key=lambda r: int(r["版本号"]))
            prev_cols = str(latest.get("列清单") or fv.base_columns(source, table))
        else:
            prev_cols = fv.base_columns(source, table)
        rows = store.rows("schema_evo")
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        record = {
            "id": new_id,
            "记录编号": f"SE-{new_id:04d}",
            "数据流": f"{table}_cdc",
            "变更列": env["变更列"],
            "列变更前": prev_cols or "（无历史列清单）",
            "列变更后": env["列清单"],
            "兼容模式": str(entry.get("兼容策略", "向前兼容")),
            "下游影响": f"影响表{table}及下游消费",
            "演进时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "已捕获",
            "pending": True,
            "abnormal": False,
            **env,
        }
        rows.append(record)
        return record

    def trigger_binlog_and_schema_evo(self, entry_id: int) -> dict[str, Any] | None:
        """DDL适配成功后，以字段版本数据级联调整解析映射并登记演进记录。

        数据流链路: ddl_handle -> binlog_parse + schema_evo

        稳定性约定：
        - 同一DDL事件重复投递/中断恢复时幂等，已确认版本不重复产生；
        - 映射与演进两侧持久化同一信封，指纹一致后版本才对外生效；
        - 任一侧缺失都能按另一侧持久化的信封补写（见 recover_interrupted）。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已应用":
            return None
        source, table, key = self._entry_key(entry)
        ref = self._ddl_ref(entry)
        rule = self._rule_for(source)
        pre = fv.diagnose(rule, key, ref)
        if pre == "conflict":
            return {"binlog": rule, "schema": None, "version": None,
                    "outcome": "conflict", "pre_state": pre}

        env = self.stage_field_mapping(entry_id)
        record = self.register_evolution_record(entry_id)
        version = fv.record_envelope(record) if record is not None else env
        if rule is not None and version is not None:
            fv.stage_envelope(rule, version)
            fv.activate(rule, key, int(version["版本号"]))
            rule["DDL处理"] = str(entry.get("DDL类型", ""))
            rule["字段映射"] = f"版本化映射@{version['字段版本']}"
            rule["status"] = "解析中"
            rule["pending"] = True
            rule["abnormal"] = False

        outcome = {
            "complete": "reused",
            "mapping_only": "recovered",
            "record_only": "repaired",
            "none": "created",
        }[pre]
        return {"binlog": rule, "schema": record, "version": version,
                "outcome": outcome, "pre_state": pre}

    def recover_interrupted(self, entry_id: int) -> dict[str, Any] | None:
        """事务中断恢复：补做缺失一侧的写入，已确认版本不重复产生。

        - mapping_only（只写入映射就中断）→ 按映射侧信封补做演进写入；
        - record_only（只登记演进就中断）→ 按记录信封补登字段映射；
        - complete（两边已对齐）→ 直接复用，不新增版本。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已应用":
            return None
        source, table, key = self._entry_key(entry)
        rule = self._rule_for(source)
        pre = fv.diagnose(rule, key, self._ddl_ref(entry))
        result = self.trigger_binlog_and_schema_evo(entry_id)
        action = {
            "mapping_only": "补做演进写入",
            "record_only": "补登字段映射",
            "complete": "已确认-无需恢复",
            "none": "无半成品-按新变更应用",
            "conflict": "指纹冲突-需人工",
        }[pre]
        return {"action": action, "pre_state": pre, "key": key, "result": result}
