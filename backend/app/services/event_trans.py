"""事件转换服务：转换完成后触发维表关联和Dedup去重。"""
from __future__ import annotations

from typing import Any

from app.services import field_version as fv
from app.services.base import BaseModuleService
from app.store import store


class EventTransService(BaseModuleService):
    MODULE = "event_trans"
    ENTITY = "转换管道"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["管道编号", "源端", "事件类型", "转换函数", "输出格式", "失败策略", "转换顺序", "最近吞吐"]
    REQUIRED_FIELDS = ["管道编号", "源端", "事件类型"]
    STATUS_ORDER = ["草稿", "转换中", "转换过载", "已停止"]
    ACTION_RULES = {"启动转换": "转换中", "扩容处理": "转换过载", "停止管道": "已停止"}
    NEGATIVE_ACTIONS = []

    def refresh_conversion_batch(self, entry_id: int) -> dict[str, Any] | None:
        """刷新转换批次：核对实际采用的列与解码依据，并检查解析侧是否已有更新版本。

        返回当前转换批次实际采用的列、对应的解码批次与字段版本；
        若解析侧已生效更新版本而本管道尚未消费，一并给出新版首个消费批次。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        source = str(entry.get("源端", ""))
        table = str(entry.get("影响表") or fv.table_for_source(source))
        key = fv.version_key(source, table)
        rule = next(
            (r for r in store.rows("binlog_parse") if str(r.get("源端", "")) == source),
            None,
        )
        effective_id = None
        if rule is not None:
            env, _ = fv.resolve_effective(rule, key)
            effective_id = str(env["字段版本"])

        adopted_id = str(entry.get("字段版本", ""))
        newer_batches = []
        if rule is not None and effective_id:
            newer_batches = [
                b for b in rule.get("解码批次", [])
                if str(b.get("影响表", "")) == table
                and str(b.get("字段版本", "")) == effective_id
                and b.get("状态") == "已交接"
            ]
        return {
            "管道编号": entry.get("管道编号", ""),
            "转换批次": entry.get("转换批次", "尚未交接"),
            "采用列": entry.get("采用列", ""),
            "解码依据": entry.get("最近解码依据", ""),
            "字段版本": adopted_id or "（未消费版本数据）",
            "解析侧生效版本": effective_id,
            "有新版待消费": bool(effective_id) and adopted_id != effective_id,
            "新版首个消费批次": newer_batches[0]["批次号"] if newer_batches else None,
            "批次历史": entry.get("批次历史", []),
        }

    def trigger_dim_join_and_dedup(self, entry_id: int) -> dict[str, Any]:
        """事件转换管道启动后，级联创建维表关联和去重窗口。

        数据流链路: event_trans -> dim_join + dedup_stream
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "转换中":
            return None

        tag = str(entry.get("源端", ""))
        results: dict[str, Any] = {"dim_join": None, "dedup": None}

        # 创建/恢复 dim_join 维表关联
        dim_rows = store.rows("dim_join")
        has_dim = [r for r in dim_rows if str(r.get("事实表", "")) == tag]
        if has_dim:
            has_dim[0]["status"] = "关联中"
            has_dim[0]["pending"] = True
            has_dim[0]["abnormal"] = False
            has_dim[0]["命中率"] = "87.3%"
            results["dim_join"] = has_dim[0]
        else:
            new_dim_id = max((int(r.get("id", 0)) for r in dim_rows), default=0) + 1
            dim_config = {
                "id": new_dim_id,
                "配置编号": f"DIM_-{new_dim_id:04d}",
                "事实表": tag,
                "维度表": "dim_customer_profile",
                "关联键": "customer_id",
                "关联类型": "LEFT JOIN",
                "缺失策略": "填充默认值",
                "缓存TTL": "600s",
                "命中率": "87.3%",
                "status": "关联中",
                "pending": True,
                "abnormal": False,
            }
            dim_rows.append(dim_config)
            results["dim_join"] = dim_config

        # 创建/恢复 dedup_stream 去重窗口
        dedup_rows = store.rows("dedup_stream")
        has_dedup = [r for r in dedup_rows if str(r.get("数据流", "")) == tag]
        if has_dedup:
            has_dedup[0]["status"] = "去重中"
            has_dedup[0]["pending"] = True
            has_dedup[0]["abnormal"] = False
            results["dedup"] = has_dedup[0]
        else:
            new_dedup_id = max((int(r.get("id", 0)) for r in dedup_rows), default=0) + 1
            dedup_config = {
                "id": new_dedup_id,
                "窗口编号": f"DEDU-{new_dedup_id:04d}",
                "数据流": tag,
                "去重键": "event_id+timestamp",
                "窗口宽度": "10min",
                "去重算法": "BloomFilter",
                "误杀率": "0.01%",
                "内存占用": "128MB",
                "最近命中": "1420次/h",
                "status": "去重中",
                "pending": True,
                "abnormal": False,
            }
            dedup_rows.append(dedup_config)
            results["dedup"] = dedup_config

        return results