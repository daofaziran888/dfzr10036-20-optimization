"""跨模块编排层：串联多模块数据流，对外暴露为可调用的端点。

每条链路含 N 个调用，覆盖完整的流变工坊CDC→湖仓全流程。
"""
from __future__ import annotations

from typing import Any

from app.services.cdc_source import CdcSourceService
from app.services.binlog_parse import BinlogParseService
from app.services.event_trans import EventTransService
from app.services.dim_join import DimJoinService
from app.services.watermark_mgr import WatermarkMgrService
from app.services.out_of_order import OutOfOrderService
from app.services.sink_writer import SinkWriterService
from app.services.lag_monitor import LagMonitorService
from app.services.checkpoint import CheckpointService
from app.services.iceberg_commit import IcebergCommitService
from app.services.compaction import CompactionService
from app.services.ddl_handle import DdlHandleService
from app.services.schema_evo import SchemaEvoService


cdc_source = CdcSourceService()
binlog_parse = BinlogParseService()
event_trans = EventTransService()
dim_join = DimJoinService()
watermark_mgr = WatermarkMgrService()
out_of_order = OutOfOrderService()
sink_writer = SinkWriterService()
lag_monitor = LagMonitorService()
checkpoint = CheckpointService()
iceberg_commit = IcebergCommitService()
compaction = CompactionService()
ddl_handle = DdlHandleService()
schema_evo = SchemaEvoService()


def flow_cdc_to_lake(cdc_entry_id: int) -> dict[str, Any]:
    """主线链路1: CDC源端→Binlog解析→事件转换→维表关联→水位→落盘→湖仓提交→小文件合并

    调用数: 8+
    """
    trace: list[str] = []

    # 1. CDC配置 → Binlog解析
    result = cdc_source.trigger_downstream_binlog(cdc_entry_id)
    if result is None:
        return {"ok": False, "step": "cdc->binlog", "message": "源端未建立连接"}
    trace.append(f"cdc_source#{cdc_entry_id} -> binlog_parse#{result['id']}")

    # 2. Binlog解析 → 事件转换
    trans = binlog_parse.trigger_downstream_event_trans(result["id"])
    if trans:
        trace.append(f"binlog_parse#{result['id']} -> event_trans#{trans['id']}")

        # 3. 事件转换 → 维表关联 + 去重
        dim_dedup = event_trans.trigger_dim_join_and_dedup(trans["id"])
        if dim_dedup and dim_dedup.get("dim_join"):
            trace.append(f"event_trans#{trans['id']} -> dim_join#{dim_dedup['dim_join']['id']}")

            # 4. 维表关联 → 水位 + Schema
            ws = dim_join.trigger_watermark_and_schema(dim_dedup["dim_join"]["id"])
            if ws and ws.get("watermark"):
                trace.append(f"dim_join#{dim_dedup['dim_join']['id']} -> watermark_mgr#{ws['watermark']['id']}")

                # 5. 水位提交 → checkpoint保存
                cp = watermark_mgr.trigger_checkpoint(ws["watermark"]["id"])
                if cp:
                    trace.append(f"watermark_mgr#{ws['watermark']['id']} -> checkpoint#{cp['id']}")

    # 6. 落盘 → iceberg提交
    sink_rows = [r for r in __import__("app.store", fromlist=["store"]).store.rows("sink_writer")
                 if r.get("status") == "写入中"]
    if sink_rows:
        sink_id = sink_rows[0]["id"]
        commit = sink_writer.trigger_iceberg_commit(sink_id)
        if commit:
            trace.append(f"sink_writer#{sink_id} -> iceberg_commit#{commit['id']}")

            # 7. iceberg提交 → compaction + expire
            ce = iceberg_commit.trigger_compaction_and_expire(commit["id"])
            if ce and ce.get("compaction"):
                trace.append(f"iceberg_commit#{commit['id']} -> compaction#{ce['compaction']['id']}")

                # 8. compaction → expire + metric
                em = compaction.trigger_expire_and_metric(ce["compaction"]["id"])
                if em and em.get("metric_aggr"):
                    trace.append(f"compaction#{ce['compaction']['id']} -> metric_aggr#{em['metric_aggr']['id']}")

    return {"ok": True, "trace": trace, "call_count": len(trace)}


def flow_lag_recovery(cdc_entry_id: int) -> dict[str, Any]:
    """主线链路2: 乱序处理→延迟监控→断点恢复→回填

    调用数: 6+
    """
    trace: list[str] = []
    store = __import__("app.store", fromlist=["store"]).store

    # 1. 查找乱序策略 → 延迟监控
    for ooo in store.rows("out_of_order"):
        if ooo.get("status") == "生效中":
            lm = out_of_order.trigger_lag_monitor_update(ooo["id"])
            if lm:
                trace.append(f"out_of_order#{ooo['id']} -> lag_monitor#{lm['id']}")

    # 2. 延迟监控 → 告警 + 健康
    for lm in store.rows("lag_monitor"):
        if lm.get("status") in ("严重滞后", "延迟偏高"):
            result = lag_monitor.escalate_to_alert_and_health(lm["id"])
            if result and result.get("alert"):
                trace.append(f"lag_monitor#{lm['id']} -> alert_rule#{result['alert']['id']}")
            if result and result.get("health"):
                trace.append(f"lag_monitor#{lm['id']} -> stream_health#{result['health']['id']}")

    # 3. ..checkpoint恢复 → 水位重置 + 回填
    for cp in store.rows("checkpoint"):
        if cp.get("status") == "已恢复" and int(cp.get("丢失行数", 0)) > 0:
            wr = checkpoint.trigger_watermark_reset_and_backfill(cp["id"])
            if wr and wr.get("backfill"):
                trace.append(f"checkpoint#{cp['id']} -> backfill_run#{wr['backfill']['id']}")
            if wr and wr.get("watermark"):
                trace.append(f"checkpoint#{cp['id']} -> watermark_mgr#{wr['watermark']['id']}")

    return {"ok": True, "trace": trace, "call_count": len(trace)}


def flow_ddl_propagation(ddl_entry_id: int) -> dict[str, Any]:
    """主线链路3: DDL适配→字段版本→Binlog调整→Schema演进（幂等可恢复）

    调用数: 4+
    """
    trace: list[str] = []

    # 1. DDL应用 → 字段版本双边落账（映射登载 + 演进登记，同一信封同一指纹）
    result = ddl_handle.trigger_binlog_and_schema_evo(ddl_entry_id)
    if result is None:
        return {"ok": False, "step": "ddl_handle", "message": "DDL未应用或不存在"}
    if result.get("outcome") == "conflict":
        return {"ok": False, "step": "field_version", "message": "版本指纹冲突，需人工核对"}

    version = result.get("version") or {}
    if result.get("binlog"):
        trace.append(
            f"ddl_handle#{ddl_entry_id} -> binlog_parse#{result['binlog']['id']}"
            f"（{version.get('字段版本', '?')}）"
        )
    if result.get("schema"):
        trace.append(
            f"ddl_handle#{ddl_entry_id} -> schema_evo#{result['schema']['id']}"
            f"（指纹{version.get('映射指纹', '?')}）"
        )

    return {
        "ok": True,
        "trace": trace,
        "call_count": len(trace),
        "outcome": result.get("outcome"),
        "version": version,
    }


def flow_parse_decode(rule_id: int, table: str | None = None) -> dict[str, Any]:
    """主线链路9: 解码批次→版本核验→同步交接事件转换→刷新转换批次

    调用数: 3+
    """
    trace: list[str] = []

    # 1. 开始解码批次：只消费一组可核验的完整映射
    result = binlog_parse.start_decode_batch(rule_id, table)
    if result is None:
        return {"ok": False, "step": "binlog_parse", "message": "规则不存在或未在解析中"}
    batch = result["batch"]
    trace.append(f"binlog_parse#{rule_id} 解码批次{batch['批次号']} 消费{batch['字段版本']}")

    # 2. 解析结果同步交接事件转换（解码依据已随批次附上）
    refresh = None
    if result.get("pipeline"):
        trace.append(
            f"binlog_parse#{rule_id} -> event_trans#{result['pipeline']['id']}"
            f"（{batch['转换批次']}）"
        )
        # 3. 刷新转换批次，核对实际采用的列
        refresh = event_trans.refresh_conversion_batch(result["pipeline"]["id"])

    return {"ok": True, "trace": trace, "call_count": len(trace),
            "batch": batch, "refresh": refresh}


def flow_ddl_recovery(ddl_entry_id: int) -> dict[str, Any]:
    """主线链路10: DDL传播中断恢复→补做演进写入/补登映射（不重复已确认版本）

    调用数: 2+
    """
    result = ddl_handle.recover_interrupted(ddl_entry_id)
    if result is None:
        return {"ok": False, "step": "ddl_handle", "message": "DDL未应用或不存在"}
    version = (result.get("result") or {}).get("version") or {}
    trace = [
        f"ddl_handle#{ddl_entry_id} 恢复动作={result['action']}"
        f"（{version.get('字段版本', '-')}）"
    ]
    return {"ok": True, "trace": trace, "call_count": len(trace), **result}


def flow_version_audit(source: str, table: str) -> dict[str, Any]:
    """主线链路11: 版本交接核对→已有批次能否继续/旧版产出/新版消费起点

    调用数: 2+
    """
    report = schema_evo.audit_version_handoff(source, table)
    trace = [f"schema_evo 核对{report['版本键']} 完整性{len(report['完整性'])}项"]
    return {"ok": True, "trace": trace, "call_count": len(trace), "report": report}


def flow_commit_failure_recovery(commit_entry_id: int) -> dict[str, Any]:
    """主线链路4: Iceberg提交冲突→数据比对→告警→回填修复

    调用数: 5+
    """
    trace: list[str] = []

    # 1. 提交冲突 → data_diff + alert
    result = iceberg_commit.handle_commit_conflict(commit_entry_id)
    if result is None:
        return {"ok": False, "step": "iceberg_commit", "message": "无提交冲突"}

    if result.get("data_diff"):
        trace.append(f"iceberg_commit#{commit_entry_id} -> data_diff#{result['data_diff']['id']}")
    if result.get("alert"):
        trace.append(f"iceberg_commit#{commit_entry_id} -> alert_rule#{result['alert']['id']}")

    return {"ok": True, "trace": trace, "call_count": len(trace)}


def flow_cache_invalidation(dim_entry_id: int) -> dict[str, Any]:
    """主线链路5: 维表缓存失效→告警→延迟指标

    调用数: 3+
    """
    trace: list[str] = []

    result = dim_join.handle_cache_invalidation(dim_entry_id)
    if result is None:
        return {"ok": False, "step": "dim_join", "message": "缓存未失效或不存在"}

    if result.get("alert"):
        trace.append(f"dim_join#{dim_entry_id} -> alert_rule#{result['alert']['id']}")
    if result.get("lag"):
        trace.append(f"dim_join#{dim_entry_id} -> lag_monitor#{result['lag']['id']}")

    return {"ok": True, "trace": trace, "call_count": len(trace)}


def flow_out_of_order_to_checkpoint(ooo_entry_id: int) -> dict[str, Any]:
    """主线链路6: 乱序超窗口→checkpoint保底+告警

    调用数: 4+
    """
    trace: list[str] = []

    result = out_of_order.handle_window_overflow(ooo_entry_id)
    if result is None:
        return {"ok": False, "step": "out_of_order", "message": "无超窗口丢弃"}

    if result.get("checkpoint"):
        trace.append(f"out_of_order#{ooo_entry_id} -> checkpoint#{result['checkpoint']['id']}")
    if result.get("alert"):
        trace.append(f"out_of_order#{ooo_entry_id} -> alert_rule#{result['alert']['id']}")

    return {"ok": True, "trace": trace, "call_count": len(trace)}


def flow_rate_propagation() -> dict[str, Any]:
    """主线链路7: 解析速率下降→事件过载→延迟上升

    调用数: 3+
    """
    trace: list[str] = []
    store = __import__("app.store", fromlist=["store"]).store

    for bp in store.rows("binlog_parse"):
        if bp.get("status") == "速率下降":
            binlog_parse.propagate_rate_adjustment()
            trace.append(f"binlog_parse#{bp['id']} -> event_trans(overload) + lag_monitor(delay)")

    return {"ok": True, "trace": trace, "call_count": len(trace)}


def flow_sink_block_to_compaction(sink_entry_id: int) -> dict[str, Any]:
    """主线链路8: 落盘阻塞→小文件合并清理

    调用数: 2+
    """
    trace: list[str] = []

    result = sink_writer.trigger_block_cleanup(sink_entry_id)
    if result:
        trace.append(f"sink_writer#{sink_entry_id} -> compaction#{result['id']}")

    return {"ok": True, "trace": trace, "call_count": len(trace)}


# 注册所有可编排的流链路
FLOWS: dict[str, Any] = {
    "cdc_to_lake": flow_cdc_to_lake,
    "lag_recovery": flow_lag_recovery,
    "ddl_propagation": flow_ddl_propagation,
    "commit_failure_recovery": flow_commit_failure_recovery,
    "cache_invalidation": flow_cache_invalidation,
    "out_of_order_to_checkpoint": flow_out_of_order_to_checkpoint,
    "rate_propagation": flow_rate_propagation,
    "sink_block_to_compaction": flow_sink_block_to_compaction,
    "parse_decode": flow_parse_decode,
    "ddl_recovery": flow_ddl_recovery,
    "version_audit": flow_version_audit,
}