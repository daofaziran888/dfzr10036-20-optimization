"""字段版本登记与核验：DDL列变化以"可核验的字段版本数据"在解析侧生效。

背景：旧链路先改 binlog_parse 字段映射、再登记 schema_evo 演进记录，
中途失败会留下"只有映射、没有演进"的半成品，解析端无法判断映射是否可信，
重新投递或恢复时又容易把同一变更重复登记。

约定：
- 每个字段版本用「版本键 = 源端@影响表」追溯同一条源端+目标表血缘，
  其他表的演进记录不会进入本键的版本序列；
- 映射侧（binlog_parse.版本映射）与演进侧（schema_evo 记录）持久化同一份
  版本信封（envelope），两侧指纹一致才算"完整可消费"；
- 指纹 = sha1(版本键|版本号|列清单|来源适配) 前12位，任一侧都可凭自己
  持久化的字段重算核验；中断恢复按同一信封补写缺失一侧，
  不会重复产生已确认的版本；
- 开始解码的批次只消费"完整可消费"的版本，半成品映射一律回退到
  最近一次已确认版本（或基线列）。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.store import store

BASELINE_REF = "基线映射"
LEGACY_REF = "历史演进"

ENVELOPE_FIELDS = [
    "版本键", "源端", "影响表", "版本号", "字段版本",
    "列清单", "变更列", "来源适配", "映射指纹",
]


# ---------- 版本标识与指纹 ----------

def version_key(source: str, table: str) -> str:
    """版本键：同一源端+同一张目标表共享一条版本序列。"""
    return f"{source}@{table}"


def version_id(key: str, version_no: int) -> str:
    return f"{key}#v{version_no}"


def fingerprint(key: str, version_no: int, columns: str, ddl_ref: str) -> str:
    raw = f"{key}|{version_no}|{columns}|{ddl_ref}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def make_envelope(
    source: str,
    table: str,
    version_no: int,
    columns: str,
    ddl_ref: str,
    change_note: str,
) -> dict[str, Any]:
    """构造版本信封：映射侧与演进侧持久化的是同一份数据。"""
    key = version_key(source, table)
    no = int(version_no)
    return {
        "版本键": key,
        "源端": source,
        "影响表": table,
        "版本号": no,
        "字段版本": version_id(key, no),
        "列清单": columns,
        "变更列": change_note,
        "来源适配": ddl_ref,
        "映射指纹": fingerprint(key, no, columns, ddl_ref),
    }


# ---------- 源端/表反查与基线列 ----------

def table_for_source(source: str) -> str:
    for row in store.rows("cdc_source"):
        if str(row.get("源端编号", "")) == source:
            return str(row.get("监听表", ""))
    return ""


def source_for_table(table: str) -> str:
    for row in store.rows("cdc_source"):
        if str(row.get("监听表", "")) == table:
            return str(row.get("源端编号", ""))
    return ""


def base_columns(source: str, table: str) -> str:
    for row in store.rows("cdc_source"):
        if str(row.get("源端编号", "")) == source and str(row.get("监听表", "")) == table:
            return str(row.get("包含列", ""))
    return ""


def baseline_envelope(key: str) -> dict[str, Any]:
    """基线版本v0：没有任何已确认演进时，批次消费源端包含列。"""
    source, table = key.split("@", 1)
    return make_envelope(source, table, 0, base_columns(source, table), BASELINE_REF, "基线列")


# ---------- 演进记录侧（schema_evo） -------- 读取与历史补登 ----------

def backfill_legacy_records() -> None:
    """给没有版本信封的历史演进记录补登信封（幂等）。

    历史记录只有「数据流」没有源端/表：按 表名_cdc 约定反查 cdc_source，
    同一版本键内按 id 顺序补版本号，多次调用结果一致。
    """
    rows = store.rows("schema_evo")
    pending = [r for r in rows if not r.get("版本键")]
    if not pending:
        return
    groups: dict[str, list[tuple[dict[str, Any], str, str]]] = {}
    for rec in pending:
        stream = str(rec.get("数据流", ""))
        table = stream[:-4] if stream.endswith("_cdc") else stream
        source = source_for_table(table) or stream
        groups.setdefault(version_key(source, table), []).append((rec, source, table))
    for key, items in groups.items():
        used = {int(r.get("版本号", 0)) for r in rows if r.get("版本键") == key}
        no = 1
        for rec, source, table in sorted(items, key=lambda x: int(x[0].get("id", 0))):
            while no in used:
                no += 1
            rec.update(make_envelope(
                source, table, no, base_columns(source, table),
                LEGACY_REF, str(rec.get("变更列", "")),
            ))
            used.add(no)
            no += 1


def records_for(key: str) -> list[dict[str, Any]]:
    """同一版本键下的全部演进记录；其他表的记录不会混入本键。"""
    backfill_legacy_records()
    return [r for r in store.rows("schema_evo") if r.get("版本键") == key]


def find_record(key: str, version_no: int) -> dict[str, Any] | None:
    for rec in records_for(key):
        if int(rec.get("版本号", -1)) == int(version_no):
            return rec
    return None


def find_record_by_ref(key: str, ddl_ref: str) -> dict[str, Any] | None:
    """按来源适配单号定位记录：同一DDL事件重新投递时据此幂等复用。"""
    for rec in records_for(key):
        if str(rec.get("来源适配", "")) == ddl_ref:
            return rec
    return None


def record_envelope(rec: dict[str, Any]) -> dict[str, Any]:
    env = {field: rec.get(field, "") for field in ENVELOPE_FIELDS}
    env["版本号"] = int(rec.get("版本号", 0))
    return env


# ---------- 映射侧（binlog_parse.版本映射）登载与生效指针 ----------

def staged(rule: dict[str, Any], key: str) -> dict[int, dict[str, Any]]:
    """解析规则本地登载的版本信封：{版本号: 信封}。"""
    mapping = rule.setdefault("版本映射", {})
    return mapping.setdefault(key, {})


def staged_by_ref(rule: dict[str, Any] | None, key: str, ddl_ref: str) -> dict[str, Any] | None:
    if rule is None:
        return None
    for env in staged(rule, key).values():
        if str(env.get("来源适配", "")) == ddl_ref:
            return env
    return None


def stage_envelope(rule: dict[str, Any], envelope: dict[str, Any]) -> None:
    staged(rule, str(envelope["版本键"]))[int(envelope["版本号"])] = dict(envelope)


def effective_versions(rule: dict[str, Any]) -> dict[str, int]:
    """生效指针：只有双边核验通过的版本才会被指针引用。"""
    return rule.setdefault("生效版本", {})


def mirror_display(rule: dict[str, Any], key: str) -> None:
    """把当前生效版本镜像到规则平铺字段，便于列表/详情直接查看。"""
    eff = effective_versions(rule).get(key)
    if eff is None:
        return
    env = staged(rule, key).get(eff)
    if env is None:
        return
    rule["版本键"] = key
    rule["字段版本"] = env["字段版本"]
    rule["映射指纹"] = env["映射指纹"]
    rule["映射列"] = env["列清单"]


def activate(rule: dict[str, Any], key: str, version_no: int) -> None:
    effective_versions(rule)[key] = int(version_no)
    mirror_display(rule, key)


# ---------- 核验：两边持久化的来源要对得上 ----------

def check_complete(key: str, envelope: dict[str, Any]) -> tuple[bool, str]:
    """核验一组字段版本数据是否"完整可消费"。

    完整 = 演进记录已登记 + 未被阻断 + 两侧指纹一致 + 记录自核验通过。
    """
    rec = find_record(key, int(envelope.get("版本号", -1)))
    if rec is None:
        return False, "演进记录缺失"
    if rec.get("status") == "已阻断":
        return False, "演进记录已阻断"
    if str(rec.get("映射指纹", "")) != str(envelope.get("映射指纹", "")):
        return False, "两侧指纹不一致"
    expect = fingerprint(
        key, int(rec.get("版本号", 0)),
        str(rec.get("列清单", "")), str(rec.get("来源适配", "")),
    )
    if expect != str(rec.get("映射指纹", "")):
        return False, "演进记录自核验失败"
    return True, "双边一致"


def diagnose(rule: dict[str, Any] | None, key: str, ddl_ref: str) -> str:
    """交接状态诊断：complete / mapping_only / record_only / none / conflict。"""
    rec = find_record_by_ref(key, ddl_ref)
    env = staged_by_ref(rule, key, ddl_ref)
    if rec is not None and env is not None:
        if str(rec.get("映射指纹", "")) != str(env.get("映射指纹", "")):
            return "conflict"
        return "complete"
    if rec is not None:
        return "record_only"
    if env is not None:
        return "mapping_only"
    return "none"


# ---------- 消费侧决策：批次只能消费一组可核验的完整映射 ----------

def catch_up_rule(rule: dict[str, Any], key: str) -> None:
    """把已确认的演进记录同步进规则本地登载（幂等）。

    解析端本地始终持有全部已确认版本的副本；生效指针只在尚未指向
    该键时初始化，不覆盖已有指针。
    """
    eff = effective_versions(rule)
    staged_map = staged(rule, key)
    complete: list[dict[str, Any]] = []
    for rec in records_for(key):
        env = record_envelope(rec)
        ok, _ = check_complete(key, env)
        if ok:
            complete.append(env)
    for env in complete:
        staged_map.setdefault(int(env["版本号"]), env)
    if key not in eff and complete:
        eff[key] = max(int(env["版本号"]) for env in complete)
        mirror_display(rule, key)


def resolve_effective(rule: dict[str, Any], key: str) -> tuple[dict[str, Any], str]:
    """决定一个"开始解码的批次"可以消费的版本。

    只返回可核验的完整映射：半成品（映射已写、演进缺失等）一律跳过，
    回退到最近一次已确认版本；一个已确认版本都没有时回退基线列。
    """
    catch_up_rule(rule, key)
    eff = effective_versions(rule)
    staged_map = staged(rule, key)
    candidates = sorted(
        {int(no) for no in staged_map} | {int(r["版本号"]) for r in records_for(key)},
        reverse=True,
    )
    skipped: list[str] = []
    for no in candidates:
        env = staged_map.get(no) or record_envelope(find_record(key, no))
        ok, reason = check_complete(key, env)
        if not ok:
            skipped.append(f"v{no}{reason}")
            continue
        if eff.get(key) != no:
            eff[key] = no
        mirror_display(rule, key)
        if skipped:
            return env, f"{'、'.join(skipped)}未闭环，回退已确认版本v{no}"
        return env, "生效版本双边核验通过"
    return baseline_envelope(key), "无已确认演进，消费基线映射"


# ---------- 交接核对报告 ----------

def audit_key(key: str) -> dict[str, Any]:
    """核对某个 源端@影响表 的版本交接全貌。

    回答三件事：已有批次能否继续、哪些数据以旧版产出、新版何时开始被转换消费。
    """
    backfill_legacy_records()
    source, table = key.split("@", 1)
    rule = next(
        (r for r in store.rows("binlog_parse") if str(r.get("源端", "")) == source),
        None,
    )
    if rule is not None:
        catch_up_rule(rule, key)
    staged_map = staged(rule, key) if rule is not None else {}
    recs = {int(r["版本号"]): r for r in records_for(key)}
    eff = effective_versions(rule).get(key) if rule is not None else None

    integrity: list[dict[str, Any]] = []
    for no in sorted(set(recs) | set(staged_map)):
        env = staged_map.get(no) or record_envelope(recs[no])
        ok, reason = check_complete(key, env)
        in_map, in_rec = no in staged_map, no in recs
        if ok:
            concl = "完整可消费"
        elif in_map and not in_rec:
            concl = "映射已写-演进缺失（待补做演进写入）"
        elif in_rec and not in_map:
            concl = "演进已登记-映射未登载"
        else:
            concl = reason
        integrity.append({
            "版本号": no,
            "字段版本": version_id(key, no),
            "映射侧": "已登载" if in_map else "缺失",
            "演进侧": "已登记" if in_rec else "缺失",
            "映射指纹": env.get("映射指纹", ""),
            "生效": eff == no,
            "结论": concl,
        })

    batches = [
        b for b in (rule.get("解码批次", []) if rule is not None else [])
        if str(b.get("影响表", "")) == table
    ]
    batch_checks: list[dict[str, Any]] = []
    for b in batches:
        no = int(b.get("版本号", 0))
        if no == 0:
            still, reason = True, "基线映射"
        else:
            still, reason = check_complete(
                key, {"版本号": no, "映射指纹": b.get("映射指纹", "")})
        if eff is not None and no == eff and no != 0:
            produced = "新版产出"
        elif no == 0:
            produced = "基线产出"
        else:
            produced = "旧版产出"
        batch_checks.append({
            "批次号": b.get("批次号"),
            "字段版本": b.get("字段版本"),
            "状态": b.get("状态"),
            "转换批次": b.get("转换批次", ""),
            "仍可核验": still,
            "能否继续": "可继续" if still else f"不可继续：{reason}",
            "产出": produced,
        })

    pipeline = next(
        (r for r in store.rows("event_trans") if str(r.get("源端", "")) == source),
        None,
    )
    handed = [
        b for b in batches
        if eff is not None and int(b.get("版本号", -1)) == eff and b.get("状态") == "已交接"
    ]
    first = handed[0] if handed else None
    return {
        "版本键": key,
        "源端": source,
        "影响表": table,
        "生效版本": eff,
        "完整性": integrity,
        "批次核对": batch_checks,
        "转换消费": {
            "管道编号": pipeline.get("管道编号") if pipeline else None,
            "当前转换批次": pipeline.get("转换批次") if pipeline else None,
            "采用列": pipeline.get("采用列") if pipeline else None,
            "管道字段版本": pipeline.get("字段版本") if pipeline else None,
        },
        "结论": {
            "已有批次能否继续": (
                {b["批次号"]: b["能否继续"] for b in batch_checks} or "无解码批次"
            ),
            "旧版产出批次": [b["批次号"] for b in batch_checks if b["产出"] == "旧版产出"],
            "新版消费起点": (
                {
                    "解码批次": first["批次号"],
                    "转换批次": first.get("转换批次", ""),
                    "开始时间": first.get("开始时间", ""),
                }
                if first else "新版尚未被转换消费"
            ),
        },
    }
