"""DDL字段版本交接稳定性测试。

覆盖场景：
1. 正常应用：列变化以可核验的字段版本数据双边生效；
2. 同一事件重新投递：已确认版本不重复产生；
3. 部分失败（只写入映射就中断）→ 解码批次回退旧版 → 补做演进写入 → 新版生效；
4. 其他表的演进记录不会被当成本次映射；
5. 交接核对报告：已有批次能否继续 / 哪些数据以旧版产出 / 新版何时开始被转换消费。
"""
from __future__ import annotations

import copy
import unittest

from app.services import field_version as fv
from app.services.binlog_parse import BinlogParseService
from app.services.ddl_handle import DdlHandleService
from app.services.event_trans import EventTransService
from app.services.schema_evo import SchemaEvoService
from app.store import store

DDL_ID = 1  # 种子数据：PROD-MYSQL-01 / order_events / ALTER TABLE ADD COLUMN / 已应用
RULE_ID = 1  # 种子数据：BINL-0001 / PROD-MYSQL-01 / 解析中
PIPE_ID = 1  # 种子数据：ETR-0001 / PROD-MYSQL-01 / 转换中
KEY = "PROD-MYSQL-01@order_events"
BASE_COLS = "order_id,user_id,amount,status,event_time"


class DdlHandoffTest(unittest.TestCase):
    def setUp(self) -> None:
        self._backup = copy.deepcopy(store._tables)
        self.ddl = DdlHandleService()
        self.parse = BinlogParseService()
        self.trans = EventTransService()
        self.evo = SchemaEvoService()

    def tearDown(self) -> None:
        store._tables.clear()
        store._tables.update(self._backup)

    # ---------- 工具 ----------

    def _records(self):
        return [r for r in store.rows("schema_evo") if r.get("版本键") == KEY]

    def _rule(self):
        return store.find("binlog_parse", RULE_ID)

    # ---------- 1. 正常应用 ----------

    def test_apply_creates_verifiable_version(self):
        result = self.ddl.trigger_binlog_and_schema_evo(DDL_ID)
        self.assertIsNotNone(result)
        self.assertEqual(result["outcome"], "created")

        version = result["version"]
        self.assertEqual(version["版本键"], KEY)
        self.assertEqual(version["版本号"], 2)  # 历史演进SE-0001补登为v1
        self.assertEqual(version["来源适配"], "DDL-0001")
        self.assertEqual(version["列清单"], BASE_COLS + ",ext_col_v2")

        # 演进侧：记录持久化同一信封，指纹自核验通过
        record = result["schema"]
        self.assertEqual(record["字段版本"], f"{KEY}#v2")
        self.assertEqual(record["映射指纹"], version["映射指纹"])
        self.assertEqual(record["数据流"], "order_events_cdc")

        # 映射侧：规则登载同一信封且指针已生效，两边对得上
        rule = self._rule()
        self.assertEqual(rule["生效版本"][KEY], 2)
        self.assertEqual(rule["字段版本"], f"{KEY}#v2")
        ok, reason = fv.check_complete(KEY, version)
        self.assertTrue(ok, reason)

    # ---------- 2. 重新投递幂等 ----------

    def test_redelivery_does_not_duplicate_confirmed_version(self):
        first = self.ddl.trigger_binlog_and_schema_evo(DDL_ID)
        second = self.ddl.trigger_binlog_and_schema_evo(DDL_ID)
        self.assertEqual(second["outcome"], "reused")
        self.assertEqual(first["schema"]["id"], second["schema"]["id"])
        self.assertEqual(len(self._records()), 2)  # v1历史 + v2，没有v3
        self.assertEqual(second["version"]["版本号"], 2)

    # ---------- 3. 部分失败 → 中断恢复 ----------

    def test_interrupted_mapping_only_then_recover(self):
        # 模拟"只写入映射就中断"：映射侧已登载v2，演进记录缺失
        env = self.ddl.stage_field_mapping(DDL_ID)
        self.assertEqual(env["版本号"], 2)
        self.assertEqual(len(self._records()), 1)  # 只有历史v1
        rule = self._rule()
        self.assertEqual(fv.diagnose(rule, KEY, "DDL-0001"), "mapping_only")

        # 变更处理到一半时发解析：批次只能消费已确认的旧版v1
        out = self.parse.start_decode_batch(RULE_ID)
        batch_old = out["batch"]
        self.assertEqual(batch_old["版本号"], 1)
        self.assertIn("回退", batch_old["解码依据"])
        self.assertEqual(batch_old["采用列"], BASE_COLS)

        # 补做演进写入：按映射侧持久化的信封补登，指纹一致、不重复版本
        recovery = self.ddl.recover_interrupted(DDL_ID)
        self.assertEqual(recovery["pre_state"], "mapping_only")
        self.assertEqual(recovery["action"], "补做演进写入")
        self.assertEqual(len(self._records()), 2)
        record = self._records()[-1]
        self.assertEqual(record["映射指纹"], env["映射指纹"])
        self.assertEqual(record["版本号"], 2)

        # 恢复后再重新投递同一事件：仍然幂等
        again = self.ddl.trigger_binlog_and_schema_evo(DDL_ID)
        self.assertEqual(again["outcome"], "reused")
        self.assertEqual(len(self._records()), 2)

        # 新版开始被消费：新批次钉住v2并同步交接事件转换
        out2 = self.parse.start_decode_batch(RULE_ID)
        batch_new = out2["batch"]
        self.assertEqual(batch_new["版本号"], 2)
        self.assertEqual(batch_new["状态"], "已交接")
        pipeline = out2["pipeline"]
        self.assertEqual(pipeline["采用列"], BASE_COLS + ",ext_col_v2")
        self.assertIn(batch_new["批次号"], pipeline["最近解码依据"])

        # 刷新转换批次能查到实际采用的列
        refresh = self.trans.refresh_conversion_batch(PIPE_ID)
        self.assertEqual(refresh["采用列"], BASE_COLS + ",ext_col_v2")
        self.assertEqual(refresh["字段版本"], f"{KEY}#v2")
        self.assertFalse(refresh["有新版待消费"])

        # 交接核对：旧批次可继续、旧版产出可追、新版消费起点明确
        report = self.evo.audit_version_handoff("PROD-MYSQL-01", "order_events")
        concl = report["结论"]
        self.assertEqual(concl["已有批次能否继续"][batch_old["批次号"]], "可继续")
        self.assertEqual(concl["旧版产出批次"], [batch_old["批次号"]])
        self.assertEqual(concl["新版消费起点"]["解码批次"], batch_new["批次号"])
        self.assertEqual(concl["新版消费起点"]["转换批次"], batch_new["转换批次"])

    # ---------- 4. 其他表的演进不会混入 ----------

    def test_other_table_evolution_not_confused(self):
        store.rows("schema_evo").append({
            "id": 99, "记录编号": "SE-0099", "数据流": "payment_log_cdc",
            "变更列": "channel(VARCHAR→ENUM)", "列变更前": "VARCHAR", "列变更后": "ENUM",
            "兼容模式": "向前兼容", "下游影响": "无", "演进时间": "2026-11-22 09:00:00",
            "status": "兼容通过", "pending": True, "abnormal": False,
        })
        result = self.ddl.trigger_binlog_and_schema_evo(DDL_ID)
        # order_events键只编到v2，payment_log的演进不影响本次映射
        self.assertEqual(result["version"]["版本号"], 2)
        keys = {r["版本键"] for r in store.rows("schema_evo")}
        self.assertIn("PROD-PG-02@payment_log", keys)
        self.assertEqual(len(self._records()), 2)
        report = self.evo.audit_version_handoff("PROD-MYSQL-01", "order_events")
        self.assertTrue(all(item["字段版本"].startswith(KEY) for item in report["完整性"]))

    # ---------- 5. 演进记录被阻断后批次回退 ----------

    def test_blocked_record_forces_fallback(self):
        self.ddl.trigger_binlog_and_schema_evo(DDL_ID)
        record = self._records()[-1]
        record["status"] = "已阻断"
        out = self.parse.start_decode_batch(RULE_ID)
        self.assertEqual(out["batch"]["版本号"], 1)
        self.assertIn("已阻断", out["batch"]["解码依据"])


if __name__ == "__main__":
    unittest.main()
