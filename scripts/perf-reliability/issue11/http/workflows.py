"""H1–H7 SQL 工作流；H8–H10/N1 不在 W0-A 执行。"""
import uuid
import hashlib
from issue11 import contract
from issue11.http import protocol


class Workflows:
    def _step(self, sql, args=(), rows=True):
        return protocol.Step(uuid.uuid4().hex, sql, list(args), rows)

    def _read_step(self, row_id, events=False):
        sql = ("SELECT value, payload FROM perf_events WHERE id = ?" if events
               else contract.read_sql("sql/11-point-read.sql").strip().rstrip(";"))
        return self._step(sql, [protocol.integer_arg(row_id)])

    def _select_one_step(self):
        return self._step(contract.read_sql("sql/10-select-one.sql").strip().rstrip(";"))

    def _check_one(self, attempt):
        if not attempt.ok or not attempt.decoded or not attempt.decoded[0]:
            return self._fail("select_one", "missing_result")
        rows = attempt.decoded[0]["rows"]
        if len(rows) != 1 or len(rows[0]) != 1 or self._int(rows[0][0]) != 1:
            return self._fail("select_one", "value_mismatch")
        return self._ok("select_one")

    def _check_row(self, attempt, index, payload, value):
        decoded = attempt.decoded[index] if attempt.decoded else None
        rows = decoded["rows"] if decoded else []
        if len(rows) != 1 or len(rows[0]) != 2 \
                or self._int(rows[0][0]) != value or rows[0][1] != payload:
            return self._fail("readback", "value_or_payload_mismatch")
        if not contract.payload_shape_ok(payload):
            return self._fail("readback", "payload_length")
        return self._ok("readback")

    def _persistent_select_one(self, sample_index):
        attempt = self._request([self._select_one_step()], self._phase, sample_index)
        verification = self._check_one(attempt)
        if attempt.ok and attempt.reply.baton is None:
            verification = self._fail("baton", "persistent_baton_missing")
        return self._finish([attempt], verification)

    def _persistent_read(self, sample_index):
        row_id = 1 + sample_index % self.fixture_rows
        attempt = self._request([self._read_step(row_id)], self._phase, sample_index)
        verification = self._check_row(attempt, 0, contract.fixture_payload(row_id),
                                       self.revisions.get(row_id, 0))
        return self._finish([attempt], verification)

    def _persistent_write(self, sample_index):
        row_id = 1 + sample_index % self.fixture_rows
        value = self.revisions.get(row_id, 0) + 1
        sql = contract.read_sql("sql/12-update.sql").strip().rstrip(";")
        write = self._request([self._step(sql, [protocol.integer_arg(row_id)], False)],
                              self._phase, sample_index)
        attempts = [write]
        verification = self._check_affected(write, 0)
        if write.ok and verification["ok"]:
            self.write_effect = {
                "request_id": write.request_id, "kind": "update", "table": "perf_kv",
                "id": row_id, "value": value,
                "payload_sha256": hashlib.sha256(contract.fixture_payload(row_id).encode()).hexdigest(),
            }
        if write.ok:
            read = self._request([self._read_step(row_id)], self._phase, sample_index)
            attempts.append(read)
            verification = self._check_row(read, 0, contract.fixture_payload(row_id), value)
            if verification["ok"]:
                self.revisions[row_id] = value
        # 只把写请求窗口作为 H3 duration；所有验证请求仍保留在 raw evidence。
        self.write_window = (write.started_ns, write.completed_ns)
        return self._finish(attempts, verification, may_have_committed=True)

    def _insert_steps(self):
        row_id = 10_000_000 + self.sequence
        payload = contract.insert_payload(row_id)
        value = row_id
        sql = contract.read_sql("sql/13-insert-readback.sql").strip().rstrip(";")
        write = self._step(sql, [protocol.integer_arg(row_id),
                                protocol.integer_arg(value), protocol.text_arg(payload)], False)
        return row_id, value, payload, write

    def _insert_read(self, sample_index, churn=False, pipeline=False):
        row_id, value, payload, write = self._insert_steps()
        if churn:
            self.baton = ""
        if pipeline:
            attempt = self._request([write, self._read_step(row_id, events=True)],
                                    self._phase, sample_index)
            attempts = [attempt]
            verification = self._check_affected(attempt, 0)
            if attempt.ok and verification["ok"]:
                self._acknowledge_insert(attempt, row_id, value, payload)
            if attempt.ok and verification["ok"]:
                verification = self._check_row(attempt, 1, payload, value)
        else:
            attempt = self._request([write], self._phase, sample_index)
            attempts = [attempt]
            verification = self._check_affected(attempt, 0)
            if attempt.ok and verification["ok"]:
                self._acknowledge_insert(attempt, row_id, value, payload)
            if attempt.ok and verification["ok"]:
                read = self._request([self._read_step(row_id, events=True)],
                                     self._phase, sample_index)
                attempts.append(read)
                verification = self._check_row(read, 0, payload, value)
        if churn and all(attempt.ok for attempt in attempts):
            close = self._request([protocol.Step(uuid.uuid4().hex, kind="close")],
                                  self._phase, sample_index)
            attempts.append(close)
            if not close.ok or not close.reply.close_acknowledged or close.reply.baton:
                verification = self._fail("close", "close_not_acknowledged")
            self.baton = ""
        if verification["ok"]:
            self.inserted.add(row_id)
        return self._finish(attempts, verification, may_have_committed=True)

    def _acknowledge_insert(self, attempt, row_id, value, payload):
        self.write_effect = {
            "request_id": attempt.request_id, "kind": "insert", "table": "perf_events",
            "id": row_id, "value": value,
            "payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
        }

    def _persistent_insert_read(self, sample_index):
        return self._insert_read(sample_index)

    def _churn_insert_read_close(self, sample_index):
        return self._insert_read(sample_index, churn=True)

    def _pipeline_write_read(self, sample_index):
        return self._insert_read(sample_index, pipeline=True)

    def _churn_read_close(self, sample_index):
        self.baton = ""
        attempt = self._request([self._select_one_step(),
                                 protocol.Step(uuid.uuid4().hex, kind="close")],
                                self._phase, sample_index)
        verification = self._check_one(attempt)
        if attempt.ok and (not attempt.reply.close_acknowledged or attempt.reply.baton):
            verification = self._fail("close", "close_not_acknowledged")
        self.baton = ""
        return self._finish([attempt], verification)
