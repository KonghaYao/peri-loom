"""客户端请求证据与顺序执行生命周期。"""
import os
import uuid
from issue11 import contract
from issue11.http import protocol
from issue11.http.runtime import RunnerError, Attempt, OperationResult, monotonic_ns
from issue11.http.workflows import Workflows


class ScenarioRunner(Workflows):
    def __init__(self, arguments, workloads, scenario, client, run_directory,
                 writers, fixture_rows, database):
        self.arguments = arguments
        self.workloads = workloads
        self.scenario_id = arguments.scenario
        self.scenario = scenario
        self.client = client
        self.run_directory = run_directory
        self.writers = writers
        self.fixture_rows = fixture_rows
        self.database = database
        self.path = protocol.pipeline_path(database)
        self.baton = ""
        self.sequence = 0
        self.revisions = {}
        self.inserted = set()
        self.verified = 0
        self.mismatches = []
        self.fatal = False
        self.error_text = ""
        self.timed_started_ns = 0
        self.timed_completed_ns = 0
        self.warmup_completed_ns = 0
        self.warmup_records = 0
        self.timed_records = 0
        self.timed_success = 0
        self.timed_failure = 0
        self.timed_rejected = 0
        self.timed_unknown = 0
        self.clock_id = "client:%d" % os.getpid()

    # -- evidence ---------------------------------------------------------

    def _span(self, phase, started_ns, duration_ns, outcome):
        return {
            "span_id": uuid.uuid4().hex,
            "parent_span_id": None,
            "process": "client",
            "phase": phase,
            "clock_id": self.clock_id,
            "duration_ns": max(0, duration_ns),
            "started_monotonic_ns": started_ns,
            "nested": False,
            "outcome": outcome,
        }

    def _request(self, steps, phase, sample_index):
        request_id = uuid.uuid4().hex
        correlation_id = uuid.uuid4().hex
        encode_started = monotonic_ns()
        payload = protocol.encode_pipeline(self.baton, steps)
        reply, exchange, failure = self.client.post(
            payload, self.path, request_id, correlation_id, steps,
            baton_in=self.baton or "")
        decode_started = monotonic_ns()
        decoded = []
        if reply is not None:
            try:
                for step, result in zip(steps, reply.results):
                    if result.ok and result.response_type == "execute":
                        decoded.append({
                            "step_id": step.step_id,
                            "rows": result.rows(),
                            "affected": result.affected_row_count(),
                            "rowid": result.last_insert_rowid(),
                        })
                    else:
                        decoded.append(None)
            except protocol.ProtocolError as decode_failure:
                failure = protocol.OutcomeUnknown("result_decode_failed")
                exchange.outcome = "outcome_unknown"
                exchange.error_code = "result_decode_failed"
                exchange.error_type = type(decode_failure).__name__
                reply = None
                decoded = []
        decode_completed = monotonic_ns()
        total_ns = max(0, decode_completed - encode_started)
        outcome = exchange.outcome
        spans = [self._span("client.encode", encode_started,
                            exchange.started_ns - encode_started, outcome)]
        spans.append(self._span("client.connect", exchange.started_ns,
                                exchange.connected_ns - exchange.started_ns, outcome))
        for span in exchange.spans:
            if span["phase"] == "client.connect":
                continue
            elif span["phase"] == "client.write":
                started = exchange.connected_ns
            elif span["phase"] == "client.wait":
                started = exchange.sent_ns
            elif span["phase"] == "client.read":
                started = exchange.headers_ns
            else:
                started = exchange.started_ns
            spans.append(self._span(span["phase"], started,
                                    span["duration_ns"], outcome))
        spans.append(self._span("client.classify", exchange.completed_ns,
                                decode_started - exchange.completed_ns, outcome))
        spans.append(self._span("client.decode", decode_started,
                                decode_completed - decode_started, outcome))
        if reply is not None and len(decoded) != len(steps):
            failure = protocol.OutcomeUnknown("decoded_step_count_mismatch")
            exchange.outcome = "outcome_unknown"
        validate_completed = monotonic_ns()
        spans.append(self._span("client.validate", decode_completed,
                                validate_completed - decode_completed, outcome))
        total_ns = validate_completed - encode_started
        request_record = exchange.as_record(self.scenario_id, sample_index,
                                            phase, self.client.tls)
        request_record["close_step"] = any(step.kind == "close" for step in steps)
        if reply is not None and not reply.ok:
            request_record["outcome"] = "error"
        self.writers["requests"].write(request_record)
        self.writers["attribution"].write({
            "schema_version": 1,
            "record_type": "request",
            "request_id": request_id,
            "pipeline_correlation_id": correlation_id,
            "scenario": self.scenario_id,
            "sample_index": sample_index,
            "phase": phase,
            "step_ids": [step.step_id for step in steps],
            "total_ns": total_ns,
            "spans": spans,
            "process_coverage": ["client"],
            "server_evidence": False,
            "outcome": request_record["outcome"],
        })
        if reply is not None and reply.baton:
            self.baton = reply.baton
        return Attempt(request_id, correlation_id, exchange, reply, failure,
                       payload, decoded, spans, total_ns,
                       [step.step_id for step in steps])

    def _operation(self, phase, sample_index):
        self.write_window = None
        started = monotonic_ns()
        if self.scenario["workflow"] == "persistent_select_one":
            result = self._persistent_select_one(sample_index)
        elif self.scenario["workflow"] == "persistent_read":
            result = self._persistent_read(sample_index)
        elif self.scenario["workflow"] == "persistent_write_verify":
            result = self._persistent_write(sample_index)
        elif self.scenario["workflow"] == "churn_read_close":
            result = self._churn_read_close(sample_index)
        elif self.scenario["workflow"] == "churn_insert_read_close":
            result = self._churn_insert_read_close(sample_index)
        elif self.scenario["workflow"] == "pipeline_write_read":
            result = self._pipeline_write_read(sample_index)
        elif self.scenario["workflow"] == "persistent_insert_read":
            result = self._persistent_insert_read(sample_index)
        else:
            raise RunnerError("scenario_not_implemented: " + self.scenario_id)
        completed = monotonic_ns()
        if self.write_window is not None:
            started, completed = self.write_window
        self.sequence += 1
        attempts = result.attempts
        record = {
            "schema_version": 1,
            "record_type": "operation",
            "op_id": uuid.uuid4().hex,
            "scenario": self.scenario_id,
            "sample_index": sample_index,
            "phase": phase,
            "issued_ns": started,
            "completed_ns": completed,
            "duration_ns": max(0, completed - started),
            "outcome": result.outcome,
            "http_request_ids": [attempt.request_id for attempt in attempts],
            "pipeline_correlation_ids": [attempt.correlation_id
                                         for attempt in attempts],
            "step_ids": [],
            "verification": result.verification,
            "may_have_committed": result.may_have_committed if not result.success else False,
            "retry_count": 0,
            "synthetic_test_fixture": bool(self.arguments.synthetic_test_fixture),
        }
        record["step_ids"] = [
            step_id for attempt in attempts for step_id in attempt.step_ids
        ]
        expected_rejections = sum(1 for attempt in attempts if attempt.expected)
        if expected_rejections:
            record["expected_rejections"] = expected_rejections
        if result.outcome == "outcome_unknown":
            record["terminal"] = {
                "recorded": True,
                "kind": "outcome_unknown",
                "no_retry": True,
            }
        elif result.outcome == "rejected":
            record["dispatched"] = False
        if result.error_code:
            record["error_code"] = result.error_code
        if self.scenario_id == "H4":
            for action, attempt in zip(("insert", "select"), attempts):
                action_record = dict(record)
                action_record.update(
                    op_id=uuid.uuid4().hex, action=action,
                    issued_ns=attempt.started_ns, completed_ns=attempt.completed_ns,
                    duration_ns=attempt.total_ns,
                    http_request_ids=[attempt.request_id],
                    pipeline_correlation_ids=[attempt.correlation_id],
                    step_ids=attempt.step_ids,
                )
                verification = (self._check_affected(attempt, 0) if action == "insert"
                                else result.verification)
                action_outcome = attempt.outcome if not attempt.ok else (
                    "success" if verification["ok"] else "error")
                action_record.update(outcome=action_outcome, verification=verification,
                                     may_have_committed=False if action_outcome == "success"
                                     else result.may_have_committed)
                if action_outcome != "outcome_unknown":
                    action_record.pop("terminal", None)
                if action_outcome == "success":
                    action_record.pop("error_code", None)
                self.writers["operations"].write(action_record)
        else:
            self.writers["operations"].write(record)
        if phase == "warmup":
            self.warmup_records += 1
            self.warmup_completed_ns = completed
        else:
            self.timed_records += 1
            if result.outcome == "success":
                self.timed_success += 1
            elif result.outcome == "rejected":
                self.timed_rejected += 1
            elif result.outcome == "outcome_unknown":
                self.timed_unknown += 1
            else:
                self.timed_failure += 1
            if not self.timed_started_ns:
                self.timed_started_ns = started
            self.timed_completed_ns = completed
        if not result.verification["ok"]:
            self._mismatch(result.verification.get("detail", "verification_failed"))
        if result.outcome != "success":
            self.fatal = True
            self.error_text = result.outcome + ": " + (result.error_code or
                                                       result.verification.get("detail", ""))
        return result

    def _mismatch(self, detail):
        self.mismatches.append(detail)
        if len(self.mismatches) > 20:
            self.mismatches = self.mismatches[:20]

    # -- verification helpers --------------------------------------------

    def _ok(self, kind, detail=""):
        self.verified += 1
        return {"ok": True, "kind": kind, "detail": detail}

    def _fail(self, kind, detail):
        return {"ok": False, "kind": kind, "detail": detail}

    @staticmethod
    def _int(value):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None


    def _check_affected(self, attempt, index, expected=1):
        decoded = attempt.decoded[index] if attempt.decoded else None
        if not decoded:
            return self._fail("affected", "missing_result")
        if decoded["affected"] != expected:
            return self._fail("affected", "%r != %r" % (decoded["affected"], expected))
        return self._ok("affected")

    @staticmethod
    def _first_failure(attempts):
        for attempt in attempts:
            if attempt.failure is not None and not attempt.expected:
                return attempt.failure
        return None

    def _finish(self, attempts, verification, may_have_committed=False,
                error_code=""):
        failure = self._first_failure(attempts)
        if failure is not None:
            return OperationResult(
                attempts,
                {"ok": False, "kind": "protocol", "detail": failure.code},
                may_have_committed=may_have_committed or failure.may_have_committed,
                error_code=failure.code)
        for attempt in attempts:
            if attempt.expected:
                continue
            if attempt.reply is not None and not attempt.reply.ok:
                code = attempt.error_code
                committed = any(result.may_have_committed
                                for result in attempt.reply.results if not result.ok)
                return OperationResult(
                    attempts,
                    {"ok": False, "kind": "step_error", "detail": code},
                    may_have_committed=committed, error_code=code)
        return OperationResult(attempts, verification,
                               may_have_committed=may_have_committed,
                               error_code=error_code)


    # -- scenarios --------------------------------------------------------


    # -- phase helpers ----------------------------------------------------

    def _warmup(self, sample_index):
        return self._phase == "warmup"

    def _timed(self, sample_index):
        return self._phase == "timed"

    # -- setup / run loop -------------------------------------------------

    def setup_fixture(self):
        statements = contract.split_sql_file("sql/00-schema.sql")
        statements += contract.split_sql_file(
            contract.fixture_relative(self.fixture_rows))
        steps = [protocol.Step(uuid.uuid4().hex, sql, [], False)
                 for sql in statements]
        steps.append(protocol.Step(uuid.uuid4().hex, kind="close"))
        attempt = self._request(steps, "setup", 0)
        if not attempt.ok:
            raise RunnerError("fixture_setup_failed: "
                              + (attempt.error_code or "request_failed"))
        if not attempt.reply.close_acknowledged:
            raise RunnerError("fixture_setup_close_not_acknowledged")
        self.baton = ""

    def run(self):
        warmup = self.arguments.warmup
        samples = self.arguments.samples
        self._phase = "warmup"
        for index in range(warmup):
            self._operation("warmup", index)
            if self.fatal:
                break
        if not self.fatal:
            self._phase = "timed"
            for index in range(samples):
                self._operation("timed", index)
                if self.fatal:
                    break
        if not self.fatal and self.baton:
            close = self._request([protocol.Step(uuid.uuid4().hex, kind="close")],
                                  "cleanup", 0)
            if not close.ok or not close.reply.close_acknowledged or close.reply.baton:
                self.fatal = True
                self.error_text = "cleanup_close_not_acknowledged"
                self._mismatch(self.error_text)
            else:
                self.baton = ""
        self._phase = "timed"
        return {
            "scenario": self.scenario_id,
            "warmup_records": self.warmup_records,
            "timed_records": self.timed_records,
            "timed_success": self.timed_success,
            "timed_failure": self.timed_failure,
            "timed_rejected": self.timed_rejected,
            "timed_unknown": self.timed_unknown,
            "verified_operations": self.verified,
            "mismatches": list(self.mismatches),
            "fatal": self.fatal,
            "error": self.error_text,
            "timed_started_ns": self.timed_started_ns,
            "timed_completed_ns": self.timed_completed_ns,
            "warmup_completed_ns": self.warmup_completed_ns,
        }
