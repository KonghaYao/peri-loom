"""Runner units that need no live platform.

The matrix itself is only exercised by a real run, so these tests pin the two
places where a silent bug would invalidate a whole bundle: the idle unload
proof and the warmup/measure split of the success counters.
"""
from __future__ import annotations

import sys
import tempfile
import unittest

from pl_retail.business import BusinessOutcome, AttemptTrace
from pl_retail.errors import Terminal
from pl_retail.fixture import FixtureConfig
from pl_retail.runner import RunnerConfig, RoundRunner, _WorkerStats


def _config(descriptor: dict | None, output: str) -> RunnerConfig:
    return RunnerConfig(
        fixture=FixtureConfig(
            endpoint="https://127.0.0.1:1",
            tenant="t",
            project="p",
            global_api_key="g",
            project_api_key="k",
        ),
        output_dir=output,
        platform_descriptor=descriptor,
    )


def _outcome(terminal: str) -> BusinessOutcome:
    outcome = BusinessOutcome(terminal=terminal, detail={}, trace=AttemptTrace())
    return outcome


class UnloadProbeTests(unittest.TestCase):
    def _runner(self, descriptor: dict | None) -> RoundRunner:
        directory = tempfile.mkdtemp(prefix="pl-retail-runner-")
        return RoundRunner(_config(descriptor, directory))

    def test_missing_probe_is_unproven(self) -> None:
        self.assertEqual(self._runner(None)._unload_probe()["status"], "unproven")

    def test_probe_without_command_is_unproven(self) -> None:
        runner = self._runner({"unload_probe": {"expect": "x"}})
        self.assertEqual(runner._unload_probe()["status"], "unproven")

    def test_zero_exit_with_expected_token_proves_unload(self) -> None:
        runner = self._runner(
            {
                "unload_probe": {
                    "command": [sys.executable, "-c", "print('loaded=0')"],
                    "expect": "loaded=0",
                }
            }
        )
        probe = runner._unload_probe()
        self.assertEqual(probe["status"], "unloaded")
        self.assertTrue(probe["expect_matched"])

    def test_zero_exit_without_expected_token_is_unproven(self) -> None:
        runner = self._runner(
            {
                "unload_probe": {
                    "command": [sys.executable, "-c", "print('loaded=1')"],
                    "expect": "loaded=0",
                }
            }
        )
        self.assertEqual(runner._unload_probe()["status"], "unproven")

    def test_nonzero_exit_is_failed(self) -> None:
        runner = self._runner(
            {
                "unload_probe": {
                    "command": [sys.executable, "-c", "import sys; sys.exit(3)"],
                    "expect": "loaded=0",
                }
            }
        )
        probe = runner._unload_probe()
        self.assertEqual(probe["status"], "failed")
        self.assertEqual(probe["exit_code"], 3)


class WorkerStatsTests(unittest.TestCase):
    def test_successes_are_split_by_window(self) -> None:
        stats = _WorkerStats()
        stats.add(_outcome(Terminal.COMMITTED_ACK.value), "warmup")
        stats.add(_outcome(Terminal.READ_OK.value), "measure")
        stats.add(_outcome(Terminal.KNOWN_FAILURE_ROLLED_BACK.value), "measure")
        self.assertEqual(stats.warmups, 1)
        self.assertEqual(stats.measure, 2)
        self.assertEqual(stats.warmup_successes, 1)
        self.assertEqual(stats.measure_successes, 1)
        self.assertEqual(stats.warmup_checkout_acks, 1)

    def test_warmup_checkout_acks_only_count_committed_checkouts(self) -> None:
        stats = _WorkerStats()
        stats.add(_outcome(Terminal.READ_OK.value), "warmup")
        self.assertEqual(stats.warmup_checkout_acks, 0)


if __name__ == "__main__":
    unittest.main()
