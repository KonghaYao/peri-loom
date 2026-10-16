import argparse
import importlib.util
import pathlib
import re
import unittest
from unittest.mock import patch


ROOT = pathlib.Path(__file__).resolve().parents[4]
DEPLOY = ROOT / "prototypes/v1/deploy"
spec = importlib.util.spec_from_file_location("shutdown_budget_dev", ROOT / "prototypes/v1/platform/dev.py")
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class TestDeploymentBudgetParity(unittest.TestCase):
    def test_deployment_grace_matches_launcher_default_and_margin(self):
        defaults = {}
        original = argparse.ArgumentParser.add_argument
        def capture(parser, *args, **kwargs):
            if args == ("--shutdown-budget-seconds",):
                defaults["budget"] = kwargs["default"]
            return original(parser, *args, **kwargs)
        with patch.object(argparse.ArgumentParser, "add_argument", capture), patch("sys.argv", ["dev.py", "--help"]), patch("sys.stdout"), self.assertRaises(SystemExit) as result:
            dev.main()
        self.assertEqual(result.exception.code, 0)
        self.assertEqual(defaults["budget"], dev.SHUTDOWN_BUDGET_SECONDS)
        self.assertEqual(sum(dev.SHUTDOWN_STAGE_CAPS.values()), defaults["budget"])
        expected = defaults["budget"] + dev.SHUTDOWN_FINALIZE_SECONDS
        for path, pattern in (
            (ROOT / "compose.yaml", r"^\s*stop_grace_period:\s*(\d+)s\s*$"),
            (DEPLOY / "compose.yaml", r"^\s*stop_grace_period:\s*(\d+)s\s*$"),
            (DEPLOY / "helm/peri/templates/deployment.yaml", r"^\s*terminationGracePeriodSeconds:\s*(\d+)\s*$"),
        ):
            with self.subTest(path=str(path.relative_to(ROOT))):
                values = re.findall(pattern, path.read_text(), re.MULTILINE)
                self.assertEqual(len(values), 1)
                self.assertEqual(int(values[0]), expected)
        self.assertEqual(dev.ORCHESTRATION_GRACE_SECONDS, expected)

    def test_stage_caps_stay_above_the_component_shutdown_budgets(self):
        # Each stage cap has to stay strictly above the budget its component gives
        # itself, or the launcher's SIGKILL ends a stop the component would have
        # reported as unclean. Reading the budgets out of the two implementations
        # keeps the assertion true when either default moves.
        budgets_ms = {
            "platform (Go defaultServiceShutdownTimeout)": self.go_component_budget_ms(),
            "rust (ShutdownBudget default)": self.rust_component_budget_ms(),
        }
        for name, budget_ms in budgets_ms.items():
            with self.subTest(component=name):
                self.assertGreater(min(dev.SHUTDOWN_STAGE_CAPS.values()) * 1000, budget_ms)

    def test_declared_stage_cap_and_grace_reach_the_platform(self):
        # The platform refuses a stage cap the declared grace does not cover, so
        # the launcher has to declare both names the platform reads, and the caps
        # it declares have to fit inside that grace.
        go_source = (ROOT / "prototypes/v1/platform/service_shutdown.go").read_text()
        launcher_source = (ROOT / "prototypes/v1/platform/dev.py").read_text()
        for name in ("PERI_SHUTDOWN_STAGE_CAP_MS", "PERI_SHUTDOWN_OUTER_GRACE_MS"):
            self.assertIn('"%s"' % name, go_source)
            self.assertIn('platform_environment["%s"]' % name, launcher_source)
        self.assertLess(max(dev.SHUTDOWN_STAGE_CAPS.values()), dev.ORCHESTRATION_GRACE_SECONDS)
        self.assertLess(dev.SHUTDOWN_BUDGET_SECONDS, dev.ORCHESTRATION_GRACE_SECONDS)

    def go_component_budget_ms(self):
        source = (ROOT / "prototypes/v1/platform/service_shutdown.go").read_text()
        found = re.findall(r"^\s*(?:const\s+)?defaultServiceShutdownTimeout\s*=\s*(\d+)\s*\*\s*time\.Second\s*$", source, re.MULTILINE)
        self.assertEqual(len(found), 1, "the platform shutdown budget default moved out of the parseable form")
        return int(found[0]) * 1000

    def rust_component_budget_ms(self):
        source = (ROOT / "prototypes/v1/src/shutdown_budget.rs").read_text()
        found = re.findall(r"^\s*Err\(std::env::VarError::NotPresent\)\s*=>\s*([\d_]+),\s*$", source, re.MULTILINE)
        self.assertEqual(len(found), 1, "the rust shutdown budget default moved out of the parseable form")
        return int(found[0].replace("_", ""))


if __name__ == "__main__":
    unittest.main()
