import io
import json
import os
import pathlib
import subprocess
import tarfile
import time
import unittest
import uuid


ROOT = pathlib.Path(__file__).resolve().parents[4]
FIXTURE = r'''
import argparse, importlib.util, pathlib, signal, subprocess, sys
spec = importlib.util.spec_from_file_location("dev", "/fixture/dev.py")
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)
directory = pathlib.Path("/tmp/peri-budget-fixture")
if (directory / "launch.json").exists():
    configuration = dev.json.loads((directory / "platform.json").read_text())["owned_stop_proof"]
    try:
        dev.verify_clean_terminal(directory, configuration)
    except RuntimeError:
        print("restart_refused", flush=True)
        sys.exit(2)
    sys.exit(3)
project = dev.Project(argparse.Namespace(run_dir=str(directory), no_build=True, backend="local", listen="127.0.0.1:19443", tls=False, writer_admission=False))
project.acquire_run_lock()
project.prepare()
(directory / "authority/authority.lock").write_bytes(b"dirty\n")
child = subprocess.Popen([sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); time.sleep(120)"], stdout=subprocess.PIPE, text=True, start_new_session=True)
assert child.stdout.readline().strip() == "ready"
project.processes["platform"] = child
signal.signal(signal.SIGTERM, lambda *_: project.begin_shutdown())
signal.signal(signal.SIGINT, lambda *_: project.begin_shutdown())
print("fixture_ready", flush=True)
project.stop.wait()
project.cleanup()
project.mark_clean_terminal()
'''


@unittest.skipUnless(os.environ.get("PERI_SHUTDOWN_TEST_IMAGE"), "设置 PERI_SHUTDOWN_TEST_IMAGE 为含 Python 的本地镜像后运行容器边界测试")
class TestOuterGraceExpiryIsDirty(unittest.TestCase):
    def docker(self, *args, timeout=20):
        return subprocess.run(["docker", *args], check=True, capture_output=True, timeout=timeout)

    def run_boundary(self, forced):
        name = "peri-r07-" + uuid.uuid4().hex
        image = os.environ["PERI_SHUTDOWN_TEST_IMAGE"]
        self.docker("create", "--name", name, "--stop-timeout", "1", "--network", "none",
                    "--env", "PERI_PROJECT_ROOT=/app",
                    "--mount", "type=bind,source=" + str(ROOT / "prototypes/v1/platform/dev.py") + ",target=/fixture/dev.py,readonly",
                    "--entrypoint", "python3", image, "-c", FIXTURE)
        try:
            self.docker("start", name)
            deadline = time.monotonic() + 15
            while b"fixture_ready" not in self.docker("logs", name).stdout:
                if self.docker("inspect", "--format", "{{.State.Running}}", name).stdout.strip() != b"true" or time.monotonic() >= deadline:
                    self.fail("容器 fixture 未就绪：" + self.docker("logs", name).stderr.decode())
                time.sleep(0.1)
            if forced:
                self.docker("kill", "--signal", "KILL", name)
            else:
                self.docker("stop", name)
            state = json.loads(self.docker("inspect", "--format", "{{json .State}}", name).stdout)
            self.assertFalse(state["Running"])
            self.assertEqual(state["ExitCode"], 137)
            archive = self.docker("cp", name + ":/tmp/peri-budget-fixture/.", "-").stdout
            with tarfile.open(fileobj=io.BytesIO(archive)) as contents:
                files = {member.name.removeprefix("./"): member for member in contents.getmembers()}
                self.assertNotIn("launcher-terminal.json", files)
                self.assertEqual(contents.extractfile(files["authority/authority.lock"]).read(), b"dirty\n")
                if not forced:
                    envelope = json.load(contents.extractfile(files["launcher-stop.json"]))
                    self.assertEqual(json.loads(envelope["payload"])["outcome"], "in_progress")
            restarted = subprocess.run(["docker", "start", "-a", name], capture_output=True, timeout=20)
            self.assertEqual(restarted.returncode, 2)
            self.assertIn(b"restart_refused", restarted.stdout)
        finally:
            running = self.docker("inspect", "--format", "{{.State.Running}}", name).stdout.strip()
            if running == b"true":
                self.docker("stop", "--time", "1", name)
            self.docker("rm", name)

    def test_outer_grace_expiry_is_dirty(self):
        self.run_boundary(forced=False)

    def test_sigkill_never_authorizes_launcher_restart(self):
        self.run_boundary(forced=True)


if __name__ == "__main__":
    unittest.main()
