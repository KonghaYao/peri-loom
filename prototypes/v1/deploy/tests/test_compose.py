import json
import os
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[4]
DEPLOY = ROOT / "prototypes/v1/deploy"


def compose_configuration(*files, profile=None, environment=None):
    command = ["docker", "compose"]
    for file in files:
        command.extend(["-f", str(file)])
    if profile:
        command.extend(["--profile", profile])
    command.extend(["config", "--format", "json"])
    result = subprocess.run(
        command, cwd=ROOT, env={**os.environ, **(environment or {})},
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)


class ComposeConfigurationTests(unittest.TestCase):
    def test_root_deployment_requires_external_s3_and_local_bind_paths(self):
        configuration = compose_configuration(
            ROOT / "compose.yaml",
            environment={
                "PERI_IMAGE": "peri-loom:test",
                "S3_ENDPOINT": "https://s3.example.com",
                "S3_BUCKET": "isolated-test-bucket",
                "S3_REGION": "us-east-1",
                "S3_ACCESS_KEY_ID": "test-access-key",
                "S3_SECRET_ACCESS_KEY": "test-secret-key",
            },
        )
        app = configuration["services"]["app"]
        self.assertNotIn("build", app)
        self.assertIn("s3", app["command"])
        self.assertEqual(app["image"], "peri-loom:test")
        self.assertNotIn("volumes", configuration)
        self.assertEqual(
            {volume["target"]: (volume["type"], volume["source"])
             for volume in app["volumes"]},
            {
                "/data/peri": ("bind", str(ROOT / "deploy/state")),
                "/run/tls": ("bind", str(ROOT / "deploy/tls")),
            },
        )

    def test_minio_fixture_has_separate_relative_state(self):
        configuration = compose_configuration(
            DEPLOY / "compose.yaml", DEPLOY / "compose.minio.yaml",
            profile="minio",
            environment={
                "MINIO_IMAGE": "minio:test",
                "MINIO_MC_IMAGE": "minio-mc:test",
                "PERI_TEST_BUCKET": "isolated-test-bucket",
                "MINIO_ROOT_USER": "testuser",
                "MINIO_ROOT_PASSWORD": "testpassword",
            },
        )
        self.assertNotIn("volumes", configuration)
        for service, mount, source in (
            ("app", "/data/peri", "deploy/minio-app-state"),
            ("app", "/run/tls", "deploy/dev-tls"),
            ("minio", "/data", "deploy/minio-state"),
        ):
            volume = next(volume for volume in configuration["services"][service]["volumes"]
                          if volume["target"] == mount)
            self.assertEqual(volume["type"], "bind")
            self.assertEqual(volume["source"], str(ROOT / source))


if __name__ == "__main__":
    unittest.main()
