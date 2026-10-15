#!/usr/bin/env python3
"""Isolated S3 shared-bucket cross-volume fencing diagnostic; retains resources."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import sys
import tarfile


BASE = Path(__file__).with_name("04_dr_driver.py")
spec = importlib.util.spec_from_file_location("peri_dr_base", BASE)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


class Diagnostic(base.Rehearsal):
    def __init__(self, args):
        super().__init__(args)
        self.prefix = "peri-s3-clone-" + self.owner
        self.bucket = "peri-clone-" + self.owner
        self.network = self.prefix
        self.directory = Path(base.tempfile.mkdtemp(prefix=self.prefix + "-", dir=args.private_parent)).resolve()
        self.directory.chmod(0o700)
        self.report = {"status": "IN_PROGRESS", "owner": self.owner, "bucket": self.bucket,
                       "events": [], "resources_retained": True}
        self.ids = {}
        self.tables = {s: "s3clone_" + secrets.token_hex(8) for s in ("source", "a", "b")}
        self.values = {s: secrets.token_hex(24) for s in ("source", "a", "b")}
        base.save(self.directory / "credentials.json", {"access": self.access, "secret": self.secret})
        base.save(self.directory / "probes.json", {"tables": self.tables, "values": self.values})
        # Private entry point keeps the repo-relative imports used by the driver.
        wrapper = self.directory / "reproduce.py"
        wrapper.write_text("import runpy\nrunpy.run_path(" + repr(str(Path(__file__).resolve())) + ", run_name='__main__')\n")
        wrapper.chmod(0o600)
        self.event("initialized", prefix=self.prefix, bucket=self.bucket)

    def state(self, suffix):
        item = self.inspect("container", suffix)
        s = item["State"]
        return {"running": s["Running"], "exit_code": s["ExitCode"],
                "oom_killed": s["OOMKilled"], "started_at": s["StartedAt"],
                "finished_at": s["FinishedAt"]}

    def marker_proof(self, archive):
        with tarfile.open(self.directory / archive["file"]) as tar:
            marker = tar.extractfile("./authority/authority.lock")
            terminal = tar.getmember("./launcher-terminal.json")
            valid = marker is not None and marker.read() == b"clean\n" and terminal.isfile()
        self.event("cold_clean_proof", valid=valid, archive_sha256=archive["sha256"])
        if not valid:
            raise RuntimeError("cold archive lacks clean proof")

    def record_write(self, suffix, config):
        table, value = self.tables[suffix], self.values[suffix]
        session = self.sql(config, "open", {"database": config["database"]})["session"]
        sql = "CREATE TABLE " + table + "(value TEXT NOT NULL);INSERT INTO " + table + " VALUES('" + value + "');"
        self.event("write_dispatch", suffix=suffix, table=table,
                   peer_state=self.state("b" if suffix == "a" else "a") if suffix != "source" else None)
        try:
            answer = self.sql(config, "execute", {"session": session, "sql": sql})
            self.event("write_response", suffix=suffix, response=answer,
                       value_sha256=hashlib.sha256(value.encode()).hexdigest())
        except Exception as error:
            self.event("write_error_or_unknown", suffix=suffix, error_type=type(error).__name__,
                       detail_private=str(error))
            answer = None
        finally:
            try:
                self.sql(config, "close", {"session": session})
            except Exception as error:
                self.event("session_close_error", suffix=suffix, error_type=type(error).__name__)
        return answer

    def read(self, suffix, config, table_suffix):
        try:
            rows = self.query(config, self.tables[table_suffix])
            match = rows == [[self.values[table_suffix]]]
            self.event("read", suffix=suffix, table_owner=table_suffix, match=match,
                       rows_sha256=hashlib.sha256(json.dumps(rows).encode()).hexdigest())
            return match
        except Exception as error:
            self.event("read_error", suffix=suffix, table_owner=table_suffix,
                       error_type=type(error).__name__, detail_private=str(error))
            return False

    def attempt_ready(self, suffix):
        try:
            config = self.ready(suffix)
            self.event("clone_ready", suffix=suffix, state=self.state(suffix))
            return config
        except Exception as error:
            self.event("clone_not_ready", suffix=suffix, state=self.state(suffix),
                       error_type=type(error).__name__, detail_private=str(error))
            return None

    def run_diagnostic(self):
        try:
            self.images = {"app": self.image(base.DEFAULT_APP), "s3": self.image(base.DEFAULT_S3)}
            self.event("images_verified", images=self.images)
            self.docker("network", "create", "--label", base.LABEL + "=" + self.owner, self.network)
            network = json.loads(self.docker("network", "inspect", self.network))[0]
            if network["Name"] != self.network or network.get("Labels", {}).get(base.LABEL) != self.owner:
                raise RuntimeError("network ownership mismatch")
            for suffix in ("s3-data", "app-source", "app-a", "app-b"):
                self.create_volume(suffix)
            s3_endpoint = self.create_s3("s3-service", "s3-data")
            self.wait_s3(s3_endpoint, 404)
            if self.s3(s3_endpoint, "PUT", "/" + self.bucket)[0] not in (200, 204):
                raise RuntimeError("unique bucket creation failed")
            self.event("bucket_created", endpoint=s3_endpoint)
            self.create_app("source", "app-source")
            source = self.ready("source")
            self.event("source_ready", state=self.state("source"))
            if self.record_write("source", source) != {"executed": True} or not self.read("source", source, "source"):
                raise RuntimeError("source ACK/read missing")
            self.stop("source")
            self.event("source_clean_exit", state=self.state("source"))
            archive = self.archive("app-source", "cold-app-state.tar")
            self.marker_proof(archive)
            self.event("source_s3_objects", objects=self.objects(s3_endpoint))
            for suffix in ("a", "b"):
                self.extract("app-" + suffix, archive)
            self.event("two_fresh_volumes_restored", archive_sha256=archive["sha256"])
            # create_app starts the container. Start both before waiting for either.
            for suffix in ("a", "b"):
                self.create_app(suffix, "app-" + suffix)
            configs = {s: self.attempt_ready(s) for s in ("a", "b")}
            self.event("concurrent_start_result", states={s: self.state(s) for s in ("a", "b")},
                       ready={s: configs[s] is not None for s in ("a", "b")})
            for suffix, config in configs.items():
                if config is None:
                    continue
                item = self.inspect("container", suffix)
                source_item = self.inspect("container", "source")
                env = {x.split("=", 1)[0]: x.split("=", 1)[1] for x in item["Config"]["Env"] if "=" in x}
                source_env = {x.split("=", 1)[0]: x.split("=", 1)[1] for x in source_item["Config"]["Env"] if "=" in x}
                identity = {"database_equal": config["database"] == source["database"],
                            "project_key_equal": config["project_api_key"] == source["project_api_key"],
                            "s3_endpoint_bucket_credentials_equal": all(env.get(k) == source_env.get(k)
                                for k in ("S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"))}
                self.event("clone_identity", suffix=suffix, **identity)
                self.read(suffix, config, "source")
            if all(configs.values()) and all(self.state(s)["running"] for s in ("a", "b")):
                for suffix in ("a", "b"):
                    if not all(self.state(s)["running"] for s in ("a", "b")):
                        self.event("overlap_ended_before_write", suffix=suffix)
                        break
                    self.record_write(suffix, configs[suffix])
                    self.read(suffix, configs[suffix], suffix)
                for suffix in ("a", "b"):
                    for table_suffix in ("source", "a", "b"):
                        self.read(suffix, configs[suffix], table_suffix)
            self.event("final_observation", states={s: self.state(s) for s in ("source", "a", "b")},
                       objects=self.objects(s3_endpoint))
            self.report["status"] = "OBSERVED"
        except BaseException as error:
            self.report["status"] = "PARTIAL"
            self.event("diagnostic_error", error_type=type(error).__name__, detail_private=str(error))
        finally:
            for suffix in ("a", "b", "source", "s3-service"):
                if suffix not in self.ids:
                    continue
                try:
                    if self.state(suffix)["running"]:
                        self.stop(suffix)
                        self.event("final_clean_stop", suffix=suffix, state=self.state(suffix))
                except Exception as error:
                    self.event("final_stop_error", suffix=suffix, error_type=type(error).__name__)
            self.flush()
        return self.report["status"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private-parent", default=None)
    parser.add_argument("--ready-timeout", type=int, default=60)
    parser.add_argument("--stop-timeout", type=int, default=90)
    args = parser.parse_args()
    os.umask(0o077)
    trial = Diagnostic(args)
    status = trial.run_diagnostic()
    print(json.dumps({"status": status, "owner": trial.owner, "private_directory": str(trial.directory)}))
    return 0 if status == "OBSERVED" else 1


if __name__ == "__main__":
    sys.exit(main())
