import hashlib
import json
from pathlib import Path
import tarfile

from runner import ROOT, digest

HISTORICAL = Path("/private/tmp/peri-goal90-build/candidate-pwtp4d9y")
IMAGE = "sha256:c197c9b0295ffaec6373deb3bebbd1b9aeb237ef2135a98b273013d0ace173ca"


def historical_image_evidence(image_id):
    if image_id != IMAGE:
        return {"status": "unattested", "reason": "image_not_historical_candidate"}
    manifest_path = HISTORICAL / "source-manifest.json"
    manifest = json.loads(manifest_path.read_text())
    root_mismatches = []
    for filename, expected in manifest["root_source_sha256"].items():
        source = HISTORICAL / "context" / filename
        if not source.is_file() or digest(source) != expected:
            root_mismatches.append(filename)
    child_seen, child_mismatches = set(), []
    with tarfile.open(HISTORICAL / "child.tar") as archive:
        for member in archive:
            filename = member.name.removeprefix("./")
            if filename in manifest["child_source_sha256"] and (member.isfile() or member.issym() or member.islnk()):
                child_seen.add(filename)
                with archive.extractfile(member) as source:
                    actual = hashlib.file_digest(source, "sha256").hexdigest()
                if actual != manifest["child_source_sha256"][filename]:
                    child_mismatches.append(filename)
    child_missing = sorted(set(manifest["child_source_sha256"]) - child_seen)
    code_suffixes = {".rs", ".go", ".py", ".ts", ".mjs", ".toml", ".lock", ".mod", ".sum"}
    current_differences = [filename for filename, expected in manifest["root_source_sha256"].items()
                           if Path(filename).suffix in code_suffixes
                           and (not (ROOT / filename).is_file() or digest(ROOT / filename) != expected)]
    linked = image_id in (HISTORICAL / "build.log").read_text()
    return {"status": "historical_frozen_inputs_verified" if not root_mismatches and not child_mismatches and not child_missing and linked else "evidence_mismatch",
            "build_directory": str(HISTORICAL), "root_head": manifest["root_head"],
            "root_dirty": manifest["root_dirty"], "child_head": manifest["child_head"],
            "root_input_count": manifest["root_input_count"], "child_input_count": manifest["child_input_count"],
            "root_mismatches": root_mismatches, "child_mismatches": child_mismatches,
            "child_missing": child_missing, "current_root_code_differences": current_differences,
            "build_log_links_image": linked,
            "evidence_sha256": {name: digest(HISTORICAL / name) for name in ["source-manifest.json", "build.log", "child.tar", "build.exit"]},
            "note": "historical dirty build inputs, not a fresh build at current HEAD; image binaries separately hashed"}
