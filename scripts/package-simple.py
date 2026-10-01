#!/usr/bin/env python3
"""生成 mise 可识别的平台归档，只包含一个可执行文件。"""
import argparse
import hashlib
import pathlib
import re
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("target")
    parser.add_argument("version")
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    for value in (args.target, args.version):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", value):
            parser.error("target/version 必须是安全的文件名片段")
    if not args.binary.is_file():
        parser.error("二进制不存在")
    args.output.mkdir(parents=True, exist_ok=True)
    archive = args.output / f"peri-loom-{args.version}-{args.target}.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle, args.binary.open("rb") as source:
        entry = bundle.gettarinfo(str(args.binary), arcname="peri-loom")
        entry.mode, entry.uid, entry.gid = 0o755, 0, 0
        entry.uname = entry.gname = ""
        bundle.addfile(entry, source)
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    archive.with_name(archive.name + ".sha256").write_text(
        f"{digest.hexdigest()}  {archive.name}\n"
    )
    print(archive)


if __name__ == "__main__":
    main()
