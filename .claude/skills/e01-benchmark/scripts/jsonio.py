"""严格 JSON 读写；输出不覆盖已有文件，权限 0600。"""

import json
import os
from pathlib import Path

MAX_INPUT_BYTES = 8 * 1024 * 1024


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def load(path, limit=MAX_INPUT_BYTES):
    """严格 JSON 读取；默认 8 MiB，调用方可为目标产物显式放宽上限。"""
    with Path(path).open("rb") as stream:
        payload = stream.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("input_size_limit")
    return json.loads(payload, object_pairs_hook=unique_object)


def create_private(path):
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w")


def write_new(path, value):
    with create_private(path) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def write_lines_new(path, lines):
    with create_private(path) as stream:
        for line in lines:
            stream.write(line + "\n")
        stream.flush()
        os.fsync(stream.fileno())
