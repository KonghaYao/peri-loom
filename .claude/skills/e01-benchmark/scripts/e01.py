"""E01 离线工具。没有建库、网络请求、子进程或性能执行入口。"""

import argparse
import json
import sys

import jsonio
from compare import compare
from contract import plan, template, validate
from evidence import summarize, template as raw_template
from model import generate
from oracle_manifest import build_manifest, verify_manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("template", "plan", "generate", "raw-template", "report",
                                            "manifest", "compare"))
    parser.add_argument("--config")
    parser.add_argument("--raw")
    parser.add_argument("--per-family", type=int, default=8)
    parser.add_argument("--depth-limit", type=int, default=100000)
    parser.add_argument("--verify-manifest")
    parser.add_argument("--base")
    parser.add_argument("--candidate")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command not in ("template", "compare") and not args.config:
        parser.error("--config required")
    if args.command == "compare" and not (args.base and args.candidate):
        parser.error("--base and --candidate required")
    if args.command == "report" and not args.raw:
        parser.error("--raw required")
    try:
        config = validate(jsonio.load(args.config)) if args.config else None
        if args.command == "compare":
            result = compare(args.base, args.candidate)
        elif args.command == "template":
            result = template()
        elif args.command == "plan":
            result = plan(config)
        elif args.command == "generate":
            result = generate(config)
        elif args.command == "manifest":
            if args.verify_manifest:
                result = {"verified": verify_manifest(config, jsonio.load(args.verify_manifest))}
            else:
                result = build_manifest(config, args.per_family,
                                        config["queries"]["page_size"],
                                        config["queries"]["deep_ppm"], args.depth_limit)
        elif args.command == "raw-template":
            result = raw_template(config)
        else:
            result = summarize(config, jsonio.load(args.raw))
        jsonio.write_new(args.output, result)
    except (ValueError, TypeError, KeyError, OSError, OverflowError, RecursionError):
        # 输入可能误含敏感内容，错误不回显路径、字段值或异常原文。
        print("ERROR: E01 离线处理失败；检查输入合同、大小上限及输出路径是否已存在。", file=sys.stderr)
        return 2
    print("E01 离线产物已写入；未执行数据库或压测。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
