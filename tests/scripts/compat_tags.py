#!/usr/bin/env python3
import os
import sys
from itertools import zip_longest

TARGET_VERSION = "v0.9.1"
SKIP_TAG = "-tags resume_query_skip"


def parse_version(ver: str):
    if not ver:
        return None
    ver = ver.strip()
    if not ver:
        return None
    if ver[0] in "vV":
        ver = ver[1:]
    segments = ver.split(".")
    try:
        return [int(part) for part in segments]
    except ValueError:
        return None


def main() -> int:
    requested = parse_version(os.environ.get("DATABEND_GO_VERSION", ""))
    target = parse_version(TARGET_VERSION)
    if requested is None or target is None:
        return 0

    for cur, tgt in zip_longest(requested, target, fillvalue=0):
        if cur < tgt:
            sys.stdout.write(SKIP_TAG)
            break
        if cur > tgt:
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
