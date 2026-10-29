# SPDX-License-Identifier: Apache-2.0
"""`python -m cleanroom_sql policy check <file>`: load a policy file and report every fault.

Exit 0 on a valid policy, 1 with one `path: message` line per fault on
stderr, 2 when the file cannot be read. No backend is touched.
"""
from __future__ import annotations

import argparse
import sys

from .policy import PolicyError, load_policy


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cleanroom-sql")
    sub = parser.add_subparsers(dest="command", required=True)
    policy = sub.add_parser("policy", help="policy file commands").add_subparsers(dest="action", required=True)
    check = policy.add_parser("check", help="validate a policy file without a backend")
    check.add_argument("file")
    args = parser.parse_args(argv)

    try:
        loaded = load_policy(args.file)
    except (OSError, UnicodeDecodeError) as e:
        print(e, file=sys.stderr)
        return 2
    except PolicyError as e:
        for path, message in e.errors:
            print(f"{path or '<root>'}: {message}", file=sys.stderr)
        return 1
    print(f"OK: {loaded.dataset.name}, {len(loaded.consumers)} consumers")
    return 0


if __name__ == "__main__":
    sys.exit(main())
