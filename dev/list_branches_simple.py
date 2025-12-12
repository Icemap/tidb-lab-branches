#!/usr/bin/env python
"""
List branches with ids, display names, state, and parent ids.

Usage:
  uv run python dev/list_branches_simple.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tidb_agent import branch_manager  # noqa: E402


def main() -> None:
    try:
        branches = branch_manager.list_branches()
    except Exception as exc:  # pragma: no cover - network/API behavior
        print(f"Failed to list branches: {exc}")
        sys.exit(1)

    payload = [
        {
            "branchId": b.get("branchId"),
            "displayName": b.get("displayName"),
            "state": b.get("state"),
            "parentId": b.get("parentId"),
        }
        for b in branches
    ]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
