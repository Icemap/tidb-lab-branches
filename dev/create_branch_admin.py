#!/usr/bin/env python
"""
Create (or fetch) an admin user for a specific branch and print the credentials.

Usage:
  uv run python dev/create_branch_admin.py <branch_display_name>
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tidb_agent import branch_manager, get_admin_user_for_branch  # noqa: E402


def _list_branches() -> None:
    branches = branch_manager.list_branches()
    payload = [
        {
            "branchId": b.get("branchId"),
            "displayName": b.get("displayName"),
            "state": b.get("state"),
            "parentId": b.get("parentId"),
        }
        for b in branches
    ]
    print("Available branches:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: uv run python dev/create_branch_admin.py <branch_display_name_or_id>")
        _list_branches()
        sys.exit(1)

    branch_name = sys.argv[1]
    try:
        raw = get_admin_user_for_branch(branch_name)
        try:
            data: Dict[str, Any] = json.loads(raw)
            print(json.dumps(data, ensure_ascii=False, indent=2))
        except json.JSONDecodeError:
            print(raw)
    except Exception as exc:  # pragma: no cover - network/API behavior
        print(f"Failed to create/fetch admin for branch '{branch_name}': {exc}")
        _list_branches()
        sys.exit(1)


if __name__ == "__main__":
    main()
