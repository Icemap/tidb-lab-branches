#!/usr/bin/env python
"""
Create a branch using a parent branchId.

Usage:
  uv run python dev/create_branch_from_parent_id.py <new_display_name> <parent_branch_id>
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tidb_agent import branch_manager  # noqa: E402


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: uv run python dev/create_branch_from_parent_id.py <new_display_name> <parent_branch_id>")
        sys.exit(1)
    display_name = sys.argv[1]
    parent_id = sys.argv[2]
    try:
        created = branch_manager.create_branch(display_name=display_name, parent_id=parent_id)
        print(json.dumps(created, ensure_ascii=False, indent=2))
    except Exception as exc:  # pragma: no cover - network/API behavior
        print(f"Failed to create branch '{display_name}' from parent '{parent_id}': {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
