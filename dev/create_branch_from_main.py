#!/usr/bin/env python
"""
Create a branch using a main branch.

Usage:
  uv run python dev/create_branch_from_main.py <new_display_name>
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tidb_agent import branch_manager  # noqa: E402


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: uv run python dev/create_branch_from_main.py <new_display_name>")
        sys.exit(1)
    display_name = sys.argv[1]
    try:
        created = branch_manager.create_branch(display_name=display_name)
        print(json.dumps(created, ensure_ascii=False, indent=2))
    except Exception as exc:  # pragma: no cover - network/API behavior
        print(f"Failed to create branch '{display_name}' from main: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
