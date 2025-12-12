#!/usr/bin/env python
"""
Delete all branches in the current TiDB Cloud cluster.

Warning: this will attempt to remove every branch returned by the branch API.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tidb_agent import branch_manager  # noqa: E402


def main() -> None:
    branches: List[Dict[str, Any]] = branch_manager.list_branches()
    if not branches:
        print("No branches returned; nothing to delete.")
        return

    results = []
    for br in branches:
        branch_id = br.get("branchId") or br.get("id")
        name = br.get("displayName") or branch_id
        try:
            resp = branch_manager.delete_branch(branch_id)
            results.append({"branchId": branch_id, "displayName": name, "status": "deleted", "response": resp})
        except Exception as exc:  # pragma: no cover - network/API behavior
            results.append({"branchId": branch_id, "displayName": name, "status": "error", "error": str(exc)})
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
