import json
import os
import sys
import unittest


class DummyManager:
    def __init__(self):
        self.branch_checks = 0

    def create_branch(self, display_name: str, parent_id=None, parent_timestamp=None):
        return {"branchId": "b1", "displayName": display_name, "parentId": parent_id}

    def create_backup(self, display_name=None):
        return {"branchId": "b2", "displayName": display_name or "backup"}

    def get_branch(self, branch_id: str):
        self.branch_checks += 1
        if self.branch_checks < 2:
            return {"branchId": branch_id, "state": "CREATING"}
        return {"branchId": branch_id, "state": "ACTIVE"}


def reload_tidb_agent():
    sys.modules.pop("tidb_agent", None)
    import tidb_agent  # noqa: WPS433

    return tidb_agent


class BranchWaitTest(unittest.TestCase):
    def setUp(self):
        self._env_backup = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env_backup)
        sys.modules.pop("tidb_agent", None)

    def test_create_branch_waits_for_active(self):
        os.environ["TIDBCLOUD_PROJECT_ID"] = "proj"
        mod = reload_tidb_agent()
        mod._branch_manager = DummyManager()
        mod.time = type("T", (), {"sleep": lambda *_: None})()  # no real sleep

        res = json.loads(mod.create_branch("demo"))
        self.assertEqual(res["branchId"], "b1")
        self.assertGreaterEqual(mod._branch_manager.branch_checks, 2)

    def test_create_branch_backup_waits_for_active(self):
        os.environ["TIDBCLOUD_PROJECT_ID"] = "proj"
        mod = reload_tidb_agent()
        mod._branch_manager = DummyManager()
        mod.time = type("T", (), {"sleep": lambda *_: None})()

        res = json.loads(mod.create_branch_backup("bk"))
        self.assertEqual(res["branchId"], "b2")
        self.assertGreaterEqual(mod._branch_manager.branch_checks, 2)

    def test_create_branch_from_display_name_resolves_parent(self):
        os.environ["TIDBCLOUD_PROJECT_ID"] = "proj"
        mod = reload_tidb_agent()

        class DM(DummyManager):
            def list_branches(self):
                return [
                    {"displayName": "parentA", "branchId": "pA"},
                    {"displayName": "other", "branchId": "pB"},
                ]

        mod._branch_manager = DM()
        mod.time = type("T", (), {"sleep": lambda *_: None})()

        res = json.loads(mod.create_branch_from_display_name("child1", "parentA"))
        self.assertEqual(res["parentId"], "pA")


if __name__ == "__main__":
    unittest.main()
