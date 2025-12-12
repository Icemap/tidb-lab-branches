import os
import sys
import tempfile
import unittest
from pathlib import Path


def reload_tidb_agent_with_env(env: dict) -> object:
    sys.modules.pop("tidb_agent", None)
    os.environ.update(env)
    import tidb_agent  # noqa: WPS433

    return tidb_agent


class BranchAdminWaitTest(unittest.TestCase):
    def setUp(self):
        self._env_backup = os.environ.copy()
        self._tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env_backup)
        sys.modules.pop("tidb_agent", None)
        for root, _, files in os.walk(self._tmpdir, topdown=False):
            for name in files:
                Path(root, name).unlink(missing_ok=True)
            Path(root).rmdir()

    def test_waits_for_active_and_uses_endpoints(self):
        class DummyManager:
            def __init__(self):
                self.calls = 0
                self.refreshed = False

            def _cluster(self):
                return "cluster-1"

            def list_branches(self):
                return [{"displayName": "demo-branch", "branchId": "b1"}]

            def get_branch(self, branch_id: str):
                self.calls += 1
                if self.calls == 1:
                    return {"branchId": branch_id, "state": "CREATING"}
                if self.calls == 2 and not self.refreshed:
                    # ACTIVE but no endpoints yet
                    return {"branchId": branch_id, "state": "ACTIVE"}
                return {
                    "branchId": branch_id,
                    "state": "ACTIVE",
                    "endpoints": {"public": {"host": "h1", "port": 1234}},
                }

            def create_admin_user_for_branch(self, branch_name: str, cluster_id: str):
                self.refreshed = True
                return {"username": "u1", "password": "p1"}

        env = {"TIDBCLOUD_PROJECT_ID": "proj", "TIDB_HOST": ""}
        mod = reload_tidb_agent_with_env(env)
        mod.CREDENTIALS_FILE = self._tmpdir / "creds.csv"
        mod._branch_manager = DummyManager()
        mod.time = type("T", (), {"sleep": lambda *_: None})()  # avoid waiting

        creds = mod._ensure_admin_credential("demo-branch")
        self.assertEqual(creds["username"], "u1")
        self.assertEqual(creds["password"], "p1")
        self.assertEqual(creds["host"], "h1")
        self.assertEqual(creds["port"], 1234)
        self.assertEqual(creds["source"], "created")
        saved = mod._load_admin_credentials()
        self.assertIn(("demo-branch", "cluster-1"), saved)
        self.assertEqual(saved[("demo-branch", "cluster-1")]["host"], "h1")
        self.assertEqual(saved[("demo-branch", "cluster-1")]["port"], 1234)


if __name__ == "__main__":
    unittest.main()
