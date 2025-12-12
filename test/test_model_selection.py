import importlib
import os
import sys
import unittest


def reload_tidb_agent():
    """Reload tidb_agent after env changes."""
    sys.modules.pop("tidb_agent", None)
    import tidb_agent  # noqa: WPS433

    return tidb_agent


class ModelSelectionTest(unittest.TestCase):
    def setUp(self):
        self._env_backup = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env_backup)
        sys.modules.pop("tidb_agent", None)

    def test_default_model_uses_nova_pro(self):
        os.environ.pop("STRANDS_MODEL_ID", None)
        os.environ.pop("BEDROCK_MODEL_ID", None)
        os.environ["AWS_REGION"] = "us-west-2"
        mod = reload_tidb_agent()

        model = mod._build_model()
        self.assertEqual(model.config["model_id"], "us.amazon.nova-pro-v1:0")
        self.assertEqual(model.client.meta.region_name, "us-west-2")
        self.assertEqual(mod.agent.model.config["model_id"], "us.amazon.nova-pro-v1:0")

    def test_env_override_model_id(self):
        os.environ["BEDROCK_MODEL_ID"] = "custom-model"
        os.environ["AWS_REGION"] = "us-east-1"
        mod = reload_tidb_agent()

        model = mod._build_model()
        self.assertEqual(model.config["model_id"], "custom-model")
        self.assertEqual(model.client.meta.region_name, "us-east-1")
        self.assertEqual(mod.agent.model.config["model_id"], "custom-model")


if __name__ == "__main__":
    unittest.main()
