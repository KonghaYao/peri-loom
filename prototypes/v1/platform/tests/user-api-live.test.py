import importlib.util
import json
import os
import pathlib
import tempfile
import unittest


source = pathlib.Path(__file__).with_name("user-api-live.py")
specification = importlib.util.spec_from_file_location("user_api_live", source)
user_api_live = importlib.util.module_from_spec(specification)
specification.loader.exec_module(user_api_live)


class UserAPILiveSafetyTests(unittest.TestCase):
    def test_connection_requires_owned_private_regular_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "connection.json"
            path.write_text(json.dumps({"endpoint": "https://127.0.0.1:9443/"}))
            path.chmod(0o600)
            self.assertEqual(user_api_live.private_connection(path)["endpoint"], "https://127.0.0.1:9443/")
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                user_api_live.private_connection(path)
            path.chmod(0o600)
            (root / "linked.json").symlink_to(path)
            with self.assertRaises(ValueError):
                user_api_live.private_connection(root / "linked.json")
            os.link(path, root / "hardlinked.json")
            with self.assertRaises(ValueError):
                user_api_live.private_connection(path)

    def test_connection_rejects_oversized_and_duplicate_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "connection.json"
            path.write_bytes(b" " * 65537)
            path.chmod(0o600)
            with self.assertRaises(ValueError):
                user_api_live.private_connection(path)
            path.write_text('{"endpoint":"a","endpoint":"b"}')
            with self.assertRaises(ValueError):
                user_api_live.private_connection(path)

    def test_catalog_oracle_rejects_equal_length_wrong_project_or_database(self):
        database = {"namespace": {"tenant": "org_alpha", "database": "ledger"}, "branch": "main"}
        identity = "org_alpha/ledger/main"
        correct = {"projects": {"alpha": {}}, "databases": {identity: {"id": database, "project": "alpha"}}}
        self.assertTrue(user_api_live.catalog_matches(correct, "alpha", database))
        wrong = {"projects": {"beta": {}}, "databases": {identity: {"id": database, "project": "beta"}}}
        self.assertFalse(user_api_live.catalog_matches(wrong, "alpha", database))
        wrong = {"projects": {"alpha": {}}, "databases": {"org_beta/ledger/main": {"id": database, "project": "alpha"}}}
        self.assertFalse(user_api_live.catalog_matches(wrong, "alpha", database))


if __name__ == "__main__":
    unittest.main()
