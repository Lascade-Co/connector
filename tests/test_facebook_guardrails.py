import unittest
from unittest.mock import patch

import utils


class FacebookLocalGroupGuardTests(unittest.TestCase):
    def test_allows_exact_d1c_locally(self):
        with patch.dict(utils.os.environ, {}, clear=True):
            utils.enforce_local_facebook_group("d1c")

    def test_rejects_every_other_local_group(self):
        with patch.dict(utils.os.environ, {}, clear=True):
            for group_name in ("d1", "d1a", "d1b", "D1C", "e1"):
                with self.subTest(group_name=group_name):
                    with self.assertRaisesRegex(SystemExit, "restricted to group 'd1c'"):
                        utils.enforce_local_facebook_group(group_name)

    def test_github_actions_bypasses_local_group_restriction(self):
        github_runner_env = {
            "GITHUB_ACTIONS": "TrUe",
            "CI": "true",
            "GITHUB_RUN_ID": "12345",
            "GITHUB_REPOSITORY": "example/connector",
            "RUNNER_NAME": "GitHub Actions 1",
        }
        with patch.dict(utils.os.environ, github_runner_env, clear=True):
            for group_name in ("d1", "e1", "subscription"):
                with self.subTest(group_name=group_name):
                    utils.enforce_local_facebook_group(group_name)


if __name__ == "__main__":
    unittest.main()
