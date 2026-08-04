import unittest
from unittest.mock import patch

from pipelines.esim_facebook import esim_facebook_pipeline
from pipelines.facebook import facebook_ads_pipeline
from pipelines.subscription_facebook import subscription_facebook_pipeline


GITHUB_RUNNER_ENV = {
    "GITHUB_ACTIONS": "true",
    "CI": "true",
    "GITHUB_RUN_ID": "12345",
    "GITHUB_REPOSITORY": "example/connector",
    "RUNNER_NAME": "GitHub Actions 1",
}


class FacebookRunnerOrderTests(unittest.TestCase):
    def test_all_facebook_runners_commit_insights_before_structural_resources(self):
        modules = (
            (facebook_ads_pipeline, "facebook"),
            (esim_facebook_pipeline, "esim_facebook"),
            (subscription_facebook_pipeline, "subscription_facebook"),
        )

        for module, platform in modules:
            with self.subTest(platform=platform):
                def insights(creds, group_name):
                    return ("insights", creds[0]["account_id"], group_name)

                def structural(creds, group_name):
                    return ("structural", creds[0]["account_id"], group_name)

                with (
                    patch.object(module.sys, "argv", ["main.py", platform, "any-group"]),
                    patch.object(
                        module,
                        "get_for_group",
                        return_value=({"token": "secret"}, ["act-1"]),
                    ) as get_for_group,
                    patch.object(module, "all_sources", [insights, structural]),
                    patch.object(module, "reset_partial_creative_accounts"),
                    patch.object(module, "get_partial_creative_accounts", return_value=()),
                    patch.object(module.dlt, "pipeline") as pipeline_factory,
                    patch.dict(module.os.environ, GITHUB_RUNNER_ENV, clear=True),
                ):
                    module.run()

                get_for_group.assert_called_once_with("any-group", platform)
                self.assertEqual(
                    pipeline_factory.return_value.run.call_args_list[0].args[0],
                    ("insights", "act-1", "any-group"),
                )
                self.assertEqual(
                    pipeline_factory.return_value.run.call_args_list[1].args[0],
                    [("structural", "act-1", "any-group")],
                )

    def test_facebook_backfills_skip_current_state_resources(self):
        modules = (
            (facebook_ads_pipeline, "facebook", "FB_BACKFILL_DAYS"),
            (esim_facebook_pipeline, "esim_facebook", "ESIM_FB_BACKFILL_DAYS"),
            (
                subscription_facebook_pipeline,
                "subscription_facebook",
                "SUB_FB_BACKFILL_DAYS",
            ),
        )

        for module, platform, backfill_env in modules:
            with self.subTest(platform=platform):
                insights = lambda *_args: ("insights", platform)
                structural = lambda *_args: ("structural", platform)
                env = {**GITHUB_RUNNER_ENV, backfill_env: "30"}
                with (
                    patch.object(module.sys, "argv", ["main.py", platform, "any-group"]),
                    patch.object(
                        module,
                        "get_for_group",
                        return_value=({"token": "secret"}, ["act-1"]),
                    ),
                    patch.object(module, "all_sources", [insights, structural]),
                    patch.object(module, "reset_partial_creative_accounts"),
                    patch.object(module, "get_partial_creative_accounts", return_value=()),
                    patch.object(module.dlt, "pipeline") as pipeline_factory,
                    patch.dict(module.os.environ, env, clear=True),
                ):
                    module.run()

                pipeline_factory.return_value.run.assert_called_once_with(
                    ("insights", platform)
                )


if __name__ == "__main__":
    unittest.main()
