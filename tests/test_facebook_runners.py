import functools
import unittest
import tomllib
from pathlib import Path
from unittest.mock import Mock, call, patch

import pendulum

from pipelines.esim_facebook import esim_facebook_pipeline
from pipelines.facebook import facebook_ads_pipeline
from pipelines.facebook import runner
from pipelines.subscription_facebook import subscription_facebook_pipeline


GITHUB_RUNNER_ENV = {
    "GITHUB_ACTIONS": "true",
    "CI": "true",
    "GITHUB_RUN_ID": "12345",
    "GITHUB_REPOSITORY": "example/connector",
    "RUNNER_NAME": "GitHub Actions 1",
}


class FacebookRunnerOrderTests(unittest.TestCase):
    def assert_ran_insights(self, run_windows, insights, group_name, backfill_env):
        """The travel runner binds its currency map onto the insights resource."""

        self.assertEqual(run_windows.call_count, 1)
        pipeline_arg, insights_arg, creds_arg, group_arg = run_windows.call_args.args
        delegate = (
            insights_arg.func
            if isinstance(insights_arg, functools.partial)
            else insights_arg
        )
        self.assertIs(delegate, insights)
        self.assertEqual(creds_arg, [{"account_id": "act-1", "token": "secret"}])
        self.assertEqual(group_arg, group_name)
        self.assertEqual(
            run_windows.call_args.kwargs, {"backfill_env_name": backfill_env}
        )
        return pipeline_arg

    def test_all_facebook_runners_commit_insights_before_structural_resources(self):
        modules = (
            (facebook_ads_pipeline, "facebook"),
            (esim_facebook_pipeline, "esim_facebook"),
            (subscription_facebook_pipeline, "subscription_facebook"),
        )

        for module, platform in modules:
            with self.subTest(platform=platform):
                insights = Mock(name=f"{platform}_insights")

                def structural(creds, group_name):
                    return ("structural", creds[0]["account_id"], group_name)

                with (
                    patch.object(
                        module.sys, "argv", ["main.py", platform, "any-group"]
                    ),
                    patch.object(
                        module,
                        "get_for_group",
                        return_value=({"token": "secret"}, ["act-1"]),
                    ) as get_for_group,
                    patch.object(module, "all_sources", [insights, structural]),
                    patch.object(
                        module, "clickhouse_destination", return_value="destination"
                    ),
                    patch.object(module, "run_insights_in_windows") as run_windows,
                    patch.object(module, "reset_partial_resources"),
                    patch.object(module, "run_structural_resources") as run_structural,
                    patch.object(module, "raise_for_partial_resources"),
                    patch.object(module.dlt, "pipeline") as pipeline_factory,
                    patch.dict(module.os.environ, GITHUB_RUNNER_ENV, clear=True),
                ):
                    module.run()

                get_for_group.assert_called_once_with("any-group", platform)
                pipeline_factory.return_value.sync_destination.assert_called_once_with()
                pipeline_arg = self.assert_ran_insights(
                    run_windows,
                    insights,
                    "any-group",
                    {
                        "facebook": "FB_BACKFILL_DAYS",
                        "esim_facebook": "ESIM_FB_BACKFILL_DAYS",
                        "subscription_facebook": "SUB_FB_BACKFILL_DAYS",
                    }[platform],
                )
                self.assertEqual(pipeline_arg, pipeline_factory.return_value)
                run_structural.assert_called_once_with(
                    pipeline_factory.return_value,
                    [structural],
                    [{"account_id": "act-1", "token": "secret"}],
                    "any-group",
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
                    patch.object(
                        module.sys, "argv", ["main.py", platform, "any-group"]
                    ),
                    patch.object(
                        module,
                        "get_for_group",
                        return_value=({"token": "secret"}, ["act-1"]),
                    ),
                    patch.object(module, "all_sources", [insights, structural]),
                    patch.object(
                        module, "clickhouse_destination", return_value="destination"
                    ),
                    patch.object(module, "run_insights_in_windows") as run_windows,
                    patch.object(module, "reset_partial_resources"),
                    patch.object(module, "run_structural_resources") as run_structural,
                    patch.object(module, "raise_for_partial_resources"),
                    patch.object(module.dlt, "pipeline") as pipeline_factory,
                    patch.dict(module.os.environ, env, clear=True),
                ):
                    module.run()

                pipeline_arg = self.assert_ran_insights(
                    run_windows, insights, "any-group", backfill_env
                )
                self.assertEqual(pipeline_arg, pipeline_factory.return_value)
                run_structural.assert_not_called()


class SharedFacebookRunnerTests(unittest.TestCase):
    WINDOWS = [
        (
            pendulum.datetime(2026, 7, 20, tz="UTC"),
            pendulum.datetime(2026, 7, 27, tz="UTC"),
        ),
        (
            pendulum.datetime(2026, 7, 28, tz="UTC"),
            pendulum.datetime(2026, 8, 4, tz="UTC"),
        ),
        (
            pendulum.datetime(2026, 8, 5, tz="UTC"),
            pendulum.datetime(2026, 8, 5, tz="UTC"),
        ),
    ]

    def test_clickhouse_loader_workers_stay_below_http_pool_capacity(self):
        config_path = Path(__file__).parents[1] / ".dlt" / "config.toml"
        with config_path.open("rb") as config_file:
            config = tomllib.load(config_file)

        self.assertEqual(config["load"]["workers"], 4)

    def test_runs_each_insights_window_in_order_with_explicit_bounds(self):
        pipeline = Mock(state={"restored": True})
        insights = Mock(side_effect=lambda *_args, **kwargs: kwargs)
        creds = [{"account_id": "act-1", "token": "secret"}]

        with (
            patch.object(
                runner, "plan_insights_date_windows", return_value=self.WINDOWS
            ) as plan,
            patch.dict(runner.os.environ, {}, clear=True),
        ):
            runner.run_insights_in_windows(
                pipeline,
                insights,
                creds,
                "d1c",
                backfill_env_name="FB_BACKFILL_DAYS",
            )

        plan.assert_called_once_with(
            {"restored": True},
            "act-1",
            initial_load_past_days=30,
            attribution_window_days_lag=7,
            max_days=8,
        )
        self.assertEqual(
            insights.call_args_list,
            [
                call(
                    creds,
                    "d1c",
                    report_start_date="2026-07-20",
                    report_end_date="2026-07-27",
                ),
                call(
                    creds,
                    "d1c",
                    report_start_date="2026-07-28",
                    report_end_date="2026-08-04",
                ),
                call(
                    creds,
                    "d1c",
                    report_start_date="2026-08-05",
                    report_end_date="2026-08-05",
                ),
            ],
        )
        self.assertEqual(
            pipeline.run.call_args_list,
            [
                call(
                    {
                        "report_start_date": "2026-07-20",
                        "report_end_date": "2026-07-27",
                    }
                ),
                call(
                    {
                        "report_start_date": "2026-07-28",
                        "report_end_date": "2026-08-04",
                    }
                ),
                call(
                    {
                        "report_start_date": "2026-08-05",
                        "report_end_date": "2026-08-05",
                    }
                ),
            ],
        )

    def test_failed_window_stops_later_windows(self):
        pipeline = Mock(state={})
        pipeline.run.side_effect = [None, RuntimeError("ClickHouse timeout")]
        insights = Mock(side_effect=lambda *_args, **kwargs: kwargs)

        with patch.object(
            runner, "plan_insights_date_windows", return_value=self.WINDOWS
        ):
            with self.assertRaisesRegex(RuntimeError, "ClickHouse timeout"):
                runner.run_insights_in_windows(
                    pipeline,
                    insights,
                    [{"account_id": "act-1", "token": "secret"}],
                    "d1c",
                    backfill_env_name="FB_BACKFILL_DAYS",
                )

        self.assertEqual(pipeline.run.call_count, 2)
        self.assertEqual(insights.call_count, 2)

    def test_backfill_days_and_window_days_are_parsed_from_environment(self):
        pipeline = Mock(state={"restored": True})
        with (
            patch.object(runner, "plan_insights_date_windows", return_value=[]) as plan,
            patch.dict(
                runner.os.environ,
                {"FB_BACKFILL_DAYS": "90", "FB_INSIGHTS_LOAD_WINDOW_DAYS": "4"},
                clear=True,
            ),
        ):
            runner.run_insights_in_windows(
                pipeline,
                Mock(),
                [{"account_id": "act-1", "token": "secret"}],
                "d1c",
                backfill_env_name="FB_BACKFILL_DAYS",
            )

        self.assertEqual(plan.call_args.kwargs["initial_load_past_days"], 90)
        self.assertEqual(plan.call_args.kwargs["max_days"], 4)
        self.assertEqual(plan.call_args.args[0], {})

    def test_suffixed_backfill_resumes_from_restored_checkpoint(self):
        pipeline = Mock(state={"restored": True})
        with (
            patch.object(runner, "plan_insights_date_windows", return_value=[]) as plan,
            patch.dict(
                runner.os.environ,
                {
                    "FB_BACKFILL_DAYS": "90",
                    "PIPELINE_NAME_SUFFIX": "_backfill_d1c_123",
                },
                clear=True,
            ),
        ):
            runner.run_insights_in_windows(
                pipeline,
                Mock(),
                [{"account_id": "act-1", "token": "secret"}],
                "d1c",
                backfill_env_name="FB_BACKFILL_DAYS",
            )

        self.assertEqual(plan.call_args.args[0], {"restored": True})

    def test_clickhouse_timeout_overrides_only_send_receive_timeout(self):
        with (
            patch.object(
                runner.dlt.destinations, "clickhouse", return_value="destination"
            ) as clickhouse,
            patch.dict(
                runner.os.environ,
                {"FB_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS": "1200"},
                clear=True,
            ),
        ):
            destination = runner.clickhouse_destination("clickhouse_esim")

        self.assertEqual(destination, "destination")
        clickhouse.assert_called_once_with(
            destination_name="clickhouse_esim",
            credentials={"send_receive_timeout": 1200},
        )

    def test_clickhouse_timeout_partial_credentials_keep_named_destination_secrets(
        self,
    ):
        with patch.dict(
            runner.os.environ,
            {
                "DESTINATION__CLICKHOUSE_ESIM__CREDENTIALS__HOST": "clickhouse.test",
                "DESTINATION__CLICKHOUSE_ESIM__CREDENTIALS__USERNAME": "etl-user",
                "DESTINATION__CLICKHOUSE_ESIM__CREDENTIALS__PASSWORD": "secret",
            },
            clear=True,
        ):
            destination = runner.clickhouse_destination("clickhouse_esim")
            config = destination.configuration(destination.spec(), accept_partial=True)

        self.assertEqual(config.credentials.host, "clickhouse.test")
        self.assertEqual(config.credentials.username, "etl-user")
        self.assertEqual(config.credentials.password, "secret")
        self.assertEqual(config.credentials.send_receive_timeout, 900)


if __name__ == "__main__":
    unittest.main()
