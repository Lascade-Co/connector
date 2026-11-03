"""Delivered-country Facebook Insights feed (travel ``fb___insights_country``)."""

import functools
import unittest
from unittest.mock import Mock, patch

from dlt.pipeline.exceptions import PipelineStepFailed

import facebook_ads as facebook_ads_module
from facebook_ads.exceptions import DegradedInsightsReport
from facebook_ads.insights import degraded_report_rows, merge_report_rows
from pipelines.facebook import facebook_ads_pipeline, runner
from pipelines.facebook import sources
from pipelines.facebook.creative_status import (
    INSIGHTS_COUNTRY_RESOURCE,
    account_has_quota_parking,
    get_partial_resources,
    mark_partial_resource,
    reset_partial_resources,
)
from pipelines.facebook.raw_sources import COUNTRY_INSIGHT_FIELDS


def _primary_key_columns(resource):
    columns = resource.compute_table_schema()["columns"]
    return {name for name, column in columns.items() if column.get("primary_key")}


def _country_row(country, **metrics):
    return {
        "campaign_id": "campaign-1",
        "adset_id": "no_adset_id",
        "ad_id": "no_ad_id",
        "date_start": "2026-10-01",
        "country": country,
        **metrics,
    }


class MergeReportRowsBreakdownTests(unittest.TestCase):
    def test_breakdown_field_keeps_rows_that_differ_only_by_country(self):
        rows = [_country_row("US", spend="3"), _country_row("GB", spend="2")]

        merged = merge_report_rows([rows], breakdown_fields=("country",))

        self.assertEqual(
            sorted((row["country"], row["spend"]) for row in merged),
            [("GB", "2"), ("US", "3")],
        )

    def test_default_key_collapses_countries_into_one_row(self):
        # Documents the bug the breakdown key fixes: without it every country of
        # one campaign-day shares the key and the last one silently wins.
        rows = [_country_row("US", spend="3"), _country_row("GB", spend="2")]

        merged = merge_report_rows([rows])

        self.assertEqual(merged, [_country_row("GB", spend="2")])

    def test_row_missing_the_breakdown_field_does_not_raise(self):
        # Only the merge tolerates it: country is in the primary key, so dlt
        # still rejects such a row downstream.
        row = _country_row("US")
        del row["country"]

        merged = merge_report_rows([[row]], breakdown_fields=("country",))

        self.assertEqual(merged, [row])


class CountryInsightsSourceRequestTests(unittest.TestCase):
    def _extract(self, rows=(), **source_kwargs):
        account = Mock()
        job = Mock()
        job.get_result.return_value = rows
        account.get_insights.return_value = job

        with (
            patch.object(facebook_ads_module, "get_ads_account", return_value=account),
            patch.object(facebook_ads_module, "execute_job", return_value=job),
            patch.object(
                facebook_ads_module.dlt.current, "resource_state", return_value={}
            ),
            patch.dict(facebook_ads_module.os.environ, {}, clear=True),
        ):
            source = facebook_ads_module.facebook_insights_source(
                account_id="act-1",
                access_token="secret",
                fields=("account_id", "date_start", "date_stop", "impressions"),
                report_start_date="2026-10-01",
                report_end_date="2026-10-01",
                **source_kwargs,
            )
            resource = source.resources["facebook_insights_act-1"]
            extracted = list(resource)
            primary_key = _primary_key_columns(resource)

        return extracted, account, primary_key

    @staticmethod
    def _item(country):
        item = Mock()
        item.export_all_data.return_value = {
            "account_id": "act-1",
            "campaign_id": "campaign-1",
            "date_start": "2026-10-01",
            "date_stop": "2026-10-01",
            "impressions": "10",
            "country": country,
        }
        return item

    def test_country_preset_sends_the_country_breakdown(self):
        _, account, _ = self._extract(breakdowns="ads_insights_country")

        query = account.get_insights.call_args.kwargs["params"]
        self.assertEqual(query["breakdowns"], ["country"])
        # Meta returns breakdown columns by itself and rejects them in fields.
        self.assertNotIn("country", query["fields"])

    def test_default_preset_sends_no_breakdowns_key(self):
        # Travel, eSIM and subscription feeds must keep sending today's request.
        _, account, _ = self._extract()

        query = account.get_insights.call_args.kwargs["params"]
        self.assertNotIn("breakdowns", query)

    def test_country_preset_adds_country_to_the_resource_primary_key(self):
        _, _, primary_key = self._extract(breakdowns="ads_insights_country")

        self.assertEqual(
            primary_key,
            {"campaign_id", "adset_id", "ad_id", "date_start", "country"},
        )

    def test_default_preset_primary_key_is_unchanged(self):
        _, _, primary_key = self._extract()

        self.assertEqual(primary_key, {"campaign_id", "adset_id", "ad_id", "date_start"})

    def test_every_country_of_one_campaign_day_is_extracted(self):
        extracted, _, _ = self._extract(
            rows=(self._item("US"), self._item("GB")),
            breakdowns="ads_insights_country",
            level="campaign",
        )

        self.assertEqual(sorted(row["country"] for row in extracted), ["GB", "US"])


class CountryDegradedSentinelTests(unittest.TestCase):
    def test_country_fields_request_no_spend_paired_cost_ratios(self):
        for ratio in ("cpc", "cpm", "cpp"):
            with self.subTest(ratio=ratio):
                self.assertNotIn(ratio, COUNTRY_INSIGHT_FIELDS)
        self.assertIn("ctr", COUNTRY_INSIGHT_FIELDS)

    def test_tiny_country_slice_with_zero_spend_is_not_flagged(self):
        row = {"spend": "0", "impressions": "5", "clicks": "0", "ctr": "0"}

        self.assertEqual(degraded_report_rows([row]), [])

    def test_zero_spend_beside_positive_cpm_would_be_flagged(self):
        # Why the country feed does not request cpm: a sub-unit spend slice that
        # Meta rounds to "0" would refuse an ordinary window.
        row = {"spend": "0", "impressions": "5", "cpm": "0.4"}

        self.assertEqual(degraded_report_rows([row]), [row])

    def test_zeroed_impressions_beside_positive_ctr_is_still_flagged(self):
        row = {"spend": "0", "impressions": "0", "clicks": "0", "ctr": "1.5"}

        self.assertEqual(degraded_report_rows([row]), [row])


class InsightsCountryResourceTests(unittest.TestCase):
    RAW_ROW = {
        "account_id": "act-raw",
        "account_name": "M4 - Hotel",
        "campaign_id": "campaign-1",
        "adset_id": "no_adset_id",
        "ad_id": "no_ad_id",
        "date_start": "2026-10-01",
        "date_stop": "2026-10-01",
        "country": "US",
        "spend": "12.5",
        "impressions": "100",
        "clicks": "4",
        "ctr": "4",
        "actions": [{"action_type": "link_click", "value": "4"}],
        "action_values": [{"action_type": "purchase", "value": "30"}],
        "actions_link_click": 4.0,
        "trial_starts": 0.0,
        "subscriptions": 0.0,
        "trial_start_value": 0.0,
        "subscription_value": 0.0,
    }

    def test_rows_drop_arrays_and_placeholders_and_carry_account_and_group(self):
        pre_flatten = Mock(name="pre_flatten")
        cred = {"account_id": "1", "token": "t"}

        with patch.object(
            sources, "country_insights_src", return_value=[dict(self.RAW_ROW)]
        ) as country_src:
            rows = list(
                sources.insights_country(
                    [cred],
                    "d1c",
                    report_start_date="2026-10-01",
                    report_end_date="2026-10-02",
                    pre_flatten=pre_flatten,
                )
            )

        country_src.assert_called_once_with(
            cred,
            report_start_date="2026-10-01",
            report_end_date="2026-10-02",
            pre_flatten=pre_flatten,
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        for dropped in (
            "actions",
            "action_values",
            "adset_id",
            "ad_id",
            "trial_starts",
            "subscriptions",
            "trial_start_value",
            "subscription_value",
        ):
            with self.subTest(dropped=dropped):
                self.assertNotIn(dropped, row)
        self.assertEqual(row["actions_link_click"], 4.0)
        self.assertEqual(row["country"], "US")
        self.assertEqual(row["account_id"], "1")
        self.assertEqual(row["managing_system"], "d1c")

    def test_table_is_keyed_by_campaign_day_and_country(self):
        resource = sources.insights_country([], "d1c")

        self.assertEqual(
            _primary_key_columns(resource),
            {"account_id", "campaign_id", "date_start", "country"},
        )


class CountryRunnerTests(unittest.TestCase):
    CREDS = [{"account_id": "act-1", "token": "secret"}]

    def setUp(self):
        reset_partial_resources()

    def tearDown(self):
        reset_partial_resources()

    @staticmethod
    def _chained_degraded():
        error = DegradedInsightsReport(
            "facebook_insights_act-1",
            "Meta returned a degraded Insights report for account act-1.",
        )
        try:
            raise PipelineStepFailed(
                Mock(pipeline_name="fb_ads_country_d1c"), "extract", None, error
            ) from error
        except PipelineStepFailed as wrapped:
            return wrapped

    def test_degraded_country_report_parks_the_country_resource(self):
        with patch.object(
            runner, "run_insights_in_windows", side_effect=self._chained_degraded()
        ):
            runner.run_insights_isolated(
                Mock(),
                Mock(),
                self.CREDS,
                "d1c",
                backfill_env_name="FB_BACKFILL_DAYS",
                resource_name=INSIGHTS_COUNTRY_RESOURCE,
            )

        self.assertEqual(get_partial_resources(), {"insights_country": ("act-1",)})
        # A report-generation fault, not throttling.
        self.assertFalse(account_has_quota_parking("act-1"))

    def test_isolated_forwards_the_default_initial_days(self):
        pipeline, insights = Mock(), Mock()

        with patch.object(runner, "run_insights_in_windows") as run_windows:
            runner.run_insights_isolated(
                pipeline,
                insights,
                self.CREDS,
                "d1c",
                backfill_env_name="FB_BACKFILL_DAYS",
                default_initial_days=92,
                resource_name=INSIGHTS_COUNTRY_RESOURCE,
            )

        run_windows.assert_called_once_with(
            pipeline,
            insights,
            self.CREDS,
            "d1c",
            backfill_env_name="FB_BACKFILL_DAYS",
            default_initial_days=92,
        )

    def _planned_initial_days(self, env):
        with (
            patch.object(runner, "plan_insights_date_windows", return_value=[]) as plan,
            patch.dict(runner.os.environ, env, clear=True),
        ):
            runner.run_insights_in_windows(
                Mock(state={}),
                Mock(),
                self.CREDS,
                "d1c",
                backfill_env_name="FB_BACKFILL_DAYS",
                default_initial_days=92,
            )
        return plan.call_args.kwargs["initial_load_past_days"]

    def test_default_initial_days_sets_the_first_load_without_a_backfill(self):
        self.assertEqual(self._planned_initial_days({}), 92)

    def test_backfill_env_still_overrides_the_default_initial_days(self):
        self.assertEqual(self._planned_initial_days({"FB_BACKFILL_DAYS": "10"}), 10)

    def test_country_parking_does_not_promise_nothing_was_written(self):
        # It parks on any failure, including a load step a later account's run
        # may still commit, so only the main feed's note says "wrote nothing".
        mark_partial_resource(INSIGHTS_COUNTRY_RESOURCE, "act-1")

        with self.assertRaises(RuntimeError) as raised:
            runner.raise_for_partial_resources("Facebook")

        message = str(raised.exception)
        self.assertIn("insights_country: act-1", message)
        self.assertIn("may or may not have committed", message)
        self.assertNotIn("wrote nothing", message)


class TravelPipelineCountryFeedTests(unittest.TestCase):
    ENV = {
        "GITHUB_ACTIONS": "true",
        "CI": "true",
        "GITHUB_RUN_ID": "12345",
        "GITHUB_REPOSITORY": "example/connector",
        "RUNNER_NAME": "GitHub Actions 1",
    }

    def setUp(self):
        reset_partial_resources()

    def tearDown(self):
        reset_partial_resources()

    def _run(self, env=None, country_error=None):
        module = facebook_ads_pipeline
        pipelines = {}
        events = []
        currency_map = Mock(name="insights_currency_map")

        def make_pipeline(**kwargs):
            return pipelines.setdefault(
                kwargs["pipeline_name"], Mock(name=kwargs["pipeline_name"])
            )

        def run_insights(pipeline, _resource, creds, _group, **kwargs):
            events.append(
                (kwargs.get("resource_name", "insights"), creds[0]["account_id"])
            )
            if country_error and kwargs.get("resource_name"):
                raise country_error

        def run_structural(_pipeline, _resources, creds, _group):
            events.append(("structural", creds[0]["account_id"]))

        def record_fx(_pipeline, _rates):
            events.append(("fx",))

        with (
            patch.object(module.sys, "argv", ["main.py", "facebook", "d1c"]),
            patch.dict(module.os.environ, {**self.ENV, **(env or {})}, clear=True),
            patch.object(module, "enforce_local_facebook_group"),
            patch.object(
                module,
                "get_for_group",
                return_value=({"token": "secret"}, ["act-1", "act-2"]),
            ),
            patch.object(module, "clickhouse_destination", return_value="destination"),
            patch.object(module.dlt, "pipeline", side_effect=make_pipeline) as factory,
            patch.object(module, "AedRateProvider"),
            patch.object(module, "budget_currency_map"),
            patch.object(module, "insights_currency_map", return_value=currency_map),
            patch.object(module, "reset_partial_resources"),
            patch.object(
                module, "run_insights_isolated", side_effect=run_insights
            ) as run_insights_mock,
            patch.object(module, "run_structural_resources", side_effect=run_structural),
            patch.object(
                module, "_record_fx_rates", side_effect=record_fx
            ) as record_fx_mock,
            patch.object(module, "raise_for_partial_resources") as raise_partial,
        ):
            module.run()

        return {
            "pipelines": pipelines,
            "factory": factory,
            "events": events,
            "run_insights": run_insights_mock,
            "record_fx": record_fx_mock,
            "raise_partial": raise_partial,
            "currency_map": currency_map,
        }

    def test_country_feed_uses_its_own_synced_pipeline(self):
        result = self._run()

        pipelines = result["pipelines"]
        self.assertEqual(set(pipelines), {"fb_ads_d1c", "fb_ads_country_d1c"})
        for name, pipeline in pipelines.items():
            with self.subTest(pipeline=name):
                # Without a sync every GitHub run starts from empty state.
                pipeline.sync_destination.assert_called_once_with()
        self.assertEqual(
            {call_.kwargs["dataset_name"] for call_ in result["factory"].call_args_list},
            {"fb"},
        )

    def test_country_feed_runs_per_account_after_main_loads_and_before_fx(self):
        result = self._run()

        self.assertEqual(
            result["events"],
            [
                ("insights", "act-1"),
                ("structural", "act-1"),
                ("insights", "act-2"),
                ("structural", "act-2"),
                ("insights_country", "act-1"),
                ("insights_country", "act-2"),
                ("fx",),
            ],
        )
        country_pipeline = result["pipelines"]["fb_ads_country_d1c"]
        country_calls = [
            call_
            for call_ in result["run_insights"].call_args_list
            if call_.kwargs.get("resource_name") == "insights_country"
        ]
        for call_, account_id in zip(country_calls, ("act-1", "act-2")):
            with self.subTest(account_id=account_id):
                pipeline_arg, resource_arg, creds_arg, group_arg = call_.args
                self.assertIs(pipeline_arg, country_pipeline)
                self.assertIsInstance(resource_arg, functools.partial)
                self.assertIs(resource_arg.func, sources.insights_country)
                self.assertIs(
                    resource_arg.keywords["pre_flatten"], result["currency_map"]
                )
                self.assertEqual(
                    creds_arg, [{"account_id": account_id, "token": "secret"}]
                )
                self.assertEqual(group_arg, "d1c")
                self.assertEqual(
                    call_.kwargs,
                    {
                        "backfill_env_name": "FB_BACKFILL_DAYS",
                        "default_initial_days": 92,
                        "resource_name": "insights_country",
                    },
                )
        # The FX audit goes to the main pipeline, not the country one.
        self.assertIs(
            result["record_fx"].call_args.args[0], result["pipelines"]["fb_ads_d1c"]
        )
        result["raise_partial"].assert_called_once_with("Facebook")

    def test_backfill_skips_the_country_feed(self):
        result = self._run(env={"FB_BACKFILL_DAYS": "30"})

        self.assertEqual(
            result["events"],
            [("insights", "act-1"), ("insights", "act-2"), ("fx",)],
        )

    def test_country_failure_parks_the_account_and_the_run_continues(self):
        result = self._run(country_error=RuntimeError("ClickHouse timeout"))

        # Every account is still attempted, then the FX audit and the
        # end-of-run report that turns the job red.
        self.assertEqual(
            result["events"][-3:],
            [("insights_country", "act-1"), ("insights_country", "act-2"), ("fx",)],
        )
        self.assertEqual(
            get_partial_resources(), {"insights_country": ("act-1", "act-2")}
        )
        result["raise_partial"].assert_called_once_with("Facebook")


if __name__ == "__main__":
    unittest.main()
