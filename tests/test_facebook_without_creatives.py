import functools
import unittest
from unittest.mock import Mock, patch

from pipelines.facebook import facebook_ads_pipeline as pipeline
from pipelines.facebook import sources


class MetadataOnlySource:
    def __init__(self):
        self.ads = [{"id": "ad-1", "name": "Search ad", "campaign_id": "campaign-1"}]
        self.campaigns = [{"id": "campaign-1", "name": "Search campaign"}]
        self.ad_sets = [{"id": "adset-1", "name": "Search ad set"}]

    @property
    def ad_creatives(self):
        raise AssertionError("The metrics pipeline must not access creatives")


def exhaust_resource(factory, *args, **kwargs):
    """Exercise resource generators without starting a dlt destination."""
    if isinstance(factory, functools.partial):
        kwargs = {**factory.keywords, **kwargs}
        factory = factory.func
    return list(factory._pipe.gen(*args, **kwargs))


class FacebookWithoutCreativesTests(unittest.TestCase):
    def test_default_run_keeps_metrics_and_metadata_without_reading_creatives(self):
        loaded = {}
        bounds = {
            "report_start_date": "2026-10-18",
            "report_end_date": "2026-10-19",
        }

        def load_insights(destination, resource, creds, group, **kwargs):
            self.assertEqual(kwargs, {"backfill_env_name": "FB_BACKFILL_DAYS"})
            loaded["insights"] = exhaust_resource(resource, creds, group, **bounds)
            # Truthy, or the caller treats the account as parked and skips its
            # current-state resources.
            return True

        def load_metadata(destination, resources, creds, group):
            for resource in resources:
                delegate = (
                    resource.func
                    if isinstance(resource, functools.partial)
                    else resource
                )
                loaded[delegate.name] = exhaust_resource(resource, creds, group)

        def fake_insights(cred, *, report_start_date, report_end_date, pre_flatten):
            self.assertEqual(report_start_date, bounds["report_start_date"])
            self.assertEqual(report_end_date, bounds["report_end_date"])
            yield pre_flatten({"ad_id": "ad-1", "spend": "10", "clicks": "3"})

        rates = Mock()
        rates.resolved_rates.return_value = []
        identity = lambda row: row
        env = {
            "GITHUB_ACTIONS": "true",
            "CI": "true",
            "GITHUB_RUN_ID": "12345",
            "GITHUB_REPOSITORY": "example/connector",
            "RUNNER_NAME": "GitHub Actions 1",
            # An inherited legacy setting must not re-enable creative calls.
            "FB_CREATIVE_REFRESH_MODE": "full",
        }
        with (
            patch.object(pipeline.sys, "argv", ["main.py", "facebook", "d2a"]),
            patch.dict(pipeline.os.environ, env, clear=True),
            patch.object(
                pipeline, "get_for_group",
                return_value=({"token": "secret"}, ["act-1"]),
            ),
            patch.object(pipeline, "clickhouse_destination", return_value="destination"),
            patch.object(pipeline.dlt, "pipeline") as destination,
            patch.object(pipeline, "AedRateProvider", return_value=rates),
            patch.object(pipeline, "budget_currency_map", return_value=identity),
            patch.object(pipeline, "insights_currency_map", return_value=identity),
            patch.object(
                sources, "ads_src", side_effect=lambda cred: MetadataOnlySource()
            ) as ads,
            patch.object(sources, "insights_src", side_effect=fake_insights),
            patch.object(
                pipeline, "run_insights_isolated", side_effect=load_insights
            ) as insights,
            patch.object(
                pipeline, "run_structural_resources", side_effect=load_metadata
            ),
        ):
            pipeline.run()

        destination.return_value.sync_destination.assert_called_once_with()
        insights.assert_called_once()
        self.assertEqual(ads.call_count, 3)
        self.assertEqual(set(loaded), {"insights", "ads", "campaigns", "ad_sets"})
        self.assertEqual(loaded["insights"], [{
            "ad_id": "ad-1", "spend": "10", "clicks": "3",
            "account_id": "act-1", "managing_system": "d2a",
        }])
        for name, expected in (
            ("ads", "Search ad"),
            ("campaigns", "Search campaign"),
            ("ad_sets", "Search ad set"),
        ):
            self.assertEqual(loaded[name][0]["name"], expected)
            self.assertEqual(loaded[name][0]["account_id"], "act-1")
            self.assertEqual(loaded[name][0]["managing_system"], "d2a")
