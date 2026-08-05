import unittest
from unittest.mock import Mock, patch

import pendulum

import facebook_ads as facebook_ads_module
from facebook_ads import _is_splittable_insights_failure
from facebook_ads.exceptions import InsightsJobFailed, InsightsJobTimeout
from facebook_ads.helpers import execute_job
from facebook_ads.insights import (
    INSIGHTS_SOURCE_NAME,
    INSIGHTS_WINDOW_CHECKPOINT,
    iter_date_windows,
    merge_report_rows,
    plan_insights_date_windows,
    split_insight_fields,
)
from facebook_ads.settings import INSIGHTS_PRIMARY_KEY


class _FakeJob:
    def __init__(self, *states):
        self._states = list(states)
        self._index = -1
        self.api_get_calls = 0

    def api_get(self):
        self.api_get_calls += 1
        self._index = min(self._index + 1, len(self._states) - 1)
        return self

    def get(self, key, default=None):
        return self._states[self._index].get(key, default)

    def export_all_data(self):
        return dict(self._states[self._index])


class InsightsJobTests(unittest.TestCase):
    def test_returns_the_completed_job(self):
        job = _FakeJob(
            {
                "id": "job-1",
                "async_status": "Job Completed",
                "async_percent_completion": 100,
            }
        )

        result = execute_job(job)

        self.assertIs(result, job)
        self.assertEqual(job.api_get_calls, 1)

    def test_failed_and_skipped_jobs_raise_immediately_with_error_codes(self):
        for status in ("Job Failed", "Job Skipped"):
            with self.subTest(status=status):
                job = _FakeJob(
                    {
                        "id": "job-terminal",
                        "async_status": status,
                        "async_percent_completion": 42,
                        "error_user_msg": "Report could not be produced",
                        "error_code": "80004",
                        "error_subcode": "2446079",
                    }
                )
                with patch("facebook_ads.helpers.time.sleep") as sleep:
                    with self.assertRaises(InsightsJobFailed) as raised:
                        execute_job(job)

                self.assertEqual(job.api_get_calls, 1)
                sleep.assert_not_called()
                self.assertEqual(raised.exception.status, status)
                self.assertEqual(raised.exception.error_code, 80004)
                self.assertEqual(raised.exception.error_subcode, 2446079)
                self.assertIn("Report could not be produced", str(raised.exception))

    def test_timeout_only_polls_the_original_job_and_never_resubmits(self):
        job = _FakeJob(
            {
                "id": "job-timeout",
                "async_status": "Job Running",
                "async_percent_completion": 0,
            }
        )
        with (
            patch("facebook_ads.helpers.time.time", side_effect=[100.0, 106.0]),
            patch("facebook_ads.helpers.time.sleep") as sleep,
        ):
            with self.assertRaises(InsightsJobTimeout):
                execute_job(job, insights_max_wait_to_start_seconds=5)

        self.assertEqual(job.api_get_calls, 1)
        sleep.assert_not_called()

    def test_completed_status_below_100_percent_still_times_out(self):
        job = _FakeJob(
            {
                "id": "job-incomplete",
                "async_status": "Job Completed",
                "async_percent_completion": 99,
            }
        )
        with (
            patch("facebook_ads.helpers.time.time", side_effect=[100.0, 106.0]),
            patch("facebook_ads.helpers.time.sleep") as sleep,
        ):
            with self.assertRaises(InsightsJobTimeout):
                execute_job(
                    job,
                    insights_max_wait_to_start_seconds=100,
                    insights_max_wait_to_finish_seconds=5,
                )

        self.assertEqual(job.api_get_calls, 1)
        sleep.assert_not_called()


class InsightsPlanningTests(unittest.TestCase):
    END_DATE = pendulum.datetime(2026, 8, 5, tz="UTC")

    @staticmethod
    def _state(account_id, *, checkpoint=None, incremental=None):
        resource_state = {}
        if checkpoint is not None:
            resource_state[INSIGHTS_WINDOW_CHECKPOINT] = checkpoint
        if incremental is not None:
            resource_state["incremental"] = {"date_start": {"last_value": incremental}}
        return {
            "sources": {
                INSIGHTS_SOURCE_NAME: {
                    "resources": {
                        f"facebook_insights_{account_id}": resource_state,
                    }
                }
            }
        }

    def _plan(self, state, **overrides):
        options = {
            "initial_load_past_days": 30,
            "attribution_window_days_lag": 7,
            "max_days": 8,
            "end_date": self.END_DATE,
        }
        options.update(overrides)
        return plan_insights_date_windows(state, "act-1", **options)

    def test_new_state_starts_at_initial_window_and_covers_today(self):
        windows = self._plan({})

        self.assertEqual(windows[0][0].to_date_string(), "2026-07-06")
        self.assertEqual(windows[-1][1].to_date_string(), "2026-08-05")
        self.assertTrue(all((end - start).days < 8 for start, end in windows))

    def test_completed_window_checkpoint_is_preferred_over_incremental_cursor(self):
        state = self._state(
            "act-1",
            checkpoint="2026-08-01",
            incremental="2026-07-15",
        )

        windows = self._plan(state)

        self.assertEqual(windows[0][0].to_date_string(), "2026-07-25")
        self.assertEqual(windows[-1][1].to_date_string(), "2026-08-05")

    def test_existing_pipeline_falls_back_to_incremental_cursor(self):
        state = self._state("act-1", incremental="2026-07-30T00:00:00Z")

        windows = self._plan(state)

        self.assertEqual(windows[0][0].to_date_string(), "2026-07-23")

    def test_invalid_checkpoint_falls_back_to_valid_incremental_cursor(self):
        state = self._state(
            "act-1",
            checkpoint="not-a-date",
            incremental="2026-07-30",
        )

        with self.assertLogs(level="WARNING") as logs:
            windows = self._plan(state)

        self.assertEqual(windows[0][0].to_date_string(), "2026-07-23")
        self.assertIn("Ignoring invalid Facebook Insights", "\n".join(logs.output))

    def test_initial_window_is_clamped_to_meta_retention(self):
        windows = self._plan({}, initial_load_past_days=10_000)

        self.assertEqual(windows[0][0], self.END_DATE.subtract(months=37))

    def test_only_data_volume_failures_are_split(self):
        auth = InsightsJobFailed(
            "facebook_insights",
            "Permission denied",
            status="Job Failed",
            error_code=190,
        )
        too_large = InsightsJobFailed(
            "facebook_insights",
            "Report could not be produced",
            status="Job Failed",
            error_code=100,
            error_subcode=1487534,
        )

        self.assertFalse(_is_splittable_insights_failure(auth))
        self.assertTrue(_is_splittable_insights_failure(too_large))

    def test_unique_metrics_are_split_from_core_with_row_identity(self):
        fields = [
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
            "date_start",
            "date_stop",
            "impressions",
            "actions",
            "reach",
            "frequency",
            "unique_clicks",
        ]

        core, unique = split_insight_fields(fields)

        self.assertIn("impressions", core)
        self.assertIn("actions", core)
        self.assertNotIn("reach", core)
        self.assertNotIn("frequency", core)
        self.assertNotIn("actions", unique)
        self.assertEqual(
            set(unique),
            {
                "account_id",
                "campaign_id",
                "adset_id",
                "ad_id",
                "date_start",
                "date_stop",
                "reach",
                "frequency",
                "unique_clicks",
            },
        )

    def test_iterates_inclusive_non_overlapping_eight_day_windows(self):
        start = pendulum.datetime(2026, 1, 1, tz="UTC")
        end = pendulum.datetime(2026, 1, 20, tz="UTC")

        windows = list(iter_date_windows(start, end, max_days=8))

        self.assertEqual(
            [
                (left.to_date_string(), right.to_date_string())
                for left, right in windows
            ],
            [
                ("2026-01-01", "2026-01-08"),
                ("2026-01-09", "2026-01-16"),
                ("2026-01-17", "2026-01-20"),
            ],
        )

    def test_outer_merges_on_every_primary_key_field(self):
        self.assertEqual(
            INSIGHTS_PRIMARY_KEY,
            ("campaign_id", "adset_id", "ad_id", "date_start"),
        )
        core_rows = [
            {
                "campaign_id": "campaign-1",
                "adset_id": "adset-1",
                "ad_id": "ad-1",
                "date_start": "2026-01-01",
                "impressions": 10,
            },
            {
                "campaign_id": "campaign-2",
                "adset_id": "adset-2",
                "ad_id": "ad-1",
                "date_start": "2026-01-01",
                "impressions": 20,
            },
        ]
        unique_rows = [
            {
                "campaign_id": "campaign-1",
                "adset_id": "adset-1",
                "ad_id": "ad-1",
                "date_start": "2026-01-01",
                "reach": 7,
            },
            {
                "campaign_id": "campaign-3",
                "adset_id": "adset-3",
                "ad_id": "ad-3",
                "date_start": "2026-01-02",
                "reach": 3,
            },
        ]

        merged = merge_report_rows([core_rows, unique_rows])
        by_key = {
            tuple(row[field] for field in INSIGHTS_PRIMARY_KEY): row for row in merged
        }

        self.assertEqual(len(by_key), 3)
        self.assertEqual(
            by_key[("campaign-1", "adset-1", "ad-1", "2026-01-01")],
            {
                **core_rows[0],
                "reach": 7,
            },
        )
        self.assertEqual(
            by_key[("campaign-2", "adset-2", "ad-1", "2026-01-01")]["impressions"],
            20,
        )
        self.assertEqual(
            by_key[("campaign-3", "adset-3", "ad-3", "2026-01-02")]["reach"],
            3,
        )


class InsightsResultPagingTests(unittest.TestCase):
    def _extract(
        self,
        env,
        rows=(),
        report_start_date="2026-08-01",
        report_end_date="2026-08-01",
    ):
        account = Mock()
        job = Mock()
        job.get_result.return_value = rows
        account.get_insights.return_value = job
        resource_state = {}

        with (
            patch.object(facebook_ads_module, "get_ads_account", return_value=account),
            patch.object(facebook_ads_module, "execute_job", return_value=job),
            patch.object(
                facebook_ads_module.dlt.current,
                "resource_state",
                return_value=resource_state,
            ),
            patch.dict(facebook_ads_module.os.environ, env, clear=True),
        ):
            source = facebook_ads_module.facebook_insights_source(
                account_id="act-1",
                access_token="secret",
                fields=("account_id", "date_start", "date_stop", "impressions"),
                report_start_date=report_start_date,
                report_end_date=report_end_date,
            )
            extracted = list(source.resources["facebook_insights_act-1"])

        return extracted, job, resource_state

    def test_completed_report_download_defaults_to_100_rows(self):
        _, job, _ = self._extract({})
        self.assertEqual(job.get_result.call_args.kwargs["params"]["limit"], 100)

    def test_completed_report_download_honors_result_page_size_environment(self):
        _, job, _ = self._extract({"FB_INSIGHTS_RESULT_PAGE_SIZE": "25"})
        self.assertEqual(job.get_result.call_args.kwargs["params"]["limit"], 25)

    def test_explicit_window_keeps_rows_on_both_date_boundaries(self):
        def item(date_start):
            row = Mock()
            row.export_all_data.return_value = {
                "account_id": "act-1",
                "campaign_id": "campaign-1",
                "adset_id": "adset-1",
                "ad_id": f"ad-{date_start}",
                "date_start": date_start,
                "date_stop": date_start,
                "impressions": "1",
            }
            return row

        extracted, _, resource_state = self._extract(
            {},
            rows=(item("2026-08-01"), item("2026-08-02")),
            report_end_date="2026-08-02",
        )

        self.assertEqual(
            [row["date_start"] for row in extracted],
            ["2026-08-01", "2026-08-02"],
        )
        self.assertEqual(
            resource_state[INSIGHTS_WINDOW_CHECKPOINT],
            "2026-08-02",
        )


if __name__ == "__main__":
    unittest.main()
