import unittest
from unittest.mock import patch

import pendulum

from facebook_ads import _is_splittable_insights_failure
from facebook_ads.exceptions import InsightsJobFailed, InsightsJobTimeout
from facebook_ads.helpers import execute_job
from facebook_ads.insights import (
    iter_date_windows,
    merge_report_rows,
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
            {"id": "job-1", "async_status": "Job Completed", "async_percent_completion": 100}
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
            {"id": "job-timeout", "async_status": "Job Running", "async_percent_completion": 0}
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
            [(left.to_date_string(), right.to_date_string()) for left, right in windows],
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


if __name__ == "__main__":
    unittest.main()
