import datetime
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pipelines.google import backfill, date_range, sources


ACCOUNT_A = "2749505452"
ACCOUNT_B = "2196291315"
ACCOUNT_C = "1786885301"


def group(*accounts):
    return {
        "account_ids": list(accounts),
        "client_id": "client",
        "client_secret": "secret",
        "refresh_token": "refresh",
        "project_id": "project",
        "dev_token": "developer",
        "email": "example@example.com",
        "login_customer_id": "1234567890",
    }


class ReportingDatesTests(unittest.TestCase):
    def test_explicit_bounds_are_inclusive_and_override_days_back(self):
        self.assertEqual(
            date_range.reporting_dates(1, start_date="2026-09-01", end_date="2026-10-30"),
            (datetime.date(2026, 9, 1), datetime.date(2026, 10, 30)),
        )

    def test_single_day_and_leap_day_are_valid(self):
        self.assertEqual(
            date_range.reporting_dates(start_date="2024-02-29", end_date="2024-02-29"),
            (datetime.date(2024, 2, 29), datetime.date(2024, 2, 29)),
        )

    def test_bounds_must_be_supplied_together(self):
        for kwargs in ({"start_date": "2026-09-01"}, {"end_date": "2026-10-30"}):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(ValueError, "both"):
                date_range.reporting_dates(**kwargs)

    def test_rejects_noncanonical_and_invalid_dates(self):
        for value in ("", "2026-9-01", "20260901", "2026-02-29", "2026-09-31", "2026-09-01'", " 2026-09-01", None, 20260901):
            for bound in ("start_date", "end_date"):
                kwargs = {"start_date": "2026-01-01", "end_date": "2026-12-31", bound: value}
                with self.subTest(value=value, bound=bound), self.assertRaises(ValueError):
                    date_range.reporting_dates(**kwargs)

    def test_rejects_reversed_range(self):
        with self.assertRaisesRegex(ValueError, "on or before"):
            date_range.reporting_dates(start_date="2026-10-30", end_date="2026-09-01")

    def test_preserves_the_existing_rolling_window(self):
        today = datetime.date(2026, 10, 30)
        with patch.object(date_range.datetime, "date", wraps=datetime.date) as date_type:
            date_type.today.return_value = today
            self.assertEqual(date_range.reporting_dates(7), (datetime.date(2026, 10, 23), today))
            self.assertEqual(date_range.reporting_dates(), (datetime.date(2026, 9, 30), today))


class AccountSelectionTests(unittest.TestCase):
    def setUp(self):
        self.config = {"d1": group(ACCOUNT_A, ACCOUNT_B), "m4": group(ACCOUNT_C)}

    def test_all_accounts_in_one_group(self):
        self.assertEqual(backfill.select_accounts(self.config, "d1", "all"), {"d1": [ACCOUNT_A, ACCOUNT_B]})

    def test_one_account_in_one_group(self):
        self.assertEqual(backfill.select_accounts(self.config, "d1", ACCOUNT_A), {"d1": [ACCOUNT_A]})

    def test_all_accounts_in_all_configured_groups(self):
        self.assertEqual(
            backfill.select_accounts(self.config, "all", "all"),
            {"d1": [ACCOUNT_A, ACCOUNT_B], "m4": [ACCOUNT_C]},
        )

    def test_one_account_with_all_groups_only_selects_its_owner(self):
        self.assertEqual(backfill.select_accounts(self.config, "all", ACCOUNT_C), {"m4": [ACCOUNT_C]})

    def test_accepts_google_formatted_ids_and_surrounding_whitespace(self):
        self.assertEqual(backfill.select_accounts(self.config, " d1 ", " 274-950-5452 "), {"d1": [ACCOUNT_A]})
        self.assertEqual(backfill.select_accounts(self.config, "d1", " ALL "), {"d1": [ACCOUNT_A, ACCOUNT_B]})

    def test_normalizes_configured_ids_and_deduplicates_within_group(self):
        self.assertEqual(
            backfill.select_accounts({"d1": group(int(ACCOUNT_A), "274-950-5452")}, "d1", "all"),
            {"d1": [ACCOUNT_A]},
        )

    def test_rejects_invalid_or_unconfigured_account(self):
        for account in ("", "1", "27495054520", "274 950 5452", "../../secrets", "1111111111"):
            with self.subTest(account=account), self.assertRaises(ValueError):
                backfill.select_accounts(self.config, "d1", account)

    def test_rejects_unknown_or_invalid_group(self):
        for name in ("d2", "", "../d1", "d1; echo bad"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                backfill.select_accounts(self.config, name, "all")

    def test_rejects_empty_configuration_or_account_list(self):
        for config in ({}, {"d1": group()}, {"d1": {"account_ids": ACCOUNT_A}}, {"d1": {}}):
            with self.subTest(config=config), self.assertRaises(ValueError):
                backfill.select_accounts(config, "all", "all")

    def test_rejects_accounts_owned_by_multiple_selected_groups(self):
        config = {"d1": group(ACCOUNT_A), "m4": group(ACCOUNT_A)}
        for account in ("all", ACCOUNT_A):
            with self.subTest(account=account), self.assertRaisesRegex(ValueError, "multiple"):
                backfill.select_accounts(config, "all", account)
        self.assertEqual(backfill.select_accounts(config, "d1", ACCOUNT_A), {"d1": [ACCOUNT_A]})


class GoogleDateResourceTests(unittest.TestCase):
    def test_both_metric_queries_use_exact_requested_bounds(self):
        for source in (sources.ads_metrics, sources.conversion_actions):
            with self.subTest(source=source.name), patch.object(sources, "run_query", return_value=iter([])) as query:
                resource = source(client=None, customer_id=ACCOUNT_A, group_name="d1", start_date="2026-09-01", end_date="2026-10-30")
                self.assertEqual(list(resource), [])
                self.assertEqual(query.call_args.args[1], ACCOUNT_A)
                self.assertIn("BETWEEN '2026-09-01' AND '2026-10-30'", query.call_args.args[2])

    def test_both_metric_queries_preserve_rolling_scheduled_behavior(self):
        for source in (sources.ads_metrics, sources.conversion_actions):
            with self.subTest(source=source.name), patch.object(sources, "run_query", return_value=iter([])) as query:
                list(source(client=None, customer_id=ACCOUNT_A, group_name="d1", days_back=7))
                end = datetime.date.today()
                start = end - datetime.timedelta(days=7)
                self.assertIn(f"BETWEEN '{start}' AND '{end}'", query.call_args.args[2])

    def test_backfill_resources_are_merge_only_with_existing_primary_keys(self):
        expected = {
            "google_ads": {"account_id", "ad_id", "date"},
            "google_ads_conversion_actions": {"account_id", "ad_id", "date", "conversion_action"},
        }
        self.assertEqual({resource.name for resource in backfill.METRIC_SOURCES}, set(expected))
        for source in backfill.METRIC_SOURCES:
            resource = source(client=None, customer_id=ACCOUNT_A, group_name="d1")
            schema = resource.compute_table_schema()
            with self.subTest(source=source.name):
                self.assertEqual(schema["write_disposition"], "merge")
                self.assertEqual({name for name, column in schema["columns"].items() if column.get("primary_key")}, expected[source.name])


class BackfillRunnerTests(unittest.TestCase):
    def setUp(self):
        self.config = {"d1": group(ACCOUNT_A, ACCOUNT_B), "m4": group(ACCOUNT_C)}
        self.env = {
            "GOOGLE_BACKFILL_START_DATE": "2026-09-01",
            "GOOGLE_BACKFILL_END_DATE": "2026-10-30",
            "GOOGLE_BACKFILL_GROUP": "all",
            "GOOGLE_BACKFILL_ACCOUNT_ID": "all",
            "PIPELINE_NAME_SUFFIX": "_backfill_123_2",
        }

    def runner_context(self, env=None):
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch.dict(backfill.os.environ, self.env if env is None else env, clear=True))
        stack.enter_context(patch.object(backfill, "Path", return_value=SimpleNamespace(read_text=lambda: json.dumps(self.config))))
        credentials = stack.enter_context(patch.object(backfill, "GcpOAuthCredentials"))
        client = stack.enter_context(patch.object(backfill, "get_client"))
        factory = stack.enter_context(patch.object(backfill.dlt, "pipeline"))
        # dlt's lazy plugin imports inspect this public factory during resource iteration.
        factory.__name__ = "pipeline"
        return stack, credentials, client, factory

    def test_runs_each_selected_account_with_exact_dates_and_checks_load_result(self):
        stack, credentials, client, factory = self.runner_context()
        info = Mock()
        resources_seen = []

        def run_resources(resources):
            resources_seen.append(resources)
            for resource in resources:
                list(resource)
            return info

        factory.return_value.run.side_effect = run_resources
        with stack, patch.object(sources, "run_query", return_value=[]) as query:
            backfill.run()

        self.assertEqual(credentials.call_count, 2)
        self.assertEqual(client.call_count, 2)
        self.assertEqual([call.kwargs["pipeline_name"] for call in factory.call_args_list], ["google_ads_d1_backfill_123_2", "google_ads_m4_backfill_123_2"])
        self.assertEqual(len(resources_seen), 3)
        self.assertEqual([call.args[1] for call in query.call_args_list], [ACCOUNT_A, ACCOUNT_A, ACCOUNT_B, ACCOUNT_B, ACCOUNT_C, ACCOUNT_C])
        for call in query.call_args_list:
            self.assertIn("BETWEEN '2026-09-01' AND '2026-10-30'", call.args[2])
        info.raise_on_failed_jobs.assert_has_calls([unittest.mock.call(), unittest.mock.call(), unittest.mock.call()])

    def test_invalid_selection_is_rejected_before_authentication_or_loading(self):
        for change in ({"GOOGLE_BACKFILL_ACCOUNT_ID": "1111111111"}, {"GOOGLE_BACKFILL_GROUP": "d2"}, {"GOOGLE_BACKFILL_START_DATE": "2026-11-01"}):
            env = {**self.env, **change}
            stack, credentials, client, factory = self.runner_context(env)
            with self.subTest(change=change), stack, self.assertRaises(ValueError):
                backfill.run()
            credentials.assert_not_called()
            client.assert_not_called()
            factory.assert_not_called()

    def test_single_account_with_all_groups_never_loads_other_accounts(self):
        stack, _, client, factory = self.runner_context({**self.env, "GOOGLE_BACKFILL_ACCOUNT_ID": ACCOUNT_C})
        with stack:
            backfill.run()
        client.assert_called_once()
        factory.assert_called_once_with(pipeline_name="google_ads_m4_backfill_123_2", destination="clickhouse", dataset_name="google")
        factory.return_value.run.assert_called_once()

    def test_failed_load_does_not_write_a_success_summary_or_continue(self):
        with tempfile.TemporaryDirectory() as directory:
            summary = Path(directory) / "summary.md"
            stack, _, _, factory = self.runner_context({**self.env, "GITHUB_STEP_SUMMARY": str(summary)})
            factory.return_value.run.return_value.raise_on_failed_jobs.side_effect = RuntimeError("failed job")
            with stack, self.assertRaisesRegex(RuntimeError, "failed job"):
                backfill.run()
            self.assertEqual(factory.return_value.run.call_count, 1)
            self.assertFalse(summary.exists())

    def test_success_summary_lists_actual_accounts_and_inclusive_range(self):
        with tempfile.TemporaryDirectory() as directory:
            summary = Path(directory) / "summary.md"
            stack, _, _, _ = self.runner_context({**self.env, "GOOGLE_BACKFILL_ACCOUNT_ID": ACCOUNT_C, "GITHUB_STEP_SUMMARY": str(summary)})
            with stack:
                backfill.run()
            content = summary.read_text()
            self.assertIn("2026-09-01 through 2026-10-30 (inclusive)", content)
            self.assertIn(ACCOUNT_C, content)
            self.assertNotIn(ACCOUNT_A, content)
            self.assertIn("provisional", content)

    def test_empty_account_range_continues_and_writes_success_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            summary = Path(directory) / "summary.md"
            stack, _, _, factory = self.runner_context({**self.env, "GITHUB_STEP_SUMMARY": str(summary)})
            factory.return_value.run.return_value = None
            factory.return_value.last_trace = None
            with stack:
                backfill.run()
            self.assertEqual(factory.return_value.run.call_count, 3)
            self.assertEqual(factory.call_count, 2)
            content = summary.read_text()
            for account in (ACCOUNT_A, ACCOUNT_B, ACCOUNT_C):
                self.assertIn(account, content)
            self.assertIn("completed", content)


if __name__ == "__main__":
    unittest.main()
