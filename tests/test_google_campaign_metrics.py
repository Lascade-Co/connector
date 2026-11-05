import datetime
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from google.ads.googleads.errors import GoogleAdsException

from pipelines.google import sources


def _enum(name):
    return SimpleNamespace(name=name)


def _campaign_row():
    return SimpleNamespace(
        segments=SimpleNamespace(date="2026-10-20"),
        customer=SimpleNamespace(
            id=7798955907,
            descriptive_name="Hotels70",
            currency_code="AED",
            time_zone="Asia/Dubai",
        ),
        campaign=SimpleNamespace(
            id=23480706613,
            name="Performance Max - WW",
            advertising_channel_type=_enum("PERFORMANCE_MAX"),
        ),
        metrics=SimpleNamespace(
            cost_micros=1234560000,
            impressions=9876,
            clicks=543,
            conversions=12.5,
            conversions_value=4321.75,
            conversions_by_conversion_date=11.0,
            conversions_value_by_conversion_date=4000.25,
        ),
    )


def _customer_client_row():
    return SimpleNamespace(
        customer_client=SimpleNamespace(
            id=6431034448,
            descriptive_name="Hotelsoff",
            currency_code="AED",
            time_zone="Asia/Dubai",
            manager=False,
            status=_enum("ENABLED"),
            level=1,
        )
    )


class GoogleCampaignMetricsTests(unittest.TestCase):
    def test_maps_every_field_and_keeps_click_date_naming(self):
        with patch.object(sources, "run_query", return_value=iter([_campaign_row()])):
            rows = list(sources.campaign_metrics(client=None, customer_id="7798955907", group_name="d1"))

        self.assertEqual(
            rows,
            [
                {
                    "date": "2026-10-20",
                    "account_id": 7798955907,
                    "account_name": "Hotels70",
                    "account_currency_code": "AED",
                    "account_time_zone": "Asia/Dubai",
                    "managing_system": "d1",
                    "campaign_id": 23480706613,
                    "campaign_name": "Performance Max - WW",
                    "advertising_channel_type": "PERFORMANCE_MAX",
                    "cost_micros": 1234560000,
                    "impressions": 9876,
                    "clicks": 543,
                    # metrics.conversions is click-date attributed; the name matches
                    # ads_metrics so downstream SQL can swap tables cleanly
                    "conversions_by_click_date": 12.5,
                    "conversions_value_by_click_date": 4321.75,
                    "conversions_by_conversion_date": 11.0,
                    "conversions_value_by_conversion_date": 4000.25,
                }
            ],
        )

    def test_reads_from_campaign_so_performance_max_is_included(self):
        with patch.object(sources, "run_query", return_value=iter([])) as run_query:
            list(sources.campaign_metrics(client=None, customer_id="1", group_name="d1", days_back=7))

        query = run_query.call_args.args[2]
        self.assertIn("FROM campaign", query)
        self.assertNotIn("ad_group_ad", query)

    def test_requests_exactly_the_days_back_window(self):
        with patch.object(sources, "run_query", return_value=iter([])) as run_query:
            list(sources.campaign_metrics(client=None, customer_id="1", group_name="d1", days_back=7))

        end = datetime.date.today()
        start = end - datetime.timedelta(days=7)
        self.assertIn(f"BETWEEN '{start}' AND '{end}'", run_query.call_args.args[2])

    def test_column_hints_pin_the_money_and_count_types(self):
        # dlt inferred-type drift already left google___google_ads carrying an
        # orphaned target_roas__v_double beside target_roas. These hints are the guard,
        # and they only take effect when the table is created.
        columns = sources.campaign_metrics(
            client=None, customer_id="1", group_name="d1"
        ).compute_table_schema()["columns"]

        self.assertEqual(columns["cost_micros"]["data_type"], "bigint")
        self.assertEqual(columns["impressions"]["data_type"], "bigint")
        self.assertEqual(columns["clicks"]["data_type"], "bigint")
        self.assertEqual(columns["conversions_by_click_date"]["data_type"], "double")
        self.assertEqual(columns["conversions_value_by_click_date"]["data_type"], "double")
        self.assertEqual(columns["conversions_by_conversion_date"]["data_type"], "double")
        self.assertEqual(columns["conversions_value_by_conversion_date"]["data_type"], "double")

    def test_warns_when_an_account_does_not_bill_in_aed(self):
        row = _campaign_row()
        row.customer.currency_code = "GBP"

        with patch.object(sources, "run_query", return_value=iter([row])):
            with self.assertLogs(level="WARNING") as logs:
                list(sources.campaign_metrics(client=None, customer_id="1", group_name="d1"))

        self.assertIn("GBP", "".join(logs.output))


class GoogleCustomerClientsTests(unittest.TestCase):
    def test_exposes_currency_time_zone_and_status(self):
        with patch.object(sources, "run_query", return_value=iter([_customer_client_row()])):
            rows = list(sources.customer_clients(client=None, mcc_id="7712523697", group_name="d1"))

        self.assertEqual(
            rows,
            [
                {
                    "account_id": 6431034448,
                    "descriptive_name": "Hotelsoff",
                    "currency_code": "AED",
                    "time_zone": "Asia/Dubai",
                    "is_manager": False,
                    "status": "ENABLED",
                    "level": 1,
                    "managing_system": "d1",
                }
            ],
        )

    def test_queries_the_mcc_not_a_child_account(self):
        with patch.object(sources, "run_query", return_value=iter([])) as run_query:
            list(sources.customer_clients(client=None, mcc_id="7712523697", group_name="d1"))

        self.assertEqual(run_query.call_args.args[1], "7712523697")

    def test_a_failed_mcc_query_does_not_take_down_the_spend_load(self):
        # customer_clients shares one pipe.run() with every account's spend, and an
        # MCC-scoped query is a path this pipeline has never exercised.
        error = GoogleAdsException(None, None, None, None)

        with patch.object(sources, "run_query", side_effect=error):
            with self.assertLogs(level="WARNING"):
                rows = list(sources.customer_clients(client=None, mcc_id="7712523697", group_name="d1"))

        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
