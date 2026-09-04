import unittest

from pipelines.esim.constants import DATASET_COLUMN_HINTS
from pipelines.esim.sources import make_incremental_resource


class OrdersResourceTests(unittest.TestCase):
    def test_resource_applies_additive_column_hints(self):
        resource = make_incremental_resource(
            dataset_name="orders",
            config={
                "watermark_field": "updated_at",
                "primary_key": "id",
                "write_disposition": "merge",
                "default_limit": 500,
                "columns": DATASET_COLUMN_HINTS["orders"],
            },
            base_url="https://example.invalid",
            api_key="test-key",
            endpoint="/internal/analytics/exports/orders/",
        )

        table_schema = resource.compute_table_schema()
        self.assertEqual(
            table_schema["columns"]["profit_eur"]["data_type"],
            "decimal",
        )
        self.assertEqual(
            table_schema["columns"]["item_count"]["data_type"],
            "bigint",
        )

    def test_attribution_columns_have_explicit_text_hints(self):
        expected = {
            "users": {"acquisition_route", "acquisition_session_id"},
            "sessions": {
                "brand_code",
                "visitor_id",
                "route",
                "raw_route",
                "referrer_host",
                "utm_medium",
                "utm_campaign",
                "utm_term",
            },
            "orders": {"conversion_route", "acquisition_route"},
        }
        for dataset, columns in expected.items():
            with self.subTest(dataset=dataset):
                for column in columns:
                    self.assertEqual(
                        DATASET_COLUMN_HINTS[dataset][column]["data_type"],
                        "text",
                    )

    def test_session_funnel_columns_have_explicit_bool_and_timestamp_hints(self):
        session_hints = DATASET_COLUMN_HINTS["sessions"]

        for column in (
            "plans_viewed",
            "plan_added",
            "checkout_viewed",
            "purchased",
        ):
            with self.subTest(column=column):
                self.assertEqual(session_hints[column]["data_type"], "bool")
                self.assertFalse(session_hints[column]["nullable"])

        for column in (
            "plans_viewed_at",
            "plan_added_at",
            "checkout_viewed_at",
            "purchased_at",
        ):
            with self.subTest(column=column):
                self.assertEqual(session_hints[column]["data_type"], "timestamp")
                self.assertTrue(session_hints[column]["nullable"])

        resource = make_incremental_resource(
            dataset_name="sessions",
            config={
                "watermark_field": "updated_at",
                "primary_key": "id",
                "write_disposition": "merge",
                "default_limit": 500,
                "columns": session_hints,
            },
            base_url="https://example.invalid",
            api_key="test-key",
            endpoint="/internal/analytics/exports/sessions/",
        )
        table_schema = resource.compute_table_schema()
        self.assertEqual(
            table_schema["columns"]["purchased_at"]["data_type"],
            "timestamp",
        )
        self.assertEqual(
            table_schema["columns"]["purchased"]["data_type"],
            "bool",
        )


if __name__ == "__main__":
    unittest.main()
