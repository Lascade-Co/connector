import unittest

from pipelines.esim.manifest import parse_manifest


def _orders_dataset(schema_version: str) -> dict:
    return {
        "name": "orders",
        "endpoint": "/internal/analytics/exports/orders/",
        "primary_key": "id",
        "watermark_field": "updated_at",
        "default_limit": 500,
        "strategy": "incremental",
        "schema_version": schema_version,
        "available": True,
    }


class OrdersManifestTests(unittest.TestCase):
    def test_accepts_rolling_versions_and_adds_only_new_column_hints(self):
        for version in ("1.1", "1.2", "1.3", "1.4"):
            parsed = parse_manifest([_orders_dataset(version)])[0]
            self.assertEqual(parsed["schema_version"], version)
            self.assertEqual(
                set(parsed["columns"]),
                {
                    "subtotal_eur",
                    "profit_eur",
                    "item_count",
                    "distinct_plan_count",
                    "is_cart",
                },
            )

    def test_rejects_unreviewed_major_version(self):
        """A MAJOR bump can change or drop existing columns, so it must stay gated."""
        with self.assertRaisesRegex(ValueError, "unsupported schema_version"):
            parse_manifest([_orders_dataset("2.0")])

    def test_rejects_malformed_version(self):
        for value in ("1", "1.2.3", "1.x", "", None):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "unsupported schema_version"):
                    parse_manifest([_orders_dataset(value)])

    def test_accepts_unreviewed_minor_of_reviewed_major_with_warning(self):
        """An additive MINOR bump must not take the whole run down.

        Regression test: the backend shipped orders 1.4 while this connector
        pinned an exact set ending at 1.3, and the resulting raise killed every
        dataset in the run, not just orders.
        """
        with self.assertLogs("pipelines.esim.manifest", level="WARNING") as logs:
            parsed = parse_manifest([_orders_dataset("1.99")])[0]

        self.assertEqual(parsed["schema_version"], "1.99")
        self.assertIn("SCHEMA DRIFT", "".join(logs.output))
        # The warning must name the file to edit, or it is not actionable in a CI log.
        self.assertIn("pipelines/esim/constants.py", "".join(logs.output))

    def test_reviewed_versions_log_no_drift_warning(self):
        for version in ("1.1", "1.2", "1.3", "1.4"):
            with self.subTest(version=version):
                with self.assertNoLogs("pipelines.esim.manifest", level="WARNING"):
                    parse_manifest([_orders_dataset(version)])

    def test_ungated_dataset_version_passes_through(self):
        """Only datasets listed in SUPPORTED_SCHEMA_VERSIONS are gated."""
        sessions = _orders_dataset("9.9")
        sessions["name"] = "sessions"
        self.assertEqual(parse_manifest([sessions])[0]["schema_version"], "9.9")


if __name__ == "__main__":
    unittest.main()
