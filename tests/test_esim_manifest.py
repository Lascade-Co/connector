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
        for version in ("1.1", "1.2", "1.3"):
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

    def test_rejects_unreviewed_orders_schema(self):
        with self.assertRaisesRegex(ValueError, "unsupported schema_version"):
            parse_manifest([_orders_dataset("2.0")])


if __name__ == "__main__":
    unittest.main()
