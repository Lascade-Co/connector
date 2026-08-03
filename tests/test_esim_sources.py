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


if __name__ == "__main__":
    unittest.main()
