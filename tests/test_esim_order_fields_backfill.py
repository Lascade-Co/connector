import unittest

from pipelines.esim.order_fields_backfill_pipeline import (
    select_backfill_fields,
    validate_staged_backfill,
)


class _FakeSqlClient:
    def __init__(self, results):
        self.results = iter(results)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def make_qualified_table_name(self, name):
        return name

    def execute_sql(self, _query):
        return next(self.results)


class _FakePipeline:
    def __init__(self, results):
        self.client = _FakeSqlClient(results)

    def sql_client(self):
        return self.client


class OrderFieldsBackfillTests(unittest.TestCase):
    def test_selects_only_additive_fields(self):
        item = {
            "id": "order-1",
            "subtotal_eur": 20.0,
            "profit_eur": 8.0,
            "item_count": 3,
            "distinct_plan_count": 2,
            "is_cart": True,
            "total_eur": 18.0,
            "cost_eur": 12.0,
        }
        self.assertEqual(
            select_backfill_fields(item),
            {
                "id": "order-1",
                "subtotal_eur": 20.0,
                "profit_eur": 8.0,
                "item_count": 3,
                "distinct_plan_count": 2,
                "is_cart": True,
            },
        )

    def test_rejects_pre_1_2_payload(self):
        with self.assertRaisesRegex(ValueError, "missing schema 1.2"):
            select_backfill_fields({"id": "order-1"})

    def test_staging_validation_accepts_exact_coverage(self):
        pipeline = _FakePipeline([
            [(3, 3, 0, 0, 0, 0, 0)],
            [(0,)],
            [(0,)],
        ])
        stats = validate_staged_backfill(pipeline, expected_count=3)
        self.assertEqual(stats["row_count"], 3)

    def test_staging_validation_rejects_missing_ids(self):
        pipeline = _FakePipeline([
            [(2, 2, 0, 0, 0, 0, 0)],
            [(1,)],
            [(0,)],
        ])
        with self.assertRaisesRegex(RuntimeError, "missing_from_staging"):
            validate_staged_backfill(pipeline, expected_count=2)


if __name__ == "__main__":
    unittest.main()
