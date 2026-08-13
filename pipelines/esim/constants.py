MANIFEST_ENDPOINT = "/internal/analytics/exports/manifest/"

STRATEGY_TO_DISPOSITION = {
    "incremental": "merge",
    "append-only": "append",
    "full-refresh": "replace",
    "full-refresh-snapshot": "replace",
}

# Only the additive order fields are hinted here. Existing ClickHouse columns
# keep their current inferred types, avoiding an unintended type migration.
DATASET_COLUMN_HINTS = {
    "orders": {
        "subtotal_eur": {
            "data_type": "decimal",
            "precision": 10,
            "scale": 2,
            "nullable": True,
        },
        "profit_eur": {
            "data_type": "decimal",
            "precision": 10,
            "scale": 2,
            "nullable": True,
        },
        "item_count": {"data_type": "bigint", "nullable": True},
        "distinct_plan_count": {"data_type": "bigint", "nullable": True},
        "is_cart": {"data_type": "bool", "nullable": True},
    }
}

# Accept the old and additive contracts during the rolling deployment. A future
# version must be reviewed before the connector advances its persisted state.
#
# 1.3 adds the Stripe-account dimension (payment_method, stripe_account_slug,
# stripe_account_acct_id) — purely additive, and unhinted columns are inferred,
# so no DATASET_COLUMN_HINTS entry is required for them.
SUPPORTED_SCHEMA_VERSIONS = {
    "orders": frozenset({"1.1", "1.2", "1.3"}),
}

DEFAULT_LIMIT = 500
MAX_PAGES = 10000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

DEFAULT_LIMIT_MIN = 1
DEFAULT_LIMIT_MAX = 2000
