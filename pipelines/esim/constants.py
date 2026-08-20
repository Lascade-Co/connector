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

# Versions of each gated dataset that have actually been reviewed here. A dataset
# with no entry is ungated (its version is passed through unchecked).
#
# This is NOT an exact-match allowlist. `_resolve_schema_version` gates on the
# MAJOR only: an unreviewed MINOR of a reviewed MAJOR is accepted with a loud
# SCHEMA DRIFT warning, while an unreviewed MAJOR still hard-fails.
#
# Why: the backend bumps MINOR for purely additive columns (1.2, 1.3 and 1.4 all
# were), and dlt never retypes an existing column — only brand-new columns are at
# stake. Exact matching meant every additive backend field took the whole hourly
# eSIM run down until someone edited this set, which is what happened when the
# backend shipped orders 1.4. A MAJOR bump can change or drop existing columns, so
# that stays gated.
#
# The residual risk a MINOR bump carries is a *new* money column: the backend
# serializes Decimals as JSON floats (`analytics_export/services/currency.py`
# `to_numeric`), so an unhinted one infers as double, which is why 1.2's money
# fields are pinned in DATASET_COLUMN_HINTS above. Adding a version here after
# reviewing it silences the warning and records that the review happened.
#
#   1.2  subtotal_eur, profit_eur, item_count, distinct_plan_count, is_cart
#   1.3  payment_method, stripe_account_slug, stripe_account_acct_id
#   1.4  support_resolution ("" or "compensated"; always a non-null string, so
#        dlt infers text). Also makes wallet-compensated orders export
#        status="refunded", so refund reporting must read support_resolution to
#        tell a wallet credit from a real Stripe reversal.
SUPPORTED_SCHEMA_VERSIONS = {
    "orders": frozenset({"1.1", "1.2", "1.3", "1.4"}),
}

DEFAULT_LIMIT = 500
MAX_PAGES = 10000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

DEFAULT_LIMIT_MIN = 1
DEFAULT_LIMIT_MAX = 2000
