MANIFEST_ENDPOINT = "/internal/analytics/exports/manifest/"

STRATEGY_TO_DISPOSITION = {
    "incremental": "merge",
    "append-only": "append",
    "full-refresh": "replace",
    "full-refresh-snapshot": "replace",
}

DEFAULT_LIMIT = 500
MAX_PAGES = 10000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

DEFAULT_LIMIT_MIN = 1
DEFAULT_LIMIT_MAX = 2000
