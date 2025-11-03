import json
import logging
import os
import sys
import datetime
import tomllib as toml  # noqa
from pathlib import Path
from typing import Any, Dict, List


def _load_secrets(path: Path | None = None) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Read `.dlt/secrets.toml` and return (pg_credentials, click_credentials).

    The expected layout is the standard one described in the dlt docs :

        [sources.pg_replication.credentials]
        drivername = "postgresql"
        host = "db.data.lascade.com"
        …

        [destination.clickhouse.credentials]
        host      = "click.lascade.com"
        http_port = 8443
        secure    = 1
        …
    """
    path = path or Path(os.getenv("DLT_SECRETS_FILE", ".dlt/secrets.toml"))
    if not path.exists():
        raise SystemExit(f"Cannot find secrets file at {path!s}")

    with path.open("rb") as fh:
        data: Dict[str, Any] = toml.load(fh)

    try:
        pg_cfg = data["sources"]["pg_replication"]["credentials"]
        ch_cfg = data["destination"]["clickhouse"]["credentials"]
    except KeyError as exc:
        raise SystemExit(f"Missing key in secrets.toml: {exc}") from exc

    # Normalise a few fields
    pg_cfg.setdefault("port", 5432)
    ch_cfg.setdefault("http_port", 8443)
    ch_cfg["secure"] = bool(ch_cfg.get("secure", 0))

    return pg_cfg, ch_cfg


def get_for_group(group: str, platform: str) -> tuple[Dict[str, str], List[str | Dict[str, str]]]:
    """
    Get configuration and account IDs for a specific group and platform.
    
    Args:
        group: Group name (e.g., 'd1', 'm4', 'd2')
        platform: Platform name ('facebook', 'google', 'google_play')
    
    Returns:
        For facebook/google: (group_config, list of account ID strings)
        For google_play: (group_config, list of app config dicts)
    """
    # check if secrets folder exists
    if Path("secrets").exists():
        secrets_path = Path("secrets") / f"{platform}.json"
    else:
        secrets_path = Path(f"{platform}.json")

    if not secrets_path.exists():
        raise SystemExit(f"Cannot find secrets file at {secrets_path!s}")

    with secrets_path.open("r") as fh:
        data = json.load(fh)

    if group not in data:
        raise SystemExit(f"Group '{group}' not found in {platform} secrets file.")

    accounts_ids = data[group]["account_ids"]

    # For google_play, account_ids is a list of dicts with package_name and app_name
    # For app_store, allow account_ids to be a list of dicts with id/app_name to keep names
    # For facebook/google, it's a list of account ID strings
    if platform == "google_play" or platform == "app_store":
        return data[group], accounts_ids
    else:
        return data[group], [str(aid) for aid in accounts_ids]


def get(dictionary: Dict[str, Any], *keys: str | int, default: Any = None) -> Any:
    """
    Get a value from a nested dictionary using a list of keys.
    If the key is not found, return the default value.
    """
    for key in keys:
        try:
            dictionary = dictionary[key]
        except (KeyError, TypeError, IndexError):
            return default
    return dictionary


def setup_logging():
    """
    Set up logging for the pipeline.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s │ %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def date_key_from_ga4(dim_date: Any | None) -> str | None:
    """
    Normalize GA4 date dimension to ISO date string YYYY-MM-DD.
    GA4 "date" is typically 'YYYYMMDD'. If already 'YYYY-MM-DD', return as-is.
    Returns None if input is None or malformed.
    """
    if dim_date is None:
        return None
    # Handle datetime/date inputs directly
    if isinstance(dim_date, datetime.datetime):
        return dim_date.date().isoformat()
    if isinstance(dim_date, datetime.date):
        return dim_date.isoformat()
    # Fallback to string handling
    s = str(dim_date).strip()
    if not s:
        return None
    if '-' in s:
        # assume already ISO date
        return s
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    # Fallback: return original string; downstream may coerce/validate
    return s


essentially_iso_lengths = {10}


def date_key_from_play(date_str: Any | None) -> str | None:
    """
    Normalize Google Play CSV Date to ISO date string YYYY-MM-DD.
    Input is usually already 'YYYY-MM-DD'; return None if missing/empty.
    """
    if date_str is None:
        return None
    if isinstance(date_str, datetime.datetime):
        return date_str.date().isoformat()
    if isinstance(date_str, datetime.date):
        return date_str.isoformat()
    s = str(date_str).strip()
    if not s:
        return None
    return s