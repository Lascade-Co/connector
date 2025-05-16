import os
from pathlib import Path
from typing import Dict, Any
import tomllib as toml

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


def get(dictionary: Dict[str, Any], *keys: str | int, default: Any = None) -> Any:
    """
    Get a value from a nested dictionary using a list of keys.
    If the key is not found, return the default value.
    """
    for key in keys:
        try:
            dictionary = dictionary[key]
        except KeyError | TypeError | IndexError:
            return default
    return dictionary
