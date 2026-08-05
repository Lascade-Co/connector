"""Process-local status for Facebook resources parked after Meta throttling."""

from __future__ import annotations

from collections import defaultdict


_partial_resources: dict[str, set[str]] = defaultdict(set)


def reset_partial_resources() -> None:
    _partial_resources.clear()


def mark_partial_resource(resource_name: str, account_id: str) -> None:
    _partial_resources[resource_name].add(str(account_id))


def get_partial_resources() -> dict[str, tuple[str, ...]]:
    return {
        resource_name: tuple(sorted(account_ids))
        for resource_name, account_ids in sorted(_partial_resources.items())
        if account_ids
    }


def account_has_partial_resources(account_id: str) -> bool:
    account_id = str(account_id)
    return any(account_id in account_ids for account_ids in _partial_resources.values())


def format_partial_resources() -> str:
    return "; ".join(
        f"{resource_name}: {', '.join(account_ids)}"
        for resource_name, account_ids in get_partial_resources().items()
    )


def reset_partial_creative_accounts() -> None:
    """Compatibility wrapper retained for callers from the creative-only guard."""

    _partial_resources.pop("ad_creatives", None)


def mark_partial_creative_account(account_id: str) -> None:
    mark_partial_resource("ad_creatives", account_id)


def get_partial_creative_accounts() -> tuple[str, ...]:
    return get_partial_resources().get("ad_creatives", ())
