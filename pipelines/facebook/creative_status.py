"""Process-local status for Facebook resources parked mid-load.

The registry holds every parked resource/account pair whatever the reason;
`account_has_quota_parking` answers only for the quota subset of it.
"""

from __future__ import annotations

from collections import defaultdict


INSIGHTS_RESOURCE = "insights"

# Marks that are not quota signals. `account_has_quota_parking` must ignore
# every one of these, or it will again stop an account's remaining current-state
# resources for a fault that has nothing to do with quota.
NON_QUOTA_RESOURCES = frozenset({INSIGHTS_RESOURCE})

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


def account_has_quota_parking(account_id: str) -> bool:
    """Report whether Meta is throttling the account, ignoring other parking.

    A degraded Insights report is a report-generation fault, not a quota
    signal: the account's current-state resources load fine, so it must not
    stop the rest of them. Reads `.items()` rather than subscripting, because
    `_partial_resources` is a defaultdict and a subscript read inserts.
    """

    account_id = str(account_id)
    return any(
        account_id in account_ids
        for resource_name, account_ids in _partial_resources.items()
        if resource_name not in NON_QUOTA_RESOURCES
    )


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
