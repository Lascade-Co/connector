"""Process-local tracking for accounts with incomplete creative extraction."""

_partial_creative_accounts: set[str] = set()


def reset_partial_creative_accounts() -> None:
    _partial_creative_accounts.clear()


def mark_partial_creative_account(account_id: str) -> None:
    _partial_creative_accounts.add(account_id)


def get_partial_creative_accounts() -> tuple[str, ...]:
    return tuple(sorted(_partial_creative_accounts))
