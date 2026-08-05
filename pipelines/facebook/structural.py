"""Shared quota-safe loaders for Facebook current-state resources."""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterable, Iterator
from typing import Any

from pipelines.facebook.creative_status import mark_partial_resource
from pipelines.facebook.rate_limit import stream_with_rate_limit_guard


def _stream_tagged_rows(
    cred: dict[str, str],
    group_name: str,
    *,
    source_factory: Callable[[dict[str, str]], Any],
    source_attribute: str,
) -> Iterator[dict]:
    for row in getattr(source_factory(cred), source_attribute):
        row["account_id"] = cred["account_id"]
        row["managing_system"] = group_name
        yield row


def load_structural_resource(
    accounts: Iterable[dict[str, str]],
    group_name: str,
    *,
    source_factory: Callable[[dict[str, str]], Any],
    source_attribute: str,
    resource_name: str,
):
    """Yield tagged rows and park only the account/resource that Meta throttles."""

    yield from stream_with_rate_limit_guard(
        accounts,
        group_name,
        functools.partial(
            _stream_tagged_rows,
            source_factory=source_factory,
            source_attribute=source_attribute,
        ),
        resource_name=resource_name,
        on_partial=functools.partial(mark_partial_resource, resource_name),
    )
