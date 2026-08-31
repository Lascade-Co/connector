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
    row_transform: Callable[[dict], dict] | None = None,
) -> Iterator[dict]:
    for row in getattr(source_factory(cred), source_attribute):
        row["account_id"] = cred["account_id"]
        row["managing_system"] = group_name
        yield row if row_transform is None else row_transform(row)


def load_structural_resource(
    accounts: Iterable[dict[str, str]],
    group_name: str,
    *,
    source_factory: Callable[[dict[str, str]], Any],
    source_attribute: str,
    resource_name: str,
    row_transform: Callable[[dict], dict] | None = None,
):
    """Yield tagged rows and park only the account/resource that Meta throttles.

    ``row_transform`` runs after tagging, so it can read ``account_id``. It is
    used to restate monetary fields; leaving it unset preserves raw rows.
    """

    yield from stream_with_rate_limit_guard(
        accounts,
        group_name,
        functools.partial(
            _stream_tagged_rows,
            source_factory=source_factory,
            source_attribute=source_attribute,
            row_transform=row_transform,
        ),
        resource_name=resource_name,
        on_partial=functools.partial(mark_partial_resource, resource_name),
    )
