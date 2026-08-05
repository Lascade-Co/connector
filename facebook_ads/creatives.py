"""Incremental and full-reconciliation loaders for Meta ad creatives."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import os
from typing import Any, Iterator, Sequence

import dlt
from dlt.common.typing import DictStrAny, TDataItems
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.api import FacebookResponse
from facebook_business.exceptions import FacebookRequestError

from .helpers import get_data_chunked


_LIGHT_CREATIVE_FIELDS = (
    "id",
    "name",
    "status",
    "effective_object_story_id",
)
_VALID_REFRESH_MODES = frozenset(("auto", "incremental", "full"))


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning("Invalid %s=%r; defaulting to %d", name, raw, default)
        return default


def get_creative_refresh_mode() -> str:
    raw = (os.getenv("FB_CREATIVE_REFRESH_MODE") or "auto").strip().lower()
    if raw not in _VALID_REFRESH_MODES:
        raise ValueError(
            "FB_CREATIVE_REFRESH_MODE must be one of auto, incremental, or full; "
            f"received {raw!r}"
        )
    return raw


def _creative_fingerprint(item: DictStrAny) -> str:
    payload = {field: item.get(field) for field in _LIGHT_CREATIVE_FIELDS}
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _hydrate_creative_batch(
    account: Any,
    light_items: Sequence[DictStrAny],
    fields: Sequence[str],
) -> list[DictStrAny]:
    """Hydrate one Meta batch and fail the whole batch if any item fails."""

    api_batch = account.get_api().new_batch()
    hydrated: dict[str, DictStrAny] = {
        str(item["id"]): {"id": str(item["id"])} for item in light_items
    }
    failures: list[BaseException] = []
    completed: set[str] = set()

    def update_item(resp: FacebookResponse, creative_id: str) -> None:
        hydrated[creative_id].update(resp.json())
        completed.add(creative_id)

    def fail_item(resp: FacebookResponse, _creative_id: str) -> None:
        failures.append(resp.error())

    for item in light_items:
        creative_id = str(item["id"])
        AdCreative(fbid=creative_id, api=account.get_api()).api_get(
            fields=fields,
            batch=api_batch,
            success=functools.partial(update_item, creative_id=creative_id),
            failure=functools.partial(fail_item, _creative_id=creative_id),
        )
    retry_batch = api_batch.execute()
    if failures:
        raise failures[0]
    if retry_batch is not None or len(completed) != len(light_items):
        raise RuntimeError(
            "Meta returned an incomplete creative batch response; "
            "leaving its fingerprints unchanged for the next run"
        )
    return [hydrated[str(item["id"])] for item in light_items]


def _iter_full_creatives(
    account: Any,
    fields: Sequence[str],
    states: Sequence[str] | None,
) -> Iterator[TDataItems]:
    """Run the weekly/manual full edge scan with a conservative page size."""

    # Development access allows very few read-score points. Begin with a larger
    # page and retain the adaptive response-size fallback for oversized payloads.
    current_size = max(1, _get_int_env("FB_ADCREATIVES_CHUNK_SIZE", 250))
    seen_ids: set[str] = set()
    while True:
        try:
            for chunk in get_data_chunked(
                account.get_ad_creatives,
                fields,
                states,
                current_size,
                {"summary": "false"},
            ):
                fresh = [
                    item for item in chunk if str(item.get("id")) not in seen_ids
                ]
                seen_ids.update(str(item["id"]) for item in fresh if item.get("id"))
                if fresh:
                    yield fresh
            return
        except FacebookRequestError as exc:
            message = (exc.api_error_message() or "").lower()
            is_reduce_data = (
                exc.api_error_code() == 1
                and "reduce the amount of data" in message
            )
            if not is_reduce_data or current_size <= 1:
                raise
            new_size = max(1, current_size // 2)
            logging.warning(
                "ad_creatives full reconciliation: Meta requested less data; "
                "shrinking page size %d -> %d (already yielded: %d)",
                current_size,
                new_size,
                len(seen_ids),
            )
            current_size = new_size


def iter_creatives(
    account: Any,
    account_id: str,
    fields: Sequence[str],
    states: Sequence[str] | None,
) -> Iterator[TDataItems]:
    """Hydrate new/light-field-changed creatives; full mode repairs heavy fields."""

    resource_state = dlt.current.resource_state()
    account_states = resource_state.setdefault("accounts", {})
    account_state = account_states.setdefault(str(account_id), {})
    fingerprints: dict[str, str] = account_state.setdefault("fingerprints", {})
    full_reconciliation_incomplete = bool(
        account_state.get("full_reconciliation_incomplete")
    )

    requested_mode = get_creative_refresh_mode()
    effective_mode = (
        "full"
        if requested_mode == "full"
        or not fingerprints
        or full_reconciliation_incomplete
        else "incremental"
    )
    if requested_mode == "incremental" and not fingerprints:
        logging.warning(
            "ad_creatives account=%s has no fingerprint baseline; running the "
            "required initial full reconciliation",
            account_id,
        )
    if full_reconciliation_incomplete:
        logging.warning(
            "ad_creatives account=%s has an incomplete full reconciliation; "
            "retrying full mode before returning to incremental refreshes",
            account_id,
        )
    logging.info(
        "ad_creatives account=%s refresh_mode=%s known_creatives=%d",
        account_id,
        effective_mode,
        len(fingerprints),
    )

    if effective_mode == "full":
        # Set this before extraction so a rate-limited scan leaves a durable
        # repair marker in the resource state committed by the outer guard.
        account_state["full_reconciliation_incomplete"] = True
        for chunk in _iter_full_creatives(account, fields, states):
            for item in chunk:
                creative_id = item.get("id")
                if creative_id is not None:
                    fingerprints[str(creative_id)] = _creative_fingerprint(item)
            yield chunk
        account_state["full_reconciliation_incomplete"] = False
        return

    id_page_size = max(1, _get_int_env("FB_ADCREATIVE_ID_PAGE_SIZE", 500))
    hydrate_batch_size = max(
        1, min(50, _get_int_env("FB_ADCREATIVE_HYDRATE_BATCH_SIZE", 50))
    )
    changed_count = 0
    scanned_count = 0

    for light_chunk in get_data_chunked(
        account.get_ad_creatives,
        _LIGHT_CREATIVE_FIELDS,
        states,
        id_page_size,
        {"summary": "false"},
    ):
        scanned_count += len(light_chunk)
        changed = [
            item
            for item in light_chunk
            if fingerprints.get(str(item.get("id"))) != _creative_fingerprint(item)
        ]
        for offset in range(0, len(changed), hydrate_batch_size):
            light_batch = changed[offset : offset + hydrate_batch_size]
            hydrated = _hydrate_creative_batch(account, light_batch, fields)
            for light_item in light_batch:
                fingerprints[str(light_item["id"])] = _creative_fingerprint(light_item)
            changed_count += len(hydrated)
            yield hydrated

    logging.info(
        "ad_creatives account=%s scanned=%d hydrated=%d",
        account_id,
        scanned_count,
        changed_count,
    )
