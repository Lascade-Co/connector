"""Convert Facebook monetary values into AED so all accounts report alike.

The source currency comes from Meta's own ``account_currency`` Insights field,
so nothing has to be configured per account. Conversion happens *before*
``flatten_facebook_insights`` runs, which means every scalar that flattening
derives from a nested array - ``action_values_*``, ``cpa_*``,
``trial_start_value``, ``cost_per_result`` - comes out in AED without being
enumerated here.

The resolved rate is written onto every row. That makes each stored amount
explainable from its own row, so ``fx_daily_rates`` is an audit record rather
than something the Insights load depends on.

Two type families exist in the destination and each is written back as it
already is (see CLAUDE.md): ``spend``/``cpc``/``cpm``/``cpp`` and every nested
``value`` are text, while the flattened scalars are Float64 and are produced by
the existing flattening code from the text this module writes.
"""

from __future__ import annotations

import functools
import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Iterable

import pendulum
from dlt.common.typing import DictStrAny

from facebook_ads.helpers import get_ads_account
from pipelines.facebook.ecb import USD_PER_AED_PEG, Observation, fetch_observations


TARGET_CURRENCY = "AED"
METHOD_VERSION = "ecb-cross-usd-peg-1"

# Six places is well below one fils and keeps sums stable without turning
# amounts into twenty-character strings.
AMOUNT_QUANTUM = Decimal("0.000001")

# Long enough to carry across a multi-day ECB closure; staleness is enforced
# separately, so a wide lookback cannot silently widen the accepted age.
CARRY_LOOKBACK_DAYS = 10

# Extra history pulled with each fetch. Meta returns a window's rows in no
# particular date order, so without slack the first row of a window could set
# the lower bound and every earlier row would trigger another fetch. The ECB
# range costs one request either way.
FETCH_MARGIN_DAYS = 60

STALE_WARN_DAYS = 4
STALE_FAIL_DAYS = 7

IDENTITY_STATUS = "identity"

MONETARY_SCALARS = ("spend", "cpc", "cpm", "cpp")

# Nested arrays whose entries carry money. ``conversions``/``actions`` are
# counts and ``purchase_roas``/``website_ctr`` are ratios, so all are excluded.
MONETARY_NESTED_FIELDS = ("action_values", "conversion_values", "cost_per_action_type")

# Budgets and bids arrive in minor units. AED and GBP are both two-decimal
# currencies, so the same rate applies with no unit adjustment.
BUDGET_MINOR_UNIT_FIELDS = ("daily_budget", "lifetime_budget", "bid_amount")

# ``value`` plus Meta's per-attribution-window breakdowns of it. Matching the
# shape rather than listing six names means a new window Meta adds is covered.
_ATTRIBUTION_KEY = re.compile(r"^_?\d+d_(click|view)$")


class RateUnavailable(RuntimeError):
    """No trustworthy rate exists, so the load must fail rather than guess."""


@dataclass(frozen=True)
class Rate:
    source_currency: str
    requested_date: date
    effective_date: date
    aed_per_source: Decimal
    status: str
    observation: Observation | None = None

    @property
    def is_identity(self) -> bool:
        return self.status == IDENTITY_STATUS

    @property
    def is_provisional(self) -> bool:
        return self.status == "provisional"


def _is_monetary_entry_key(key: str) -> bool:
    return key == "value" or bool(_ATTRIBUTION_KEY.match(key))


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        raise RateUnavailable(
            f"Cannot establish a conversion date from date_start={value!r}"
        ) from None


def _scale(raw: Any, rate: Decimal) -> Decimal:
    try:
        value = Decimal(str(raw))
    except InvalidOperation:
        raise RateUnavailable(
            f"Refusing to convert a non-numeric monetary value: {raw!r}"
        ) from None
    return (value * rate).quantize(AMOUNT_QUANTUM, rounding=ROUND_HALF_UP)


def _convert_text(raw: Any, rate: Decimal) -> Any:
    """Convert a value that lives in a text column, leaving blanks alone."""

    if raw is None or raw == "":
        return raw
    return str(_scale(raw, rate))


@functools.lru_cache(maxsize=64)
def _fetch_account_currency(account_id: str, access_token: str) -> str:
    """Read an account's billing currency. Only used where Insights cannot say."""

    account = get_ads_account(account_id, access_token, 300, None)
    currency = str(account.api_get(fields=["currency"]).get("currency") or "").upper()
    if not currency:
        raise RateUnavailable(
            f"Meta returned no currency for account {account_id}"
        )
    logging.info("Fetched currency %s for Facebook account %s", currency, account_id)
    return currency


class AedRateProvider:
    """Resolve AED rates, hitting the ECB at most once for a run's date range.

    An AED-only run never touches the network: the identity rate short-circuits
    before any fetch, which is what makes "no ECB request occurred" a meaningful
    check during rollout.
    """

    def __init__(
        self,
        *,
        fetch: Callable[[date, date], dict[date, Observation]] = fetch_observations,
        today: date | None = None,
    ) -> None:
        self._fetch = fetch
        self.today = today or pendulum.today().date()
        self._observations: dict[date, Observation] = {}
        self._covered: tuple[date, date] | None = None
        self._resolved: dict[tuple[str, date], Rate] = {}
        self._account_currencies: dict[str, str] = {}
        self.retrieved_at: str | None = None
        self.fetch_count = 0

    # -- account currency ---------------------------------------------------

    def note_account_currency(self, account_id: Any, currency: str) -> None:
        self._account_currencies[str(account_id)] = currency.upper()

    def account_currency(self, account_id: Any, access_token: str) -> str:
        """Prefer what Insights already reported; ask Meta only if it didn't."""

        known = self._account_currencies.get(str(account_id))
        if known:
            return known
        currency = _fetch_account_currency(str(account_id), access_token)
        self._account_currencies[str(account_id)] = currency
        return currency

    # -- rates --------------------------------------------------------------

    def rate(self, source_currency: str, on: date) -> Rate:
        source = (source_currency or "").strip().upper()
        if not source:
            raise RateUnavailable("No source currency was supplied")
        cached = self._resolved.get((source, on))
        if cached is not None:
            return cached

        if source == TARGET_CURRENCY:
            resolved = Rate(source, on, on, Decimal(1), IDENTITY_STATUS)
        elif source == "GBP":
            resolved = self._gbp_rate(on)
        else:
            raise RateUnavailable(
                f"No {TARGET_CURRENCY} conversion is defined for {source!r}; "
                "add a rate source before loading this account"
            )
        self._resolved[(source, on)] = resolved
        return resolved

    def resolved_rates(self) -> list[Rate]:
        return list(self._resolved.values())

    def _gbp_rate(self, on: date) -> Rate:
        self._ensure_covered(on)
        published = [day for day in self._observations if day <= on]
        if not published:
            raise RateUnavailable(
                f"The ECB published no usable observation on or before {on}"
            )

        effective = max(published)
        age_days = (on - effective).days
        if age_days > STALE_FAIL_DAYS:
            raise RateUnavailable(
                f"The newest ECB observation for {on} is {effective}, {age_days} days "
                "stale; refusing to convert GBP amounts"
            )
        if age_days > STALE_WARN_DAYS:
            logging.warning(
                "ECB rate for %s falls back to %s, which is %d days old",
                on,
                effective,
                age_days,
            )

        # A later observation proves the ECB has moved past this date, so the
        # carried rate is final. Without one it may still be restated.
        settled = any(day > on for day in self._observations)
        if effective == on:
            status = "exact"
        elif settled:
            status = "carried"
        else:
            status = "provisional"

        observation = self._observations[effective]
        return Rate(
            source_currency="GBP",
            requested_date=on,
            effective_date=effective,
            aed_per_source=observation.aed_per_gbp,
            status=status,
            observation=observation,
        )

    def _ensure_covered(self, on: date) -> None:
        margin = timedelta(days=CARRY_LOOKBACK_DAYS + FETCH_MARGIN_DAYS)
        want_from = on - margin
        want_to = max(on, self.today)
        if self._covered is not None:
            have_from, have_to = self._covered
            if (
                on - timedelta(days=CARRY_LOOKBACK_DAYS) >= have_from
                and want_to <= have_to
            ):
                return
            want_from = min(want_from, have_from - margin)
            want_to = max(want_to, have_to)

        self._observations = self._fetch(want_from, want_to)
        self._covered = (want_from, want_to)
        self.retrieved_at = pendulum.now("UTC").to_iso8601_string()
        self.fetch_count += 1
        logging.info(
            "Fetched %d ECB observations covering %s..%s",
            len(self._observations),
            want_from,
            want_to,
        )


# ---------------------------------------------------------------------------
# INSIGHTS
# ---------------------------------------------------------------------------


def _stamp(item: DictStrAny, rate: Rate) -> None:
    item["source_currency"] = rate.source_currency
    item["reporting_currency"] = TARGET_CURRENCY
    item["fx_rate_to_aed"] = str(rate.aed_per_source)
    item["fx_requested_date"] = rate.requested_date.isoformat()
    item["fx_effective_date"] = rate.effective_date.isoformat()
    item["fx_rate_status"] = rate.status


def _convert_nested_entries(entries: Any, rate: Decimal) -> None:
    if not isinstance(entries, list):
        return
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        for key in [k for k in entry if _is_monetary_entry_key(k)]:
            if key == "value":
                entry["value_source"] = entry[key]
            entry[key] = _convert_text(entry[key], rate)


def _convert_cost_per_result(series: Any, rate: Decimal) -> None:
    """``[{"indicator": ..., "values": [{"value": "0.97", ...}]}]``"""

    if not isinstance(series, list):
        return
    for indicator in series:
        if not isinstance(indicator, dict):
            continue
        values = indicator.get("values")
        if not isinstance(values, list):
            continue
        for entry in values:
            if isinstance(entry, dict) and "value" in entry:
                entry["value"] = _convert_text(entry["value"], rate)


def convert_insights_row(item: DictStrAny, provider: AedRateProvider) -> DictStrAny:
    """Restate one Insights row in AED and record how it was restated."""

    source = str(item.get("account_currency") or "").strip().upper()
    if not source:
        raise RateUnavailable(
            "Meta returned no account_currency for an Insights row, so its "
            "reporting currency cannot be established"
        )
    provider.note_account_currency(item.get("account_id"), source)

    rate = provider.rate(source, _as_date(item.get("date_start")))
    item["spend_source"] = item.get("spend")

    # An AED account is already in the reporting currency. Skipping the
    # arithmetic keeps its stored strings byte-identical to today's.
    if not rate.is_identity:
        for field in MONETARY_SCALARS:
            if field in item:
                item[field] = _convert_text(item[field], rate.aed_per_source)
        for field in MONETARY_NESTED_FIELDS:
            _convert_nested_entries(item.get(field), rate.aed_per_source)
        _convert_cost_per_result(item.get("cost_per_result"), rate.aed_per_source)

    _stamp(item, rate)
    return item


def insights_currency_map(
    provider: AedRateProvider,
) -> Callable[[DictStrAny], DictStrAny]:
    """A pre-flatten map for ``facebook_insights_source``.

    Must be a closure, not a ``functools.partial``. dlt inspects the mapped
    function's signature and treats a second positional parameter as its
    ``meta`` argument, so a partial over ``convert_insights_row(item, provider)``
    is called as ``f(item, meta)`` and collides with the bound ``provider``.
    A one-argument closure leaves dlt nothing to misread.
    """

    def convert(item: DictStrAny) -> DictStrAny:
        return convert_insights_row(item, provider)

    return convert


# ---------------------------------------------------------------------------
# BUDGETS
# ---------------------------------------------------------------------------


def convert_budget_row(
    row: DictStrAny, provider: AedRateProvider, access_token: str
) -> DictStrAny:
    """Restate campaign/ad set budgets, which are current-state minor units."""

    currency = provider.account_currency(row.get("account_id"), access_token)
    rate = provider.rate(currency, provider.today)
    row["source_currency"] = rate.source_currency
    row["fx_rate_to_aed"] = str(rate.aed_per_source)
    row["fx_rate_status"] = rate.status
    if rate.is_identity:
        return row

    for field in BUDGET_MINOR_UNIT_FIELDS:
        raw = row.get(field)
        if raw is None or raw == "":
            continue
        row[f"{field}_source"] = raw
        minor_units = int(_scale(raw, rate.aed_per_source).to_integral_value(ROUND_HALF_UP))
        # bid_amount is an integer column; the budgets are integer-valued text.
        row[field] = minor_units if isinstance(raw, int) else str(minor_units)
    return row


def budget_currency_map(
    provider: AedRateProvider, access_token: str
) -> Callable[[DictStrAny], DictStrAny]:
    """A row transform for the budget-bearing current-state resources.

    A closure for the same reason as ``insights_currency_map``, so neither
    factory can be moved onto a dlt map and break.
    """

    def convert(row: DictStrAny) -> DictStrAny:
        return convert_budget_row(row, provider, access_token)

    return convert


# ---------------------------------------------------------------------------
# AUDIT TABLE
# ---------------------------------------------------------------------------


FX_RATE_COLUMNS = {
    "source_currency": {"data_type": "text"},
    "target_currency": {"data_type": "text"},
    "requested_date": {"data_type": "text"},
    "effective_date": {"data_type": "text"},
    "gbp_per_eur": {"data_type": "text"},
    "usd_per_eur": {"data_type": "text"},
    "usd_aed_reference": {"data_type": "text"},
    "aed_per_source": {"data_type": "text"},
    "status": {"data_type": "text"},
    "is_provisional": {"data_type": "bool"},
    "method_version": {"data_type": "text"},
    "retrieved_at": {"data_type": "text"},
}


def fx_rate_rows(
    rates: Iterable[Rate], retrieved_at: str | None
) -> list[DictStrAny]:
    """Audit rows for every non-identity rate a run actually used."""

    rows: list[DictStrAny] = []
    for rate in rates:
        if rate.is_identity:
            continue
        observation = rate.observation
        rows.append(
            {
                "source_currency": rate.source_currency,
                "target_currency": TARGET_CURRENCY,
                "requested_date": rate.requested_date.isoformat(),
                "effective_date": rate.effective_date.isoformat(),
                "gbp_per_eur": str(observation.gbp_per_eur) if observation else None,
                "usd_per_eur": str(observation.usd_per_eur) if observation else None,
                "usd_aed_reference": str(USD_PER_AED_PEG),
                "aed_per_source": str(rate.aed_per_source),
                "status": rate.status,
                "is_provisional": rate.is_provisional,
                "method_version": METHOD_VERSION,
                "retrieved_at": retrieved_at,
            }
        )
    return rows
