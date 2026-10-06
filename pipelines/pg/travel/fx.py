"""Restate inline ad CPC prices in USD using currencyapi.com.

``cpc_price``/``cpc_currency`` are loaded untouched; this module only adds
``cpc_price_usd``. The rate used is ``cpc_price_usd / cpc_price``, and it always
belongs to the UTC day of ``created_at``.

The travel sync runs every 45 minutes but currencyapi's free plan allows about
300 requests a month, so rates are cached in dlt resource state. That state is
stored in ClickHouse with the load, so fresh CI runners share it:

* a finished day's ``historical`` snapshot is fetched once and kept,
* today's ``latest`` snapshot is reused for ``LATEST_TTL_HOURS``.

An API failure raises: the run commits nothing and the ``created_at``
watermark stays put, so the next run retries. Writing NULL instead would be
permanent, because rows are never re-read. A currency missing from a valid
snapshot only NULLs that row's ``cpc_price_usd``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, MutableMapping

from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Client


API_URL = "https://api.currencyapi.com/v3"
BASE_CURRENCY = "USD"

LATEST_TTL_HOURS = 6
STATE_RETENTION_DAYS = 14

AMOUNT_QUANTUM = Decimal("0.000001")

# Status codes are mapped to errors below, so the client must not raise first.
# 429 is not retried: an exhausted monthly quota will not recover by waiting.
_http = Client(raise_for_status=False, status_codes=(500, 502, 503, 504))

USD_COLUMNS = {
    "cpc_price_usd": {"data_type": "double", "nullable": True},
}


class RateUnavailable(RuntimeError):
    """No trustworthy rate could be obtained, so the load must not commit."""


class SnapshotNotPublished(RateUnavailable):
    """currencyapi has no historical snapshot for that day (yet)."""


class CurrencyApiClient:
    def __init__(self, api_key: str, *, timeout: float = 30.0) -> None:
        if not api_key:
            raise RateUnavailable(
                "Missing currencyapi api_key; set sources.currencyapi.api_key in secrets"
            )
        self._api_key = api_key
        self._timeout = timeout

    def historical(self, day: date) -> dict[str, Decimal]:
        return self._get("historical", {"date": day.isoformat()})

    def latest(self) -> dict[str, Decimal]:
        return self._get("latest", {})

    def _get(self, endpoint: str, params: dict[str, str]) -> dict[str, Decimal]:
        logging.info("currencyapi request: %s %s", endpoint, params or "")
        try:
            response = _http.get(
                f"{API_URL}/{endpoint}",
                params={**params, "base_currency": BASE_CURRENCY},
                headers={"apikey": self._api_key},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise RateUnavailable(f"currencyapi {endpoint} request failed: {exc}") from exc

        if response.status_code in (401, 403):
            raise RateUnavailable("currencyapi rejected the api key")
        if response.status_code == 429:
            raise RateUnavailable("currencyapi quota exhausted (HTTP 429)")
        if endpoint == "historical" and response.status_code in (404, 422):
            raise SnapshotNotPublished(
                f"currencyapi has no historical rates for {params.get('date')}: {response.text[:200]}"
            )
        if response.status_code >= 400:
            raise RateUnavailable(
                f"currencyapi {endpoint} returned HTTP {response.status_code}: {response.text[:200]}"
            )

        try:
            body = response.json()
        except ValueError:
            raise RateUnavailable(f"currencyapi {endpoint} returned non-JSON") from None
        return parse_rates(body)


def parse_rates(body: Any) -> dict[str, Decimal]:
    """Reduce a currencyapi v3 body to ``{CODE: units per USD}``, strictly."""

    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or not data:
        raise RateUnavailable("currencyapi response has no rate data")

    rates: dict[str, Decimal] = {}
    for code, entry in data.items():
        raw = entry.get("value") if isinstance(entry, dict) else None
        try:
            value = Decimal(str(raw))
        except InvalidOperation:
            continue
        if value.is_finite() and value > 0:
            rates[str(code).upper()] = value

    if BASE_CURRENCY not in rates:
        raise RateUnavailable("currencyapi response is missing the USD base rate")
    return rates


def _encode(rates: dict[str, Decimal]) -> dict[str, str]:
    # Strings keep state JSON-safe and exact.
    return {code: str(value) for code, value in rates.items()}


class UsdRateProvider:
    """Resolve units-per-USD rates, caching snapshots in ``state`` across runs."""

    def __init__(
        self,
        client_factory: Callable[[], CurrencyApiClient],
        state: MutableMapping[str, Any],
        *,
        now: datetime | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._client: CurrencyApiClient | None = None
        self._state = state
        self._now = now or datetime.now(timezone.utc)
        self._today = self._now.date()
        self._warned: set[str] = set()
        self._state.setdefault("historical", {})
        self._prune()

    @property
    def client(self) -> CurrencyApiClient:
        # Created lazily so a USD-only run needs neither a key nor the network.
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    def units_per_usd(self, currency: str, on: date) -> Decimal | None:
        code = currency.strip().upper()
        if code == BASE_CURRENCY:
            return Decimal(1)

        rates = self._latest_rates() if on >= self._today else self._historical_rates(on)

        raw = rates.get(code)
        if raw is None:
            if code not in self._warned:
                self._warned.add(code)
                logging.warning(
                    "currencyapi has no rate for %r; cpc_price_usd left NULL", code
                )
            return None
        return Decimal(raw)

    def _historical_rates(self, on: date) -> dict[str, str]:
        key = on.isoformat()
        cached = self._state["historical"].get(key)
        if cached is not None:
            return cached

        try:
            rates = _encode(self.client.historical(on))
        except SnapshotNotPublished:
            # Just after midnight yesterday's close may not be out yet; the
            # intraday snapshot taken that day is the best available rate.
            latest = self._state.get("latest")
            if latest and latest.get("date") == key:
                logging.warning(
                    "Historical rates for %s not published yet; using that day's latest snapshot",
                    key,
                )
                return latest["rates"]
            raise

        self._state["historical"][key] = rates
        return rates

    def _latest_rates(self) -> dict[str, str]:
        latest = self._state.get("latest")
        if latest and latest.get("date") == self._today.isoformat():
            fetched_at = datetime.fromisoformat(latest["fetched_at"])
            if self._now - fetched_at < timedelta(hours=LATEST_TTL_HOURS):
                return latest["rates"]

        rates = _encode(self.client.latest())
        self._state["latest"] = {
            "date": self._today.isoformat(),
            "fetched_at": self._now.isoformat(),
            "rates": rates,
        }
        return rates

    def _prune(self) -> None:
        cutoff = (self._today - timedelta(days=STATE_RETENTION_DAYS)).isoformat()
        historical = self._state["historical"]
        for key in [k for k in historical if k < cutoff]:
            del historical[key]


def _row_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None


def add_usd(item: dict[str, Any], provider: UsdRateProvider) -> dict[str, Any]:
    item["cpc_price_usd"] = None

    price, currency = item.get("cpc_price"), item.get("cpc_currency")
    if price is None or price == "" or not isinstance(currency, str) or not currency.strip():
        return item

    try:
        amount = Decimal(str(price))
    except InvalidOperation:
        amount = None
    if amount is None or not amount.is_finite():
        logging.warning("Non-numeric cpc_price %r on %s; cpc_price_usd left NULL", price, item.get("id"))
        return item

    on = _row_date(item.get("created_at"))
    if on is None:
        return item

    units_per_usd = provider.units_per_usd(currency, on)
    if units_per_usd is not None:
        item["cpc_price_usd"] = float(
            (amount / units_per_usd).quantize(AMOUNT_QUANTUM, rounding=ROUND_HALF_UP)
        )
    return item
