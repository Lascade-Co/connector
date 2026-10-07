"""Restate inline ad CPC prices in USD using currencyapi.com.

``cpc_price``/``cpc_currency`` are loaded untouched; this module only adds
``cpc_price_usd``. The rate used is ``cpc_price_usd / cpc_price``, and it always
belongs to the UTC day of ``created_at``.

The travel sync runs every 45 minutes but currencyapi's free plan allows about
300 requests a month, so every fetched snapshot is written straight to the
``inline_ad_fx_usd_rates`` ClickHouse table and read back by later runs:

* a finished day's ``historical`` snapshot is fetched once and kept,
* today's ``latest`` snapshot is reused for ``LATEST_TTL_HOURS``.

The write happens immediately, not with the dlt load, so a run that fails
after fetching still keeps the rates it paid for.

An API failure raises: the run commits nothing and the ``created_at``
watermark stays put, so the next run retries. Writing NULL instead would be
permanent, because rows are never re-read. A currency missing from a valid
snapshot only NULLs that row's ``cpc_price_usd``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Protocol

from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Client


API_URL = "https://api.currencyapi.com/v3"
BASE_CURRENCY = "USD"

LATEST_TTL_HOURS = 6

HISTORICAL = "historical"
LATEST = "latest"

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


RATE_TABLE = "inline_ad_fx_usd_rates"

# Wide enough for every fiat code; currencyapi also lists crypto with long
# fractions, which are rounded to fit rather than rejected.
RATE_SCALE = Decimal("1e-18")


@dataclass(frozen=True)
class Snapshot:
    kind: str  # "historical" (a finished day's close) or "latest" (intraday)
    rate_date: date
    fetched_at: datetime
    rates: dict[str, Decimal]


class RateStore(Protocol):
    def load(self, kind: str, rate_date: date) -> Snapshot | None: ...

    def save(self, snapshot: Snapshot) -> None: ...


class ClickHouseRateStore:
    """Fetched rates, saved the moment they arrive.

    This is deliberately separate from the dlt load: a rate fetched in a run
    whose load later fails is still correct, so it must not be fetched again.
    """

    def __init__(self, client: Any, table: str = RATE_TABLE) -> None:
        self._client = client
        self._table = table
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                kind LowCardinality(String),
                rate_date Date,
                currency LowCardinality(String),
                units_per_usd Decimal(38, 18),
                fetched_at DateTime64(3, 'UTC')
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            ORDER BY (kind, rate_date, currency)
            """
        )

    def load(self, kind: str, rate_date: date) -> Snapshot | None:
        """One stored snapshot, looked up by exact day so any age is found."""

        result = self._client.query(
            f"SELECT currency, units_per_usd, fetched_at FROM {self._table} FINAL "
            f"WHERE kind = {{kind:String}} AND rate_date = {{rate_date:Date}}",
            parameters={"kind": kind, "rate_date": rate_date},
        )
        if not result.result_rows:
            return None
        rates = {currency: Decimal(value) for currency, value, _ in result.result_rows}
        fetched_at = max(row[2] for row in result.result_rows)
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        return Snapshot(kind, rate_date, fetched_at, rates)

    def save(self, snapshot: Snapshot) -> None:
        self._client.insert(
            self._table,
            [
                [snapshot.kind, snapshot.rate_date, code, value.quantize(RATE_SCALE), snapshot.fetched_at]
                for code, value in snapshot.rates.items()
            ],
            column_names=["kind", "rate_date", "currency", "units_per_usd", "fetched_at"],
        )


class UsdRateProvider:
    """Resolve units-per-USD rates, reusing what ``store`` already holds."""

    def __init__(
        self,
        client_factory: Callable[[], CurrencyApiClient],
        store_factory: Callable[[], RateStore],
        *,
        now: datetime | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._store_factory = store_factory
        self._client: CurrencyApiClient | None = None
        self._store: RateStore | None = None
        self._now = now or datetime.now(timezone.utc)
        self._today = self._now.date()
        self._warned: set[str] = set()
        # Per-run memo of store lookups; None records a confirmed miss.
        self._snapshots: dict[tuple[str, date], Snapshot | None] = {}

    # Both are created lazily so a USD-only run needs no key, no network and
    # no rate table.
    @property
    def client(self) -> CurrencyApiClient:
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    @property
    def store(self) -> RateStore:
        if self._store is None:
            self._store = self._store_factory()
        return self._store

    def _stored(self, kind: str, rate_date: date) -> Snapshot | None:
        key = (kind, rate_date)
        if key not in self._snapshots:
            self._snapshots[key] = self.store.load(kind, rate_date)
        return self._snapshots[key]

    def units_per_usd(self, currency: str, on: date) -> Decimal | None:
        code = currency.strip().upper()
        if code == BASE_CURRENCY:
            return Decimal(1)

        rates = self._latest_rates() if on >= self._today else self._historical_rates(on)

        value = rates.get(code)
        if value is None and code not in self._warned:
            self._warned.add(code)
            logging.warning("currencyapi has no rate for %r; cpc_price_usd left NULL", code)
        return value

    def _fetched(self, kind: str, rate_date: date, rates: dict[str, Decimal]) -> Snapshot:
        snapshot = Snapshot(kind, rate_date, self._now, rates)
        self.store.save(snapshot)
        self._snapshots[(kind, rate_date)] = snapshot
        return snapshot

    def _historical_rates(self, on: date) -> dict[str, Decimal]:
        stored = self._stored(HISTORICAL, on)
        if stored is not None:
            return stored.rates

        try:
            rates = self.client.historical(on)
        except SnapshotNotPublished:
            # Just after midnight yesterday's close may not be out yet; the
            # intraday snapshot taken that day is the best available rate.
            # It is not saved as historical, so a later run fetches the close.
            latest = self._stored(LATEST, on)
            if latest is not None:
                logging.warning(
                    "Historical rates for %s not published yet; using that day's latest snapshot", on
                )
                return latest.rates
            raise

        return self._fetched(HISTORICAL, on, rates).rates

    def _latest_rates(self) -> dict[str, Decimal]:
        latest = self._stored(LATEST, self._today)
        if latest is not None and self._now - latest.fetched_at < timedelta(hours=LATEST_TTL_HOURS):
            return latest.rates
        return self._fetched(LATEST, self._today, self.client.latest()).rates


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
