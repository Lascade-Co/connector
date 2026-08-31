"""ECB daily reference rates reduced to an AED-per-GBP series.

The ECB publishes no AED series. AED is pegged to USD at a fixed rate, so the
GBP rate is derived from two *same-day* EUR legs:

    aed_per_gbp = (usd_per_eur / gbp_per_eur) * USD_PER_AED_PEG

Both legs must come from the same observation date. The API returns the series
grouped by key (every GBP row, then every USD row) rather than interleaved by
date, so the legs are joined on ``TIME_PERIOD`` rather than by position.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from dlt.sources.helpers import requests


ECB_SERIES_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD+GBP.EUR.SP00.A"

# UAE dirham peg, fixed since 1997.
USD_PER_AED_PEG = Decimal("3.6725")

# The stored rate must reproduce the stored amounts exactly, so the rate is
# rounded once here and that rounded value is what multiplies every amount.
RATE_QUANTUM = Decimal("0.0000000001")

_EXPECTED_SERIES = {
    "EXR.D.GBP.EUR.SP00.A": "GBP",
    "EXR.D.USD.EUR.SP00.A": "USD",
}
_CSV_LEADING_COLUMNS = ["KEY", "FREQ", "CURRENCY"]


class EcbRateError(RuntimeError):
    """The ECB response cannot be trusted to produce a rate."""


@dataclass(frozen=True)
class Observation:
    """One day on which the ECB published both required legs."""

    on: date
    gbp_per_eur: Decimal
    usd_per_eur: Decimal
    aed_per_gbp: Decimal


def _positive_decimal(raw: str, label: str) -> Decimal:
    try:
        value = Decimal(raw)
    except InvalidOperation:
        raise EcbRateError(f"ECB {label} is not a number: {raw!r}") from None
    if value <= 0:
        raise EcbRateError(f"ECB {label} must be positive, got {value}")
    return value


def _observation_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise EcbRateError(f"ECB observation has no usable date: {raw!r}") from None


def parse_observations(body: str) -> dict[date, Observation]:
    """Parse the CSV series strictly, rejecting anything that is not it.

    A stray HTML error page served with HTTP 200 fails the leading-column
    check, as does JSON, an empty body, or a series that was never requested.
    """

    reader = csv.DictReader(io.StringIO(body))
    header = reader.fieldnames or []
    if header[:3] != _CSV_LEADING_COLUMNS:
        raise EcbRateError(
            "ECB response is not the expected CSV series; leading columns were "
            f"{header[:3] or None}"
        )

    legs: dict[date, dict[str, Decimal]] = {}
    for row in reader:
        key = (row.get("KEY") or "").strip()
        currency = _EXPECTED_SERIES.get(key)
        if currency is None:
            raise EcbRateError(f"ECB returned an unrequested series: {key!r}")

        raw_value = (row.get("OBS_VALUE") or "").strip()
        if not raw_value:
            # The ECB emits placeholder rows for non-publication days.
            continue

        on = _observation_date((row.get("TIME_PERIOD") or "").strip())
        day = legs.setdefault(on, {})
        if currency in day:
            raise EcbRateError(
                f"ECB returned duplicate {currency} observations for {on}"
            )
        day[currency] = _positive_decimal(raw_value, f"{currency} rate for {on}")

    observations: dict[date, Observation] = {}
    for on, day in legs.items():
        if len(day) != 2:
            # One leg without the other cannot produce a cross rate, but it is
            # not a malformed response - the next older day is still usable.
            logging.warning(
                "ECB published only %s for %s; that day cannot yield a rate",
                ",".join(sorted(day)),
                on,
            )
            continue
        gbp_per_eur = day["GBP"]
        usd_per_eur = day["USD"]
        observations[on] = Observation(
            on=on,
            gbp_per_eur=gbp_per_eur,
            usd_per_eur=usd_per_eur,
            aed_per_gbp=((usd_per_eur / gbp_per_eur) * USD_PER_AED_PEG).quantize(
                RATE_QUANTUM
            ),
        )

    if not observations:
        raise EcbRateError(
            "ECB returned no day carrying both a GBP and a USD observation"
        )
    return observations


def fetch_observations(
    start: date, end: date, *, timeout: float = 30.0
) -> dict[date, Observation]:
    """Fetch and parse one inclusive date range. Retries come from dlt's client."""

    response = requests.get(
        ECB_SERIES_URL,
        params={
            "startPeriod": start.isoformat(),
            "endPeriod": end.isoformat(),
            "format": "csvdata",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return parse_observations(response.text)
