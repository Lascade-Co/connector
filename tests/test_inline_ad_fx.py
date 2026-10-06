import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

from pipelines.pg.travel import fx
from pipelines.pg.travel.fx import (
    RateUnavailable,
    SnapshotNotPublished,
    UsdRateProvider,
    add_usd,
    parse_rates,
)


NOW = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)
YESTERDAY = datetime(2026, 10, 5, 9, 30, tzinfo=timezone.utc)

RATES = {"USD": Decimal("1"), "EUR": Decimal("0.8"), "INR": Decimal("80")}


class FakeClient:
    def __init__(self, rates=RATES, historical_error=None):
        self.rates = rates
        self.historical_error = historical_error
        self.calls = []

    def historical(self, day):
        self.calls.append(("historical", day))
        if self.historical_error:
            raise self.historical_error
        return dict(self.rates)

    def latest(self):
        self.calls.append(("latest", None))
        return dict(self.rates)


def provider(client, state=None, now=NOW):
    return UsdRateProvider(lambda: client, {} if state is None else state, now=now)


def ad(price, currency, created_at=YESTERDAY):
    return {"id": "1-1", "cpc_price": price, "cpc_currency": currency, "created_at": created_at}


class AddUsdTests(unittest.TestCase):
    def test_usd_is_identity_and_never_touches_the_client(self):
        def boom():
            raise AssertionError("client must not be created for USD")

        row = add_usd(ad(0.42, "usd"), UsdRateProvider(boom, {}, now=NOW))

        self.assertEqual(row["cpc_price_usd"], 0.42)
        self.assertEqual(row["cpc_price"], 0.42)
        self.assertEqual(row["cpc_currency"], "usd")

    def test_past_date_converts_with_one_historical_call(self):
        client = FakeClient()
        rates = provider(client)

        eur = add_usd(ad(2, "EUR"), rates)
        inr = add_usd(ad("40", "INR"), rates)

        self.assertEqual(eur["cpc_price_usd"], 2.5)
        self.assertEqual(inr["cpc_price_usd"], 0.5)
        self.assertEqual(client.calls, [("historical", date(2026, 10, 5))])

    def test_cached_state_is_reused_by_the_next_run(self):
        state = {}
        provider(FakeClient(), state).units_per_usd("EUR", date(2026, 10, 5))
        provider(FakeClient(), state).units_per_usd("EUR", NOW.date())

        client = FakeClient()
        rates = provider(client, state, now=NOW + timedelta(hours=1))
        rates.units_per_usd("EUR", date(2026, 10, 5))
        rates.units_per_usd("EUR", NOW.date())

        self.assertEqual(client.calls, [])

    def test_latest_is_refetched_after_ttl_and_on_a_new_day(self):
        state = {}
        provider(FakeClient(), state).units_per_usd("EUR", NOW.date())

        later = FakeClient()
        provider(later, state, now=NOW + timedelta(hours=fx.LATEST_TTL_HOURS)).units_per_usd("EUR", NOW.date())
        self.assertEqual(later.calls, [("latest", None)])

        tomorrow = NOW + timedelta(days=1, hours=-11)
        next_day = FakeClient()
        provider(next_day, state, now=tomorrow).units_per_usd("EUR", tomorrow.date())
        self.assertEqual(next_day.calls, [("latest", None)])

    def test_old_historical_snapshots_are_pruned(self):
        state = {"historical": {"2026-09-01": {"EUR": "0.9"}, "2026-10-05": {"EUR": "0.8"}}}

        provider(FakeClient(), state)

        self.assertEqual(list(state["historical"]), ["2026-10-05"])

    def test_unknown_currency_loads_with_null_usd(self):
        row = add_usd(ad(3, "XYZ"), provider(FakeClient()))

        self.assertIsNone(row["cpc_price_usd"])
        self.assertEqual(row["cpc_price"], 3)

    def test_missing_price_or_currency_skips_lookup(self):
        client = FakeClient()
        rates = provider(client)

        for row in (ad(None, "EUR"), ad("", "EUR"), ad(1, None), ad(1, "  ")):
            self.assertIsNone(add_usd(row, rates)["cpc_price_usd"])
        self.assertEqual(client.calls, [])

    def test_api_failure_fails_the_run(self):
        rates = provider(FakeClient(historical_error=RateUnavailable("quota exhausted")))

        with self.assertRaises(RateUnavailable):
            add_usd(ad(1, "EUR"), rates)

    def test_unpublished_yesterday_falls_back_to_that_days_latest(self):
        state = {}
        provider(FakeClient(), state, now=YESTERDAY).units_per_usd("EUR", YESTERDAY.date())

        rates = provider(FakeClient(historical_error=SnapshotNotPublished("not yet")), state)
        row = add_usd(ad(2, "EUR"), rates)

        self.assertEqual(row["cpc_price_usd"], 2.5)

    def test_unpublished_day_without_fallback_raises(self):
        rates = provider(FakeClient(historical_error=SnapshotNotPublished("not yet")))

        with self.assertRaises(SnapshotNotPublished):
            rates.units_per_usd("EUR", date(2026, 10, 5))


class ParseRatesTests(unittest.TestCase):
    def test_parses_v3_body(self):
        body = {
            "meta": {"last_updated_at": "2026-10-05T23:59:59Z"},
            "data": {"USD": {"code": "USD", "value": 1}, "eur": {"code": "EUR", "value": 0.85}},
        }

        self.assertEqual(parse_rates(body), {"USD": Decimal("1"), "EUR": Decimal("0.85")})

    def test_rejects_malformed_bodies(self):
        for body in (None, [], {}, {"data": {}}, {"data": {"EUR": {"value": 0.8}}}):
            with self.subTest(body=body), self.assertRaises(RateUnavailable):
                parse_rates(body)


class ClientTests(unittest.TestCase):
    def response(self, status, body=None):
        resp = mock.Mock(status_code=status, text="")
        resp.json.return_value = body
        return resp

    def test_status_codes_map_to_errors(self):
        client = fx.CurrencyApiClient("key")
        cases = [(401, RateUnavailable), (429, RateUnavailable), (422, SnapshotNotPublished), (500, RateUnavailable)]
        for status, error in cases:
            with self.subTest(status=status), mock.patch.object(fx._http, "get", return_value=self.response(status)):
                with self.assertRaises(error):
                    client.historical(date(2026, 10, 5))

    def test_sends_key_header_and_usd_base(self):
        body = {"data": {"USD": {"value": 1}, "EUR": {"value": 0.8}}}
        with mock.patch.object(fx._http, "get", return_value=self.response(200, body)) as get:
            fx.CurrencyApiClient("key").latest()

        _, kwargs = get.call_args
        self.assertEqual(kwargs["headers"], {"apikey": "key"})
        self.assertEqual(kwargs["params"]["base_currency"], "USD")

    def test_missing_key_fails_clearly(self):
        with self.assertRaisesRegex(RateUnavailable, "api_key"):
            fx.CurrencyApiClient(None)


if __name__ == "__main__":
    unittest.main()
