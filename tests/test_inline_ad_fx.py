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


class FakeStore:
    """Stands in for the ClickHouse table; shared between "runs"."""

    def __init__(self, snapshots=()):
        self.snapshots = list(snapshots)
        self.loaded_since = None

    def load(self, since):
        self.loaded_since = since
        return [s for s in self.snapshots if s.rate_date >= since]

    def save(self, snapshot):
        self.snapshots.append(snapshot)


def provider(client, store=None, now=NOW):
    store = FakeStore() if store is None else store
    return UsdRateProvider(lambda: client, lambda: store, now=now)


def ad(price, currency, created_at=YESTERDAY):
    return {"id": "1-1", "cpc_price": price, "cpc_currency": currency, "created_at": created_at}


class AddUsdTests(unittest.TestCase):
    def test_usd_is_identity_and_touches_neither_client_nor_store(self):
        def boom():
            raise AssertionError("must not be created for USD")

        row = add_usd(ad(0.42, "usd"), UsdRateProvider(boom, boom, now=NOW))

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

    def test_rates_are_saved_the_moment_they_are_fetched(self):
        store = FakeStore()
        rates = provider(FakeClient(), store)

        rates.units_per_usd("EUR", date(2026, 10, 5))

        # Saved before the run yields a single row, so a load that fails later
        # does not throw the fetched rates away.
        self.assertEqual([(s.kind, s.rate_date) for s in store.snapshots], [("historical", date(2026, 10, 5))])

    def test_saved_rates_are_reused_by_the_next_run(self):
        store = FakeStore()
        provider(FakeClient(), store).units_per_usd("EUR", date(2026, 10, 5))
        provider(FakeClient(), store).units_per_usd("EUR", NOW.date())

        client = FakeClient()
        rates = provider(client, store, now=NOW + timedelta(hours=1))
        self.assertEqual(rates.units_per_usd("EUR", date(2026, 10, 5)), Decimal("0.8"))
        rates.units_per_usd("EUR", NOW.date())

        self.assertEqual(client.calls, [])

    def test_latest_is_refetched_after_ttl_and_on_a_new_day(self):
        store = FakeStore()
        provider(FakeClient(), store).units_per_usd("EUR", NOW.date())

        later = FakeClient()
        provider(later, store, now=NOW + timedelta(hours=fx.LATEST_TTL_HOURS)).units_per_usd("EUR", NOW.date())
        self.assertEqual(later.calls, [("latest", None)])

        tomorrow = NOW + timedelta(days=1, hours=-11)
        next_day = FakeClient()
        provider(next_day, store, now=tomorrow).units_per_usd("EUR", tomorrow.date())
        self.assertEqual(next_day.calls, [("latest", None)])

    def test_only_recent_days_are_read_back(self):
        store = FakeStore()

        provider(FakeClient(), store).units_per_usd("EUR", date(2026, 10, 5))

        self.assertEqual(store.loaded_since, NOW.date() - timedelta(days=fx.CACHE_LOOKBACK_DAYS))

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
        store = FakeStore()
        provider(FakeClient(), store, now=YESTERDAY).units_per_usd("EUR", YESTERDAY.date())

        rates = provider(FakeClient(historical_error=SnapshotNotPublished("not yet")), store)
        row = add_usd(ad(2, "EUR"), rates)

        self.assertEqual(row["cpc_price_usd"], 2.5)
        # The fallback is not saved as the close, so a later run still fetches it.
        self.assertEqual([s.kind for s in store.snapshots], ["latest"])

    def test_unpublished_day_without_fallback_raises(self):
        rates = provider(FakeClient(historical_error=SnapshotNotPublished("not yet")))

        with self.assertRaises(SnapshotNotPublished):
            rates.units_per_usd("EUR", date(2026, 10, 5))


class ClickHouseRateStoreTests(unittest.TestCase):
    def test_creates_table_and_saves_one_row_per_currency(self):
        client = mock.Mock()
        store = fx.ClickHouseRateStore(client)

        store.save(fx.Snapshot("historical", date(2026, 10, 5), NOW, {"EUR": Decimal("0.8"), "XBT": Decimal("0.0000000000000000001234")}))

        self.assertIn("CREATE TABLE IF NOT EXISTS inline_ad_fx_usd_rates", client.command.call_args[0][0])
        table, rows = client.insert.call_args[0]
        self.assertEqual(table, "inline_ad_fx_usd_rates")
        self.assertEqual(rows[0], ["historical", date(2026, 10, 5), "EUR", Decimal("0.8").quantize(fx.RATE_SCALE), NOW])
        self.assertEqual(rows[1][3], Decimal("0"))  # rounded to fit Decimal(38, 18)

    def test_load_groups_rows_into_snapshots(self):
        naive = datetime(2026, 10, 6, 6, 0)
        client = mock.Mock()
        client.query.return_value.result_rows = [
            ("latest", date(2026, 10, 6), "EUR", Decimal("0.8"), naive),
            ("latest", date(2026, 10, 6), "INR", Decimal("80"), naive + timedelta(hours=3)),
            ("historical", date(2026, 10, 5), "EUR", Decimal("0.81"), naive),
        ]

        snapshots = {s.kind: s for s in fx.ClickHouseRateStore(client).load(date(2026, 9, 22))}

        self.assertEqual(snapshots["latest"].rates, {"EUR": Decimal("0.8"), "INR": Decimal("80")})
        self.assertEqual(snapshots["latest"].fetched_at, datetime(2026, 10, 6, 9, 0, tzinfo=timezone.utc))
        self.assertEqual(snapshots["historical"].rates, {"EUR": Decimal("0.81")})
        self.assertEqual(client.query.call_args.kwargs["parameters"], {"since": date(2026, 9, 22)})


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
