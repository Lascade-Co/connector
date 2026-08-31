import logging
import unittest
from datetime import date
from decimal import Decimal
from unittest import mock

from facebook_ads.helpers import flatten_facebook_insights
from facebook_ads.insights import merge_report_rows, split_insight_fields
from facebook_ads.settings import DEFAULT_INSIGHT_FIELDS
from pipelines.facebook.currency import (
    AedRateProvider,
    RateUnavailable,
    budget_currency_map,
    convert_budget_row,
    convert_insights_row,
    fx_rate_rows,
)
from pipelines.facebook.ecb import EcbRateError, parse_observations
from pipelines.facebook.raw_sources import INSIGHT_FIELDS
from pipelines.facebook.facebook_ads_pipeline import resolve_dataset_name


def csv_series(*rows: str) -> str:
    header = (
        "KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,"
        "TIME_PERIOD,OBS_VALUE,OBS_STATUS"
    )
    return "\n".join((header, *rows)) + "\n"


def gbp(day: str, value: str) -> str:
    return f"EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,{day},{value},A"


def usd(day: str, value: str) -> str:
    return f"EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,{day},{value},A"


# The API groups by series, so a correct parser cannot rely on row order.
TWO_DAYS = csv_series(
    gbp("2026-08-27", "0.8574"),
    gbp("2026-08-28", "0.8572"),
    usd("2026-08-27", "1.1645"),
    usd("2026-08-28", "1.1643"),
)


class EcbParsingTests(unittest.TestCase):
    def test_derives_the_cross_rate_from_same_day_legs(self):
        observations = parse_observations(TWO_DAYS)

        self.assertEqual(
            sorted(observations), [date(2026, 8, 27), date(2026, 8, 28)]
        )
        # (usd_per_eur / gbp_per_eur) * 3.6725, rounded once at 10 places.
        self.assertEqual(
            observations[date(2026, 8, 28)].aed_per_gbp, Decimal("4.9882078278")
        )
        self.assertEqual(
            observations[date(2026, 8, 27)].aed_per_gbp, Decimal("4.9879009214")
        )

    def test_pairs_legs_by_date_not_by_position(self):
        shuffled = csv_series(
            gbp("2026-08-28", "0.8572"),
            gbp("2026-08-27", "0.8574"),
            usd("2026-08-27", "1.1645"),
            usd("2026-08-28", "1.1643"),
        )

        self.assertEqual(
            parse_observations(shuffled)[date(2026, 8, 28)].aed_per_gbp,
            Decimal("4.9882078278"),
        )

    def test_rejects_an_html_error_page_served_with_http_200(self):
        with self.assertRaisesRegex(EcbRateError, "not the expected CSV series"):
            parse_observations("<html><body>Service unavailable</body></html>")

    def test_rejects_an_empty_body(self):
        with self.assertRaisesRegex(EcbRateError, "not the expected CSV series"):
            parse_observations("")

    def test_rejects_a_series_that_was_never_requested(self):
        body = csv_series(
            "EXR.D.CHF.EUR.SP00.A,D,CHF,EUR,SP00,A,2026-08-28,0.94,A",
            usd("2026-08-28", "1.1643"),
        )

        with self.assertRaisesRegex(EcbRateError, "unrequested series"):
            parse_observations(body)

    def test_rejects_duplicate_observations_for_one_day(self):
        body = csv_series(
            gbp("2026-08-28", "0.8572"),
            gbp("2026-08-28", "0.8600"),
            usd("2026-08-28", "1.1643"),
        )

        with self.assertRaisesRegex(EcbRateError, "duplicate GBP observations"):
            parse_observations(body)

    def test_rejects_a_non_positive_rate(self):
        body = csv_series(gbp("2026-08-28", "0"), usd("2026-08-28", "1.1643"))

        with self.assertRaisesRegex(EcbRateError, "must be positive"):
            parse_observations(body)

    def test_rejects_a_non_numeric_rate(self):
        body = csv_series(gbp("2026-08-28", "n/a"), usd("2026-08-28", "1.1643"))

        with self.assertRaisesRegex(EcbRateError, "is not a number"):
            parse_observations(body)

    def test_skips_placeholder_rows_that_carry_no_value(self):
        body = csv_series(
            gbp("2026-08-28", ""),
            usd("2026-08-28", ""),
            gbp("2026-08-27", "0.8574"),
            usd("2026-08-27", "1.1645"),
        )

        self.assertEqual(sorted(parse_observations(body)), [date(2026, 8, 27)])

    def test_skips_a_day_missing_one_leg_without_failing(self):
        body = csv_series(
            gbp("2026-08-28", "0.8572"),
            gbp("2026-08-27", "0.8574"),
            usd("2026-08-27", "1.1645"),
        )

        with self.assertLogs(level=logging.WARNING):
            observations = parse_observations(body)

        self.assertEqual(sorted(observations), [date(2026, 8, 27)])

    def test_fails_when_no_day_carries_both_legs(self):
        body = csv_series(gbp("2026-08-28", "0.8572"))

        with self.assertLogs(level=logging.WARNING):
            with self.assertRaisesRegex(EcbRateError, "no day carrying both"):
                parse_observations(body)


class RateResolutionTests(unittest.TestCase):
    def setUp(self):
        self.observations = parse_observations(TWO_DAYS)
        self.fetches = []

    def provider(self, today: str) -> AedRateProvider:
        def fetch(start, end):
            self.fetches.append((start, end))
            return self.observations

        return AedRateProvider(fetch=fetch, today=date.fromisoformat(today))

    def test_exact_when_the_requested_day_was_published(self):
        rate = self.provider("2026-08-31").rate("GBP", date(2026, 8, 28))

        self.assertEqual(rate.status, "exact")
        self.assertEqual(rate.effective_date, date(2026, 8, 28))
        self.assertFalse(rate.is_provisional)

    def test_a_weekend_carries_the_prior_business_day_as_final(self):
        # 2026-08-29 is a Saturday, and a later observation proves the ECB has
        # moved past it, so the carry-forward can never be restated.
        self.observations = parse_observations(
            csv_series(
                gbp("2026-08-28", "0.8572"),
                gbp("2026-08-31", "0.8560"),
                usd("2026-08-28", "1.1643"),
                usd("2026-08-31", "1.1650"),
            )
        )

        rate = self.provider("2026-09-01").rate("GBP", date(2026, 8, 29))

        self.assertEqual(rate.status, "carried")
        self.assertEqual(rate.effective_date, date(2026, 8, 28))
        self.assertFalse(rate.is_provisional)

    def test_today_is_provisional_because_the_ecb_has_not_published_yet(self):
        rate = self.provider("2026-08-29").rate("GBP", date(2026, 8, 29))

        self.assertEqual(rate.status, "provisional")
        self.assertEqual(rate.effective_date, date(2026, 8, 28))
        self.assertTrue(rate.is_provisional)

    def test_a_provisional_rate_becomes_exact_once_published(self):
        provisional = self.provider("2026-08-29").rate("GBP", date(2026, 8, 29))
        self.assertTrue(provisional.is_provisional)

        self.observations = parse_observations(
            csv_series(
                gbp("2026-08-28", "0.8572"),
                gbp("2026-08-29", "0.8560"),
                usd("2026-08-28", "1.1643"),
                usd("2026-08-29", "1.1650"),
            )
        )
        settled = self.provider("2026-08-31").rate("GBP", date(2026, 8, 29))

        self.assertEqual(settled.status, "exact")
        self.assertNotEqual(settled.aed_per_source, provisional.aed_per_source)

    def test_warns_but_converts_when_the_rate_is_moderately_stale(self):
        with self.assertLogs(level=logging.WARNING) as captured:
            rate = self.provider("2026-09-03").rate("GBP", date(2026, 9, 2))

        self.assertEqual(rate.effective_date, date(2026, 8, 28))
        self.assertIn("days old", "".join(captured.output))

    def test_refuses_to_convert_once_the_rate_is_too_stale(self):
        with self.assertRaisesRegex(RateUnavailable, "days\\s+stale"):
            self.provider("2026-09-06").rate("GBP", date(2026, 9, 5))

    def test_fails_when_nothing_was_published_before_the_requested_day(self):
        with self.assertRaisesRegex(RateUnavailable, "no usable observation"):
            self.provider("2026-08-31").rate("GBP", date(2026, 8, 20))

    def test_aed_is_an_identity_rate_and_never_calls_the_ecb(self):
        provider = self.provider("2026-08-31")

        rate = provider.rate("AED", date(2026, 8, 28))

        self.assertEqual(rate.status, "identity")
        self.assertEqual(rate.aed_per_source, Decimal(1))
        self.assertEqual(provider.fetch_count, 0)
        self.assertEqual(self.fetches, [])

    def test_rejects_a_currency_with_no_configured_rate_source(self):
        for unsupported in ("GPB", "EUR", "USD"):
            with self.subTest(unsupported):
                with self.assertRaisesRegex(RateUnavailable, "No AED conversion"):
                    self.provider("2026-08-31").rate(unsupported, date(2026, 8, 28))

    def test_fetches_the_ecb_once_for_a_whole_run(self):
        provider = self.provider("2026-08-31")

        for day in range(27, 32):
            provider.rate("GBP", date(2026, 8, day))
        provider.rate("AED", date(2026, 8, 28))

        self.assertEqual(provider.fetch_count, 1)
        self.assertEqual(len(self.fetches), 1)

    def test_fetches_once_even_when_rows_arrive_newest_first(self):
        # Meta returns a window's rows in no guaranteed date order.
        provider = self.provider("2026-08-31")

        for day in reversed(range(27, 32)):
            provider.rate("GBP", date(2026, 8, day))

        self.assertEqual(provider.fetch_count, 1)

    def test_fetches_a_range_wide_enough_to_carry_a_rate_forward(self):
        self.provider("2026-08-31").rate("GBP", date(2026, 8, 28))

        start, end = self.fetches[0]
        self.assertLessEqual(start, date(2026, 8, 18), "needs carry-forward history")
        self.assertGreaterEqual(end, date(2026, 8, 31), "needs to reach today")


class CurrencyFieldRequestTests(unittest.TestCase):
    """Every row must state its currency, whichever split job produced it."""

    def test_currency_is_requested_in_both_split_reports(self):
        core, unique = split_insight_fields(list(INSIGHT_FIELDS))

        self.assertIn("account_currency", core)
        self.assertIn(
            "account_currency",
            unique,
            "a row returned only by the unique-metrics job would have no currency",
        )

    def test_a_row_from_only_the_unique_report_still_carries_the_currency(self):
        identity = {
            "campaign_id": "c",
            "adset_id": "s",
            "ad_id": "a",
            "date_start": "2026-08-28",
        }
        merged = merge_report_rows(
            [[], [{**identity, "account_currency": "GBP", "reach": "10"}]]
        )

        self.assertEqual(merged[0]["account_currency"], "GBP")

    def test_sibling_pipelines_do_not_request_the_currency(self):
        core, unique = split_insight_fields(list(DEFAULT_INSIGHT_FIELDS))

        self.assertNotIn("account_currency", core)
        self.assertNotIn("account_currency", unique)


class InsightsConversionTests(unittest.TestCase):
    def setUp(self):
        self.provider = AedRateProvider(
            fetch=lambda start, end: parse_observations(TWO_DAYS),
            today=date(2026, 8, 31),
        )
        self.rate = Decimal("4.9882078278")

    def gbp_row(self, **overrides):
        row = {
            "account_id": "555",
            "date_start": "2026-08-28",
            "account_currency": "GBP",
            "spend": "123.37",
            "cpc": "0.51",
            "cpm": "7.20",
            "cpp": "9.10",
            "ctr": "1.23",
            "unique_ctr": "0.98",
            "frequency": "1.4",
            "impressions": "4000",
            "clicks": "50",
            "reach": "2800",
        }
        row.update(overrides)
        return row

    def expected(self, raw: str) -> str:
        return str((Decimal(raw) * self.rate).quantize(Decimal("0.000001")))

    def test_converts_every_monetary_scalar(self):
        row = self.gbp_row()

        convert_insights_row(row, self.provider)

        self.assertEqual(row["spend"], self.expected("123.37"))
        self.assertEqual(row["cpc"], self.expected("0.51"))
        self.assertEqual(row["cpm"], self.expected("7.20"))
        self.assertEqual(row["cpp"], self.expected("9.10"))
        self.assertEqual(row["spend_source"], "123.37")

    def test_leaves_ratios_and_counts_untouched(self):
        row = self.gbp_row(
            purchase_roas=[{"action_type": "omni_purchase", "value": "3.5"}],
            website_ctr=[{"action_type": "link_click", "value": "0.8"}],
            actions=[{"action_type": "link_click", "value": "50", "7d_click": "50"}],
        )

        convert_insights_row(row, self.provider)

        self.assertEqual(row["ctr"], "1.23")
        self.assertEqual(row["unique_ctr"], "0.98")
        self.assertEqual(row["frequency"], "1.4")
        self.assertEqual(row["impressions"], "4000")
        self.assertEqual(row["clicks"], "50")
        self.assertEqual(row["reach"], "2800")
        self.assertEqual(row["purchase_roas"][0]["value"], "3.5")
        self.assertEqual(row["website_ctr"][0]["value"], "0.8")
        self.assertEqual(row["actions"][0], {
            "action_type": "link_click",
            "value": "50",
            "7d_click": "50",
        })

    def test_converts_nested_values_and_every_attribution_window(self):
        row = self.gbp_row(
            action_values=[
                {
                    "action_type": "omni_purchase",
                    "value": "115.22",
                    "1d_click": "100.00",
                    "7d_click": "110.00",
                    "28d_click": "115.22",
                    "1d_view": "5.00",
                    "7d_view": "6.00",
                    "28d_view": "7.00",
                }
            ],
            cost_per_action_type=[
                {"action_type": "link_click", "value": "2.50", "7d_click": "2.50"}
            ],
        )

        convert_insights_row(row, self.provider)

        entry = row["action_values"][0]
        self.assertEqual(entry["value"], self.expected("115.22"))
        self.assertEqual(entry["value_source"], "115.22")
        for window, raw in (
            ("1d_click", "100.00"),
            ("7d_click", "110.00"),
            ("28d_click", "115.22"),
            ("1d_view", "5.00"),
            ("7d_view", "6.00"),
            ("28d_view", "7.00"),
        ):
            with self.subTest(window):
                self.assertEqual(entry[window], self.expected(raw))
        self.assertEqual(entry["action_type"], "omni_purchase")

        cpa = row["cost_per_action_type"][0]
        self.assertEqual(cpa["value"], self.expected("2.50"))
        self.assertEqual(cpa["7d_click"], self.expected("2.50"))

    def test_converts_every_entry_when_an_action_type_repeats(self):
        row = self.gbp_row(
            action_values=[
                {"action_type": "omni_purchase", "value": "10.00"},
                {"action_type": "omni_purchase", "value": "20.00"},
            ]
        )

        convert_insights_row(row, self.provider)

        self.assertEqual(
            [entry["value"] for entry in row["action_values"]],
            [self.expected("10.00"), self.expected("20.00")],
        )

    def test_converts_the_cost_per_result_series(self):
        row = self.gbp_row(
            cost_per_result=[
                {"indicator": "cost_per_link_click", "values": [{"value": "0.97"}]}
            ]
        )

        convert_insights_row(row, self.provider)

        self.assertEqual(
            row["cost_per_result"][0]["values"][0]["value"], self.expected("0.97")
        )

    def test_leaves_blank_and_missing_amounts_alone(self):
        row = self.gbp_row(spend="", cpc=None)
        row.pop("cpp")

        convert_insights_row(row, self.provider)

        self.assertEqual(row["spend"], "")
        self.assertIsNone(row["cpc"])
        self.assertNotIn("cpp", row)

    def test_refuses_a_non_numeric_monetary_value(self):
        row = self.gbp_row(spend="unknown")

        with self.assertRaisesRegex(RateUnavailable, "non-numeric monetary value"):
            convert_insights_row(row, self.provider)

    def test_stamps_the_rate_it_used_onto_the_row(self):
        row = self.gbp_row()

        convert_insights_row(row, self.provider)

        self.assertEqual(row["source_currency"], "GBP")
        self.assertEqual(row["reporting_currency"], "AED")
        self.assertEqual(row["fx_rate_to_aed"], str(self.rate))
        self.assertEqual(row["fx_requested_date"], "2026-08-28")
        self.assertEqual(row["fx_effective_date"], "2026-08-28")
        self.assertEqual(row["fx_rate_status"], "exact")

    def test_an_aed_row_keeps_its_stored_strings_byte_identical(self):
        row = self.gbp_row(account_currency="AED", spend="12.34", cpc="0", cpm="7.20")
        row["action_values"] = [{"action_type": "omni_purchase", "value": "115.22"}]

        convert_insights_row(row, self.provider)

        self.assertEqual(row["spend"], "12.34")
        self.assertEqual(row["cpc"], "0")
        self.assertEqual(row["cpm"], "7.20")
        self.assertEqual(row["action_values"][0]["value"], "115.22")
        self.assertNotIn("value_source", row["action_values"][0])
        self.assertEqual(row["source_currency"], "AED")
        self.assertEqual(row["fx_rate_to_aed"], "1")
        self.assertEqual(row["fx_rate_status"], "identity")
        self.assertEqual(self.provider.fetch_count, 0)

    def test_refuses_a_row_meta_gave_no_currency_for(self):
        for missing in (None, "", "   "):
            with self.subTest(repr(missing)):
                row = self.gbp_row(account_currency=missing)
                with self.assertRaisesRegex(RateUnavailable, "no account_currency"):
                    convert_insights_row(row, self.provider)

    def test_refuses_a_row_with_an_unusable_date(self):
        row = self.gbp_row(date_start="not-a-date")

        with self.assertRaisesRegex(RateUnavailable, "conversion date"):
            convert_insights_row(row, self.provider)

    def test_records_the_account_currency_for_later_budget_conversion(self):
        convert_insights_row(self.gbp_row(), self.provider)

        self.assertEqual(self.provider.account_currency("555", "unused-token"), "GBP")


class FlattenedScalarTests(unittest.TestCase):
    """The flattened Float64 columns must inherit the conversion."""

    def test_flattening_after_conversion_yields_aed_scalars(self):
        provider = AedRateProvider(
            fetch=lambda start, end: parse_observations(TWO_DAYS),
            today=date(2026, 8, 31),
        )
        rate = Decimal("4.9882078278")
        row = {
            "account_id": "555",
            "date_start": "2026-08-28",
            "account_currency": "GBP",
            "spend": "100.00",
            "action_values": [
                {"action_type": "omni_purchase", "value": "115.22"},
                {"action_type": "omni_start_trial", "value": "9.00"},
                {"action_type": "omni_subscribe", "value": "11.00"},
            ],
            "cost_per_action_type": [{"action_type": "link_click", "value": "2.50"}],
            "actions": [
                {"action_type": "link_click", "value": "50"},
                {"action_type": "omni_start_trial", "value": "3"},
                {"action_type": "omni_subscribe", "value": "2"},
            ],
            "cost_per_result": [{"indicator": "x", "values": [{"value": "0.97"}]}],
        }

        flatten_facebook_insights(convert_insights_row(row, provider))

        def aed(raw: str) -> float:
            return float((Decimal(raw) * rate).quantize(Decimal("0.000001")))

        self.assertAlmostEqual(row["action_values_omni_purchase"], aed("115.22"), 6)
        self.assertAlmostEqual(row["cpa_link_click"], aed("2.50"), 6)
        self.assertAlmostEqual(row["cost_per_result"], aed("0.97"), 6)
        self.assertAlmostEqual(row["trial_start_value"], aed("9.00"), 6)
        self.assertAlmostEqual(row["subscription_value"], aed("11.00"), 6)
        # Counts stay counts.
        self.assertEqual(row["actions_link_click"], 50.0)
        self.assertEqual(row["trial_starts"], 3.0)
        self.assertEqual(row["subscriptions"], 2.0)


class BudgetConversionTests(unittest.TestCase):
    def setUp(self):
        self.provider = AedRateProvider(
            fetch=lambda start, end: parse_observations(TWO_DAYS),
            today=date(2026, 8, 28),
        )
        self.rate = Decimal("4.9882078278")

    def test_converts_minor_units_and_rounds_the_integer_bid(self):
        self.provider.note_account_currency("555", "GBP")
        row = {
            "account_id": "555",
            "daily_budget": "5000",
            "lifetime_budget": "125000",
            "bid_amount": 250,
        }

        convert_budget_row(row, self.provider, "unused-token")

        self.assertEqual(row["daily_budget"], "24941")
        self.assertEqual(row["daily_budget_source"], "5000")
        self.assertEqual(row["lifetime_budget"], "623526")
        self.assertEqual(row["bid_amount"], 1247)
        self.assertIsInstance(row["bid_amount"], int)
        self.assertEqual(row["bid_amount_source"], 250)
        self.assertEqual(row["source_currency"], "GBP")
        self.assertEqual(row["fx_rate_to_aed"], str(self.rate))
        self.assertEqual(row["fx_rate_status"], "exact")

    def test_leaves_an_aed_account_budget_untouched(self):
        self.provider.note_account_currency("777", "AED")
        row = {"account_id": "777", "daily_budget": "5000", "bid_amount": 250}

        convert_budget_row(row, self.provider, "unused-token")

        self.assertEqual(row["daily_budget"], "5000")
        self.assertEqual(row["bid_amount"], 250)
        self.assertNotIn("daily_budget_source", row)
        self.assertEqual(row["source_currency"], "AED")
        self.assertEqual(self.provider.fetch_count, 0)

    def test_asks_meta_only_when_insights_did_not_report_the_currency(self):
        with mock.patch(
            "pipelines.facebook.currency._fetch_account_currency",
            return_value="GBP",
        ) as fetch_currency:
            transform = budget_currency_map(self.provider, "a-token")
            transform({"account_id": "888", "daily_budget": "100"})
            transform({"account_id": "888", "daily_budget": "200"})

        fetch_currency.assert_called_once_with("888", "a-token")

    def test_skips_blank_budgets(self):
        self.provider.note_account_currency("555", "GBP")
        row = {"account_id": "555", "daily_budget": None, "lifetime_budget": ""}

        convert_budget_row(row, self.provider, "unused-token")

        self.assertIsNone(row["daily_budget"])
        self.assertEqual(row["lifetime_budget"], "")


class FxAuditRowTests(unittest.TestCase):
    def test_records_the_legs_behind_each_converted_rate(self):
        provider = AedRateProvider(
            fetch=lambda start, end: parse_observations(TWO_DAYS),
            today=date(2026, 8, 31),
        )
        provider.rate("GBP", date(2026, 8, 28))
        provider.rate("AED", date(2026, 8, 28))

        rows = fx_rate_rows(provider.resolved_rates(), provider.retrieved_at)

        self.assertEqual(len(rows), 1, "identity rates are not worth storing")
        self.assertEqual(
            rows[0],
            {
                "source_currency": "GBP",
                "target_currency": "AED",
                "requested_date": "2026-08-28",
                "effective_date": "2026-08-28",
                "gbp_per_eur": "0.8572",
                "usd_per_eur": "1.1643",
                "usd_aed_reference": "3.6725",
                "aed_per_source": "4.9882078278",
                "status": "exact",
                "is_provisional": False,
                "method_version": "ecb-cross-usd-peg-1",
                "retrieved_at": provider.retrieved_at,
            },
        )

    def test_records_nothing_for_an_all_aed_run(self):
        provider = AedRateProvider(fetch=lambda start, end: {}, today=date(2026, 8, 31))
        provider.rate("AED", date(2026, 8, 28))

        self.assertEqual(fx_rate_rows(provider.resolved_rates(), None), [])


class DatasetNameTests(unittest.TestCase):
    def resolve(self, **env):
        with mock.patch.dict("os.environ", env, clear=True):
            return resolve_dataset_name()

    def test_defaults_to_production(self):
        self.assertEqual(self.resolve(), "fb")
        self.assertEqual(self.resolve(FB_DATASET_NAME="fb"), "fb")

    def test_allows_a_smoke_dataset(self):
        with self.assertLogs(level=logging.WARNING):
            self.assertEqual(
                self.resolve(FB_DATASET_NAME="fb_smoke_gbp_1"), "fb_smoke_gbp_1"
            )

    def test_rejects_an_arbitrary_dataset_name(self):
        for bad in ("travel", "fb2", "fb_smoke_", "FB_SMOKE_X", "fb_smoke_x;drop"):
            with self.subTest(bad):
                with self.assertRaises(SystemExit):
                    self.resolve(FB_DATASET_NAME=bad)

    def test_rejects_any_override_on_a_scheduled_run(self):
        with self.assertRaises(SystemExit):
            self.resolve(FB_DATASET_NAME="fb_smoke_x", GITHUB_EVENT_NAME="schedule")

    def test_still_allows_production_on_a_scheduled_run(self):
        self.assertEqual(
            self.resolve(FB_DATASET_NAME="fb", GITHUB_EVENT_NAME="schedule"), "fb"
        )


if __name__ == "__main__":
    unittest.main()
