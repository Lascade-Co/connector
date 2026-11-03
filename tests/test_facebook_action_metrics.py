import unittest

from facebook_ads.helpers import flatten_facebook_insights
from facebook_ads.settings import INSIGHT_FIELDS_TYPES


def _entries(**values):
    return [{"action_type": k, "value": v} for k, v in values.items()]


class FacebookSubscriptionEventMetricTests(unittest.TestCase):
    def test_emits_stable_zeroes_for_missing_or_malformed_arrays(self):
        row = {"actions": "invalid", "action_values": [{"value": "bad"}]}

        flatten_facebook_insights(row)

        self.assertEqual(row["trial_starts"], 0.0)
        self.assertEqual(row["trial_start_value"], 0.0)
        self.assertEqual(row["subscriptions"], 0.0)
        self.assertEqual(row["subscription_value"], 0.0)

    def test_omni_rollups_suppress_channel_specific_aliases(self):
        row = {
            "actions": [
                {"action_type": "omni_start_trial", "value": "4.5"},
                {
                    "action_type": "offsite_conversion.fb_pixel_start_trial",
                    "value": "3",
                },
                {"action_type": "start_trial_mobile_app", "value": "2"},
                {"action_type": "omni_subscribe", "value": "7"},
                {
                    "action_type": "offsite_conversion.fb_pixel_subscribe",
                    "value": "5",
                },
            ]
        }

        flatten_facebook_insights(row)

        self.assertEqual(row["trial_starts"], 4.5)
        self.assertEqual(row["subscriptions"], 7.0)

    def test_combines_one_web_and_one_mobile_component(self):
        row = {
            "actions": [
                {
                    "action_type": "offsite_conversion.fb_pixel_start_trial",
                    "value": "3",
                },
                {"action_type": "start_trial_mobile_app", "value": "2"},
                {
                    "action_type": "offsite_conversion.fb_pixel_subscribe",
                    "value": "5",
                },
                {"action_type": "subscribe_mobile_app", "value": "1.5"},
            ]
        }

        flatten_facebook_insights(row)

        self.assertEqual(row["trial_starts"], 5.0)
        self.assertEqual(row["subscriptions"], 6.5)

    def test_deduplicates_same_type_across_conversion_and_action_arrays(self):
        row = {
            "conversions": [
                {"action_type": "subscribe_mobile_app", "value": "2"}
            ],
            "actions": [
                {"action_type": "subscribe_mobile_app", "value": "99"}
            ],
            "conversion_values": [
                {"action_type": "subscribe_mobile_app", "value": "12.25"}
            ],
            "action_values": [
                {"action_type": "subscribe_mobile_app", "value": "999"}
            ],
        }

        flatten_facebook_insights(row)

        self.assertEqual(row["subscriptions"], 2.0)
        self.assertEqual(row["subscription_value"], 12.25)

    def test_total_rollup_is_not_summed_with_its_website_and_app_parts(self):
        # Real figures from a production m8 load.
        row = {
            "conversions": _entries(
                subscribe_total="37",
                subscribe_website="29",
                subscribe_mobile_app="8",
                start_trial_total="4982",
                start_trial_website="4982",
            ),
            "conversion_values": _entries(
                subscribe_total="149.2",
                subscribe_website="149.2",
                start_trial_total="9702.59",
                start_trial_website="9702.59",
            ),
        }

        flatten_facebook_insights(row)

        self.assertEqual(row["subscriptions"], 37.0)
        self.assertEqual(row["subscription_value"], 149.2)
        self.assertEqual(row["trial_starts"], 4982.0)
        self.assertEqual(row["trial_start_value"], 9702.59)

    def test_website_and_app_parts_sum_when_no_rollup_is_reported(self):
        row = {
            "conversions": _entries(
                subscribe_website="29",
                subscribe_mobile_app="8",
                start_trial_website="12",
            )
        }

        flatten_facebook_insights(row)

        self.assertEqual(row["subscriptions"], 37.0)
        self.assertEqual(row["trial_starts"], 12.0)

    def test_pixel_and_website_names_count_once_as_the_web_component(self):
        row = {
            "conversions": [
                {"action_type": "offsite_conversion.fb_pixel_subscribe", "value": "5"},
                {"action_type": "subscribe_website", "value": "5"},
                {"action_type": "subscribe_mobile_app", "value": "1"},
            ]
        }

        flatten_facebook_insights(row)

        self.assertEqual(row["subscriptions"], 6.0)

    def test_omni_rollup_outranks_total_rollup_even_from_the_actions_array(self):
        row = {
            "conversions": _entries(subscribe_total="37"),
            "actions": _entries(omni_subscribe="40"),
        }

        flatten_facebook_insights(row)

        self.assertEqual(row["subscriptions"], 40.0)

    def test_requested_columns_have_fractional_schema_types(self):
        for column in (
            "trial_starts",
            "trial_start_value",
            "subscriptions",
            "subscription_value",
        ):
            with self.subTest(column=column):
                self.assertEqual(INSIGHT_FIELDS_TYPES[column]["data_type"], "decimal")


if __name__ == "__main__":
    unittest.main()
