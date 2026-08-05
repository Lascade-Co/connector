import unittest

from facebook_ads.helpers import flatten_facebook_insights
from facebook_ads.settings import INSIGHT_FIELDS_TYPES


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
