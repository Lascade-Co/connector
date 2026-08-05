import unittest

from pipelines.subscription_facebook.sources import _subscribe_revenue


class SubscriptionFacebookCompatibilityMetricTests(unittest.TestCase):
    def test_legacy_revenue_keeps_custom_subscription_labels(self):
        values = [
            {
                "action_type": "offsite_conversion.custom.123",
                "label": "Annual Subscribe Complete",
                "value": "19.995",
            },
            {
                "action_type": "subscribe_mobile_app",
                "value": "5.25",
            },
            {
                "action_type": "offsite_conversion.fb_pixel_purchase",
                "label": "Purchase",
                "value": "99",
            },
        ]

        self.assertEqual(_subscribe_revenue(values), 25.245)

    def test_legacy_revenue_ignores_malformed_values(self):
        values = [
            {"action_type": "subscribe_mobile_app", "value": "invalid"},
            {"label": "Subscribe", "value": None},
            "invalid",
        ]

        self.assertEqual(_subscribe_revenue(values), 0.0)


if __name__ == "__main__":
    unittest.main()
