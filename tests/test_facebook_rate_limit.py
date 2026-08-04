import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from facebook_ads import rate_limit


def _response(headers, status_code=200):
    return SimpleNamespace(
        headers=headers,
        status_code=status_code,
        request=SimpleNamespace(url="https://graph.facebook.com/v24.0/act_1/insights"),
    )


class MetaUsageHeaderTests(unittest.TestCase):
    def test_parses_all_supported_header_families_and_shapes(self):
        headers = {
            "X-Business-Use-Case-Usage": json.dumps(
                {
                    "act_1": [
                        {
                            "call_count": 91,
                            "estimated_time_to_regain_access": 4,
                            "ads_api_access_tier": "standard_access",
                        }
                    ]
                }
            ),
            "x-ad-account-usage": json.dumps(
                {"acc_id_util_pct": 88, "reset_time_duration": 15}
            ),
            "X-APP-USAGE": json.dumps(
                {"call_count": 72, "total_cputime": 73, "total_time": 74}
            ),
            "x-fb-ads-insights-throttle": json.dumps(
                [{"app_id_util_pct": 96}]
            ),
        }

        snapshot = rate_limit.parse_usage_headers(headers)

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.utilization_pct, 96.0)
        self.assertEqual(snapshot.reset_seconds, 240)
        self.assertEqual(snapshot.access_tier, "standard_access")
        self.assertEqual(
            snapshot.header_names,
            (
                "x-business-use-case-usage",
                "x-ad-account-usage",
                "x-app-usage",
                "x-fb-ads-insights-throttle",
            ),
        )

    def test_returns_none_without_any_parseable_usage_header(self):
        self.assertIsNone(rate_limit.parse_usage_headers({"content-type": "json"}))

    def test_success_near_saturation_uses_bounded_proactive_pacing(self):
        response = _response(
            {
                "x-business-use-case-usage": json.dumps(
                    {
                        "act_1": [
                            {
                                "call_count": 99,
                                "estimated_time_to_regain_access": 10,
                            }
                        ]
                    }
                )
            }
        )
        with (
            patch.dict(
                rate_limit.os.environ,
                {
                    "FB_RATE_LIMIT_PACE_THRESHOLD": "90",
                    "FB_RATE_LIMIT_MAX_PACE_SECONDS": "30",
                },
                clear=True,
            ),
            patch.object(rate_limit.time, "sleep") as sleep,
        ):
            returned = rate_limit.observe_meta_response(response)

        self.assertIs(returned, response)
        sleep.assert_called_once_with(30)

    def test_error_response_never_sleeps_even_when_usage_is_saturated(self):
        response = _response(
            {"x-app-usage": json.dumps({"call_count": 100})},
            status_code=429,
        )
        with (
            patch.dict(
                rate_limit.os.environ,
                {
                    "FB_RATE_LIMIT_PACE_THRESHOLD": "90",
                    "FB_RATE_LIMIT_MAX_PACE_SECONDS": "30",
                },
                clear=True,
            ),
            patch.object(rate_limit.time, "sleep") as sleep,
        ):
            returned = rate_limit.observe_meta_response(response)

        self.assertIs(returned, response)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
