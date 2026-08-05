import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call

from facebook_business.exceptions import FacebookRequestError

from pipelines.facebook.creative_status import (
    get_partial_resources,
    mark_partial_resource,
    reset_partial_resources,
)
from pipelines.facebook.runner import (
    raise_for_partial_resources,
    run_structural_resources,
)
from pipelines.facebook.structural import load_structural_resource


def _rate_limit_error(code=17):
    return FacebookRequestError(
        "rate limited",
        {},
        400,
        {},
        {
            "error": {
                "code": code,
                "error_subcode": 2446079,
                "message": "User request limit reached",
                "type": "OAuthException",
            }
        },
    )


class FacebookStructuralGuardTests(unittest.TestCase):
    def setUp(self):
        reset_partial_resources()

    def test_keeps_yielded_rows_and_marks_the_exact_resource(self):
        def rows():
            yield {"id": "ad-1"}
            raise _rate_limit_error()

        source_factory = lambda _cred: SimpleNamespace(ads=rows())

        loaded = list(
            load_structural_resource(
                [{"account_id": "act-1", "token": "secret"}],
                "d1c",
                source_factory=source_factory,
                source_attribute="ads",
                resource_name="ads",
            )
        )

        self.assertEqual(
            loaded,
            [{"id": "ad-1", "account_id": "act-1", "managing_system": "d1c"}],
        )
        self.assertEqual(get_partial_resources(), {"ads": ("act-1",)})

    def test_non_rate_limit_error_is_not_parked(self):
        error = _rate_limit_error(code=100)

        def rows():
            raise error
            yield

        with self.assertRaises(FacebookRequestError) as raised:
            list(
                load_structural_resource(
                    [{"account_id": "act-1", "token": "secret"}],
                    "d1c",
                    source_factory=lambda _cred: SimpleNamespace(ads=rows()),
                    source_attribute="ads",
                    resource_name="ads",
                )
            )

        self.assertIs(raised.exception, error)
        self.assertEqual(get_partial_resources(), {})

    def test_runner_checkpoints_resources_and_stops_after_current_account_throttle(self):
        pipeline = Mock()
        first = Mock(return_value="ads-resource")
        second = Mock(return_value="adsets-resource")
        third = Mock(return_value="creative-resource")

        def load(resource):
            if resource == "adsets-resource":
                mark_partial_resource("ad_sets", "act-1")

        pipeline.run.side_effect = load
        creds = [{"account_id": "act-1", "token": "secret"}]

        run_structural_resources(pipeline, [first, second, third], creds, "d1c")

        self.assertEqual(
            pipeline.run.call_args_list,
            [call("ads-resource"), call("adsets-resource")],
        )
        third.assert_not_called()

    def test_partial_account_does_not_stop_a_different_account(self):
        mark_partial_resource("ad_sets", "act-1")
        pipeline = Mock()
        first = Mock(return_value="ads-resource")
        second = Mock(return_value="adsets-resource")

        run_structural_resources(
            pipeline,
            [first, second],
            [{"account_id": "act-2", "token": "secret"}],
            "d1c",
        )

        self.assertEqual(
            pipeline.run.call_args_list,
            [call("ads-resource"), call("adsets-resource")],
        )

    def test_partial_error_explains_committed_rows_and_reconciliation(self):
        mark_partial_resource("ad_sets", "act-2")

        with self.assertRaisesRegex(
            RuntimeError,
            "Valid rows and checkpoints were committed.*no saved rows were corrupted",
        ):
            raise_for_partial_resources("Facebook")


if __name__ == "__main__":
    unittest.main()
