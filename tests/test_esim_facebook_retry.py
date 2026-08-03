import json
import unittest
from unittest.mock import patch

from dlt.extract.exceptions import ResourceExtractionError
from facebook_business.exceptions import FacebookRequestError

from pipelines.facebook import rate_limit
from pipelines.esim_facebook import sources
from pipelines.esim_facebook import esim_facebook_pipeline


def _rate_limit_error(minutes: int, code: int = 17) -> FacebookRequestError:
    headers = {
        "x-business-use-case-usage": json.dumps(
            {"account": [{"estimated_time_to_regain_access": minutes}]}
        )
    }
    return FacebookRequestError(
        "rate limited",
        {},
        429,
        headers,
        {"error": {"code": code, "message": "rate limited", "type": "OAuthException"}},
    )


def _wrapped(error: FacebookRequestError) -> ResourceExtractionError:
    wrapped = ResourceExtractionError(
        "ad_creatives", iter(()), "creative extraction failed", "resource"
    )
    wrapped.__cause__ = error
    return wrapped


def _run_creatives(accounts):
    return list(sources.creatives_all._pipe.gen(accounts, "esim"))


class EsimFacebookCreativeRetryTests(unittest.TestCase):
    def setUp(self):
        sources.reset_partial_creative_accounts()

    def test_waits_through_exact_cap_and_retries_direct_rate_limit(self):
        calls = 0

        def stream(cred, group_name):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise _rate_limit_error(minutes=30)
            yield {"id": "creative-1", "account_id": cred["account_id"]}

        with (
            patch.object(sources, "_stream_creatives", side_effect=stream),
            patch.object(rate_limit.time, "sleep") as sleep,
        ):
            rows = _run_creatives([{"account_id": "act-1", "token": "secret"}])

        self.assertEqual([row["id"] for row in rows], ["creative-1"])
        self.assertEqual(calls, 2)
        sleep.assert_called_once_with(rate_limit.WAIT_CAP_SECONDS)

    def test_unwraps_dlt_error_before_waiting_and_retrying(self):
        calls = 0

        def stream(_cred, _group_name):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise _wrapped(_rate_limit_error(minutes=2))
            yield {"id": "creative-1"}

        with (
            patch.object(sources, "_stream_creatives", side_effect=stream),
            patch.object(rate_limit.time, "sleep") as sleep,
        ):
            rows = _run_creatives([{"account_id": "act-1", "token": "secret"}])

        self.assertEqual([row["id"] for row in rows], ["creative-1"])
        sleep.assert_called_once_with(120)

    def test_wait_above_cap_skips_account_and_continues(self):
        def stream(cred, _group_name):
            if cred["account_id"] == "act-limited":
                raise _rate_limit_error(minutes=31)
            yield {"id": "creative-next"}

        accounts = [
            {"account_id": "act-limited", "token": "secret"},
            {"account_id": "act-next", "token": "secret"},
        ]
        with (
            patch.object(sources, "_stream_creatives", side_effect=stream),
            patch.object(rate_limit.time, "sleep") as sleep,
        ):
            rows = _run_creatives(accounts)

        self.assertEqual([row["id"] for row in rows], ["creative-next"])
        sleep.assert_not_called()
        self.assertEqual(sources.get_partial_creative_accounts(), ("act-limited",))

    def test_second_rate_limit_skips_account_and_continues(self):
        attempts = {"act-limited": 0, "act-next": 0}

        def stream(cred, _group_name):
            account_id = cred["account_id"]
            attempts[account_id] += 1
            if account_id == "act-limited":
                raise _rate_limit_error(minutes=1)
            yield {"id": "creative-next"}

        accounts = [
            {"account_id": "act-limited", "token": "secret"},
            {"account_id": "act-next", "token": "secret"},
        ]
        with (
            patch.object(sources, "_stream_creatives", side_effect=stream),
            patch.object(rate_limit.time, "sleep") as sleep,
        ):
            rows = _run_creatives(accounts)

        self.assertEqual([row["id"] for row in rows], ["creative-next"])
        self.assertEqual(attempts, {"act-limited": 2, "act-next": 1})
        sleep.assert_called_once_with(60)
        self.assertEqual(sources.get_partial_creative_accounts(), ("act-limited",))

    def test_non_rate_limit_failure_on_first_attempt_is_raised(self):
        error = _rate_limit_error(minutes=1, code=100)

        def stream(_cred, _group_name):
            raise error
            yield

        with patch.object(sources, "_stream_creatives", side_effect=stream):
            with self.assertRaises(FacebookRequestError) as raised:
                _run_creatives([{"account_id": "act-1", "token": "secret"}])

        self.assertIs(raised.exception, error)

    def test_non_rate_limit_failure_on_retry_is_raised(self):
        attempts = 0
        permission_error = _rate_limit_error(minutes=1, code=100)

        def stream(_cred, _group_name):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise _rate_limit_error(minutes=1)
            raise permission_error
            yield

        with (
            patch.object(sources, "_stream_creatives", side_effect=stream),
            patch.object(rate_limit.time, "sleep"),
        ):
            with self.assertRaises(FacebookRequestError) as raised:
                _run_creatives([{"account_id": "act-1", "token": "secret"}])

        self.assertIs(raised.exception, permission_error)

    def test_retry_restarts_stream_and_may_repeat_partial_rows(self):
        attempts = 0

        def stream(_cred, _group_name):
            nonlocal attempts
            attempts += 1
            yield {"id": "creative-1"}
            if attempts == 1:
                raise _rate_limit_error(minutes=1)
            yield {"id": "creative-2"}

        with (
            patch.object(sources, "_stream_creatives", side_effect=stream),
            patch.object(rate_limit.time, "sleep"),
        ):
            rows = _run_creatives([{"account_id": "act-1", "token": "secret"}])

        self.assertEqual(
            [row["id"] for row in rows],
            ["creative-1", "creative-1", "creative-2"],
        )


class EsimFacebookAccountDelayTests(unittest.TestCase):
    def test_configured_delay_runs_only_between_accounts(self):
        events = []

        def source(creds, group_name):
            return {
                "account_id": creds[0]["account_id"],
                "group_name": group_name,
            }

        with (
            patch.object(
                esim_facebook_pipeline.sys,
                "argv",
                ["main.py", "esim_facebook", "daily"],
            ),
            patch.object(
                esim_facebook_pipeline,
                "get_for_group",
                return_value=(
                    {"token": "secret"},
                    ["act-first", "act-second"],
                ),
            ),
            patch.object(esim_facebook_pipeline, "all_sources", [source]),
            patch.object(esim_facebook_pipeline.dlt, "pipeline") as pipeline_factory,
            patch.dict(
                esim_facebook_pipeline.os.environ,
                {"ESIM_FB_ACCOUNT_DELAY_SECONDS": "7"},
            ),
            patch.object(
                esim_facebook_pipeline.time,
                "sleep",
                side_effect=lambda seconds: events.append(("sleep", seconds)),
            ) as sleep,
        ):
            pipeline_factory.return_value.run.side_effect = lambda resources: events.append(
                ("run", resources[0]["account_id"])
            )
            esim_facebook_pipeline.run()

        sleep.assert_called_once_with(7)
        self.assertEqual(
            events,
            [("run", "act-first"), ("sleep", 7), ("run", "act-second")],
        )
        self.assertEqual(
            [call.args[0] for call in pipeline_factory.return_value.run.call_args_list],
            [
                [{"account_id": "act-first", "group_name": "daily"}],
                [{"account_id": "act-second", "group_name": "daily"}],
            ],
        )

    def test_partial_creative_load_fails_after_committed_account_runs(self):
        with (
            patch.object(
                esim_facebook_pipeline.sys,
                "argv",
                ["main.py", "esim_facebook", "daily"],
            ),
            patch.object(
                esim_facebook_pipeline,
                "get_for_group",
                return_value=({"token": "secret"}, ["act-1"]),
            ),
            patch.object(esim_facebook_pipeline, "all_sources", [lambda *_args: {}]),
            patch.object(esim_facebook_pipeline.dlt, "pipeline") as pipeline_factory,
            patch.object(
                esim_facebook_pipeline,
                "get_partial_creative_accounts",
                return_value=("act-1",),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "act-1"):
                esim_facebook_pipeline.run()

        pipeline_factory.return_value.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
