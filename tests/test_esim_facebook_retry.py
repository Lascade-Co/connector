import json
import unittest
from unittest.mock import patch

from dlt.extract.exceptions import ResourceExtractionError
from facebook_business.exceptions import FacebookRequestError

from pipelines.esim_facebook import esim_facebook_pipeline, sources


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


class EsimFacebookCreativeParkingTests(unittest.TestCase):
    def setUp(self):
        sources.reset_partial_creative_accounts()

    def test_direct_rate_limit_parks_without_sleep_or_retry(self):
        calls = 0

        def stream(_cred, _group_name):
            nonlocal calls
            calls += 1
            raise _rate_limit_error(minutes=30)
            yield

        with patch.object(sources, "_stream_creatives", side_effect=stream):
            rows = _run_creatives([{"account_id": "act-limited", "token": "secret"}])

        self.assertEqual(rows, [])
        self.assertEqual(calls, 1)
        self.assertEqual(sources.get_partial_creative_accounts(), ("act-limited",))

    def test_wrapped_rate_limit_parks_and_continues_to_next_account(self):
        attempts = {"act-limited": 0, "act-next": 0}

        def stream(cred, _group_name):
            account_id = cred["account_id"]
            attempts[account_id] += 1
            if account_id == "act-limited":
                raise _wrapped(_rate_limit_error(minutes=2))
            yield {"id": "creative-next"}

        rows = []
        with patch.object(sources, "_stream_creatives", side_effect=stream):
            rows = _run_creatives(
                [
                    {"account_id": "act-limited", "token": "secret"},
                    {"account_id": "act-next", "token": "secret"},
                ]
            )

        self.assertEqual([row["id"] for row in rows], ["creative-next"])
        self.assertEqual(attempts, {"act-limited": 1, "act-next": 1})
        self.assertEqual(sources.get_partial_creative_accounts(), ("act-limited",))

    def test_partial_rows_are_not_replayed_before_account_is_parked(self):
        calls = 0

        def stream(_cred, _group_name):
            nonlocal calls
            calls += 1
            yield {"id": "creative-1"}
            raise _rate_limit_error(minutes=1)

        with patch.object(sources, "_stream_creatives", side_effect=stream):
            rows = _run_creatives([{"account_id": "act-limited", "token": "secret"}])

        self.assertEqual([row["id"] for row in rows], ["creative-1"])
        self.assertEqual(calls, 1)
        self.assertEqual(sources.get_partial_creative_accounts(), ("act-limited",))

    def test_non_rate_limit_failure_is_raised(self):
        error = _rate_limit_error(minutes=1, code=100)

        def stream(_cred, _group_name):
            raise error
            yield

        with patch.object(sources, "_stream_creatives", side_effect=stream):
            with self.assertRaises(FacebookRequestError) as raised:
                _run_creatives([{"account_id": "act-1", "token": "secret"}])

        self.assertIs(raised.exception, error)


class EsimFacebookRunnerTests(unittest.TestCase):
    def test_runs_insights_before_structural_resources_and_delays_between_accounts(
        self,
    ):
        events = []

        def run_insights(_pipeline, _resource, creds, _group_name, **_kwargs):
            events.append(("insights", creds[0]["account_id"]))

        insights = object()

        def structural(creds, group_name):
            return {
                "kind": "structural",
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
                return_value=({"token": "secret"}, ["act-first", "act-second"]),
            ),
            patch.object(esim_facebook_pipeline, "all_sources", [insights, structural]),
            patch.object(
                esim_facebook_pipeline,
                "run_insights_in_windows",
                side_effect=run_insights,
            ),
            patch.object(esim_facebook_pipeline.dlt, "pipeline") as pipeline_factory,
            patch.dict(
                esim_facebook_pipeline.os.environ,
                {
                    "GITHUB_ACTIONS": "true",
                    "CI": "true",
                    "GITHUB_RUN_ID": "12345",
                    "GITHUB_REPOSITORY": "example/connector",
                    "RUNNER_NAME": "GitHub Actions 1",
                    "ESIM_FB_ACCOUNT_DELAY_SECONDS": "7",
                },
                clear=True,
            ),
            patch.object(
                esim_facebook_pipeline.time,
                "sleep",
                side_effect=lambda seconds: events.append(("sleep", seconds)),
            ) as sleep,
        ):

            def record_run(resources):
                batch = resources if isinstance(resources, list) else [resources]
                events.extend(
                    (resource["kind"], resource["account_id"]) for resource in batch
                )

            pipeline_factory.return_value.run.side_effect = record_run
            esim_facebook_pipeline.run()

        sleep.assert_called_once_with(7)
        self.assertEqual(
            events,
            [
                ("insights", "act-first"),
                ("structural", "act-first"),
                ("sleep", 7),
                ("insights", "act-second"),
                ("structural", "act-second"),
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
            patch.object(
                esim_facebook_pipeline,
                "all_sources",
                [
                    lambda *_args: {"kind": "insights"},
                    lambda *_args: {"kind": "structural"},
                ],
            ),
            patch.object(
                esim_facebook_pipeline, "run_insights_in_windows"
            ) as run_insights,
            patch.object(esim_facebook_pipeline.dlt, "pipeline") as pipeline_factory,
            patch.object(
                esim_facebook_pipeline,
                "get_partial_creative_accounts",
                return_value=("act-1",),
            ),
            patch.dict(
                esim_facebook_pipeline.os.environ,
                {
                    "GITHUB_ACTIONS": "true",
                    "CI": "true",
                    "GITHUB_RUN_ID": "12345",
                    "GITHUB_REPOSITORY": "example/connector",
                    "RUNNER_NAME": "GitHub Actions 1",
                },
                clear=True,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "act-1"):
                esim_facebook_pipeline.run()

        run_insights.assert_called_once()
        pipeline_factory.return_value.run.assert_called_once_with(
            [{"kind": "structural"}]
        )


if __name__ == "__main__":
    unittest.main()
