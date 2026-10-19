"""Mock-only focused probe coverage; execute through GitHub Actions."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from facebook_business.exceptions import FacebookRequestError

from pipelines.facebook import probe_creative as diagnostic


def facebook_error(code=100, subcode=2446211, http_status=400):
    return FacebookRequestError(
        "private exception message", {"access_token": "private-request-token"},
        http_status, {}, {"error": {
            "message": "private-error-message", "code": code,
            "error_subcode": subcode, "fbtrace_id": "trace-123",
        }},
    )


class CreativeProbeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name)
        self.api = Mock()
        self.account = SimpleNamespace(get_api=Mock(return_value=self.api))
        self.report = dict(
            account_id="123", group="d2a", creative_id="456", ad_id="789",
            api_version="v25.0", status="incomplete", probes=[], observations={},
        )
        self.calls = []
        self.creative_objects = []
        self.direct_outcome = lambda fields: self.payload(fields)
        self.batch_outcome = lambda: self.payload(diagnostic.DEFAULT_ADCREATIVE_FIELDS)
        self.ad_payload = {"id": "789", "account_id": "123", "creative": {"id": "456"}}
        self.batch_mode = "normal"

    def payload(self, fields):
        # Realistic SDK responses only contain requested fields plus identity.
        structured = {"object_story_spec", "asset_feed_spec", "degrees_of_freedom_spec"}
        return {
            field: ({} if field in structured else "456" if field == "id" else "private-success-" + field)
            for field in fields
        }

    def install_sdk(self):
        def creative(fbid, api):
            self.assertEqual(fbid, "456")
            self.assertIs(api, self.api)
            instance = SimpleNamespace()
            self.creative_objects.append(instance)

            def api_get(*, fields, batch=None, success=None, failure=None):
                self.calls.append(("batch" if batch is not None else "direct", tuple(fields), instance))
                if batch is not None:
                    batch.callbacks.append((success, failure))
                    return None
                outcome = self.direct_outcome(tuple(fields))
                if isinstance(outcome, Exception):
                    raise outcome
                return SimpleNamespace(export_all_data=lambda: outcome)

            instance.api_get = Mock(side_effect=api_get)
            return instance

        def ad(fbid, api):
            self.assertEqual(fbid, "789")
            self.assertIs(api, self.api)

            def api_get(*, fields):
                self.calls.append(("ad", tuple(fields), None))
                if isinstance(self.ad_payload, Exception):
                    raise self.ad_payload
                if "account_id" in fields:
                    return self.ad_payload
                return {"id": "789", "creative": {"id": "456", "name": "private-expanded-name"}}

            return SimpleNamespace(api_get=Mock(side_effect=api_get))

        def new_batch():
            batch = SimpleNamespace(callbacks=[])

            def execute():
                if self.batch_mode == "missing":
                    return None
                if self.batch_mode == "retry":
                    return Mock()
                outcome = self.batch_outcome()
                for success, failure in batch.callbacks:
                    if isinstance(outcome, Exception):
                        failure(SimpleNamespace(error=lambda: outcome))
                    else:
                        success(SimpleNamespace(json=lambda: outcome))
                return None

            batch.execute = Mock(side_effect=execute)
            return batch

        self.api.new_batch.side_effect = new_batch
        self.enterContext(patch.object(diagnostic, "AdCreative", side_effect=creative))
        self.enterContext(patch.object(diagnostic, "Ad", side_effect=ad))
        self.page = self.enterContext(patch.object(diagnostic, "Page"))
        self.page.return_value.api_get.return_value = {"id": "321"}

    def run_probe(self):
        self.install_sdk()
        self.probe = diagnostic.CreativeProbe(self.account, self.report, self.output)
        self.probe.run()
        return {entry["name"]: entry for entry in self.report["probes"]}

    def saved_report(self):
        return json.loads((self.output / "report.json").read_text())

    def test_all_fields_success_and_each_direct_probe_uses_fresh_sdk_object(self):
        results = self.run_probe()
        self.assertEqual(self.report["status"], "complete")
        self.assertTrue(self.report["observations"]["ad_still_uses_creative"])
        self.assertEqual(self.report["observations"]["failing_default_fields"], [])
        self.assertEqual(results["full_direct"]["status"], "success")
        self.assertEqual(results["full_batch"]["status"], "success")
        self.assertIn("expanded_full", results)
        self.assertIn("expanded_story", results)
        direct_objects = [id(instance) for mode, _, instance in self.calls if mode == "direct"]
        self.assertEqual(len(direct_objects), len(set(direct_objects)))
        for field in diagnostic.DEFAULT_ADCREATIVE_FIELDS:
            self.assertEqual(results["field:" + field]["returned_fields"], sorted({"id", field}))

    def test_single_failing_field_isolated_and_reduced_fieldset_verified(self):
        def read(fields):
            return facebook_error() if "object_story_spec" in fields else self.payload(fields)

        self.direct_outcome = read
        self.batch_outcome = lambda: facebook_error()
        results = self.run_probe()
        self.assertEqual(self.report["status"], "complete")
        self.assertEqual(self.report["observations"]["failing_default_fields"], ["object_story_spec"])
        self.assertEqual(results["field:object_story_spec"]["error"]["subcode"], 2446211)
        self.assertEqual(results["without_failing_fields"]["status"], "success")
        self.assertNotIn("object_story_spec", results["without_failing_fields"]["returned_fields"])
        self.assertFalse(any(name.startswith("without:") for name in results))

    def test_batch_failure_is_distinct_from_successful_direct_reads(self):
        self.batch_outcome = lambda: facebook_error()
        results = self.run_probe()
        self.assertEqual(results["full_direct"]["status"], "success")
        self.assertEqual(results["full_batch"]["status"], "error")
        self.assertEqual(self.report["observations"]["failing_default_fields"], [])
        self.assertNotIn("without_failing_fields", results)
        self.assertEqual(self.report["status"], "complete")

    def test_full_failure_with_passing_single_fields_checks_each_omission(self):
        default_fields = set(diagnostic.DEFAULT_ADCREATIVE_FIELDS)
        self.direct_outcome = lambda fields: facebook_error() if set(fields) == default_fields else self.payload(fields)
        results = self.run_probe()
        self.assertEqual(results["full_direct"]["status"], "error")
        self.assertEqual(self.report["observations"]["failing_default_fields"], [])
        for field in diagnostic.DEFAULT_ADCREATIVE_FIELDS:
            result = results["without:" + field]
            self.assertEqual(result["status"], "success")
            self.assertNotIn(field, result["returned_fields"])

    def test_optional_field_errors_do_not_claim_default_field_failure(self):
        optional_fields = {"asset_feed_spec", "degrees_of_freedom_spec"}
        self.direct_outcome = lambda fields: facebook_error(subcode=999) if set(fields) & optional_fields else self.payload(fields)
        results = self.run_probe()
        self.assertEqual(self.report["status"], "complete")
        self.assertEqual(self.report["observations"]["failing_default_fields"], [])
        for field in optional_fields:
            self.assertEqual(results["optional:" + field]["status"], "error")

    def test_auth_and_quota_errors_stop_immediately_and_save_partial_report(self):
        for code, status in ((190, 400), (17, 400), (100, 429)):
            with self.subTest(code=code, http_status=status):
                # Reinitialize mocks/state because each subcase executes a complete run.
                self.report["probes"] = []
                self.calls = []
                self.direct_outcome = lambda fields, code=code, status=status: facebook_error(code=code, http_status=status)
                self.run_probe()
                self.assertEqual(self.report["status"], "incomplete_auth_or_quota")
                self.assertEqual([r["name"] for r in self.report["probes"]], ["ad_association", "basic"])
                self.assertEqual(self.saved_report()["status"], "incomplete_auth_or_quota")
                self.assertEqual(len([call for call in self.calls if call[0] == "direct"]), 1)

    def test_auth_error_during_association_makes_no_creative_requests(self):
        self.ad_payload = facebook_error(code=190)
        self.run_probe()
        self.assertEqual(self.report["status"], "incomplete_auth_or_quota")
        self.assertEqual([mode for mode, _, _ in self.calls], ["ad"])

    def test_account_mismatch_stops_before_creative_reads(self):
        self.ad_payload["account_id"] = "999"
        self.run_probe()
        self.assertEqual(self.report["status"], "incomplete")
        self.assertEqual([mode for mode, _, _ in self.calls], ["ad"])

    def test_wrong_returned_ad_id_stops_before_creative_reads(self):
        self.ad_payload["id"] = "999"
        self.run_probe()
        self.assertEqual(self.report["status"], "incomplete")
        self.assertEqual([mode for mode, _, _ in self.calls], ["ad"])

    def test_changed_creative_skips_ad_expansion(self):
        self.ad_payload["creative"]["id"] = "999"
        results = self.run_probe()
        self.assertEqual(self.report["status"], "complete")
        self.assertFalse(self.report["observations"]["ad_still_uses_creative"])
        self.assertNotIn("expanded_full", results)
        self.assertNotIn("expanded_story", results)
        self.assertEqual(len([mode for mode, _, _ in self.calls if mode == "ad"]), 1)

    def test_success_payloads_and_error_request_secrets_are_never_persisted(self):
        self.batch_outcome = lambda: facebook_error()
        self.run_probe()
        for filename in ("report.json", "summary.md"):
            text = (self.output / filename).read_text()
            self.assertNotIn("private-success", text)
            self.assertNotIn("private-expanded", text)
            self.assertNotIn("private-request-token", text)
            self.assertNotIn("private-error-message", text)
            self.assertNotIn("private exception message", text)
        self.assertEqual(self.saved_report(), self.report)

    def test_missing_batch_callback_or_sdk_retry_is_incomplete(self):
        for mode in ("missing", "retry"):
            with self.subTest(mode=mode):
                self.report["probes"] = []
                self.batch_mode = mode
                self.run_probe()
                self.assertEqual(self.report["status"], "incomplete")
                self.assertFalse(any(r["name"].startswith("field:") for r in self.report["probes"]))

    def test_malformed_success_response_is_incomplete(self):
        self.batch_outcome = lambda: ["unexpected payload"]
        self.run_probe()
        self.assertEqual(self.report["status"], "incomplete")
        self.assertEqual(self.saved_report()["status"], "incomplete")

    def test_story_asset_comparison_only_saves_booleans_and_page_access_names(self):
        def read(fields):
            result = self.payload(fields)
            if "object_story_spec" in fields:
                result["object_story_spec"] = {"page_id": "321", "link_data": {
                    "message": "private-story-text", "name": "private-title",
                }}
            if "asset_feed_spec" in fields:
                result["asset_feed_spec"] = {
                    "bodies": [{"text": "private-story-text"}],
                    "titles": [{"text": "private-other-title"}],
                }
            return result

        self.direct_outcome = read
        results = self.run_probe()
        self.assertEqual(self.report["observations"]["first_asset_matches_story"], {
            "bodies": True, "titles": False, "descriptions": None,
            "link_urls": None, "images": None, "videos": None,
        })
        self.assertEqual(results["story_page_access"]["status"], "success")
        self.page.assert_called_once_with("321", api=self.api)
        for filename in ("report.json", "summary.md"):
            self.assertNotIn("private-story-text", (self.output / filename).read_text())
            self.assertNotIn("private-title", (self.output / filename).read_text())

    def test_video_assets_use_video_field_names_and_cta_link(self):
        def read(fields):
            result = self.payload(fields)
            if "object_story_spec" in fields:
                result["object_story_spec"] = {"video_data": {
                    "title": "private-title", "link_description": "private-description",
                    "call_to_action": {"value": {"link": "private-link"}},
                }}
            if "asset_feed_spec" in fields:
                result["asset_feed_spec"] = {
                    "titles": [{"text": "private-title"}],
                    "descriptions": [{"text": "private-description"}],
                    "link_urls": [{"website_url": "private-link"}],
                }
            return result

        self.direct_outcome = read
        self.run_probe()
        comparisons = self.report["observations"]["first_asset_matches_story"]
        self.assertTrue(comparisons["titles"])
        self.assertTrue(comparisons["descriptions"])
        self.assertTrue(comparisons["link_urls"])
        self.assertIsNone(comparisons["bodies"])

    def test_story_page_falls_back_to_effective_post_id(self):
        def read(fields):
            result = self.payload(fields)
            if "object_story_spec" in fields:
                return facebook_error()
            if "effective_object_story_id" in fields:
                result["effective_object_story_id"] = "321_654"
            return result

        self.direct_outcome = read
        results = self.run_probe()
        self.assertEqual(results["story_page_access"]["status"], "success")
        self.page.assert_called_once_with("321", api=self.api)


class CreativeProbeCliTests(unittest.TestCase):
    def test_local_guard_runs_before_loading_config_or_creating_account(self):
        with (
            patch("sys.argv", ["probe_creative", "--group", "d2a"]),
            patch.object(diagnostic, "enforce_local_facebook_group", side_effect=SystemExit("local prohibited")) as guard,
            patch.object(diagnostic, "select_accounts") as select,
            patch.object(diagnostic, "get_ads_account") as account,
        ):
            with self.assertRaisesRegex(SystemExit, "local prohibited"):
                diagnostic.main()
        guard.assert_called_once_with("d2a")
        select.assert_not_called()
        account.assert_not_called()


if __name__ == "__main__":
    unittest.main()
