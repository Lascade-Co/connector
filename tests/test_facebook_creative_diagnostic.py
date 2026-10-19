import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from facebook_business.exceptions import FacebookRequestError

from pipelines.facebook import diagnose_creatives as diagnostic


def facebook_error(code=100, subcode=2446211, trace_id="trace-123"):
    return FacebookRequestError(
        "request failed: do not publish this message",
        {"access_token": "private-request-token"},
        400,
        {},
        {"error": {
            "message": "private-error-message",
            "code": code,
            "error_subcode": subcode,
            "fbtrace_id": trace_id,
        }},
    )


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.api = Mock()
        self.account = SimpleNamespace(
            get_api=Mock(return_value=self.api),
            get_ads=Mock(name="get_ads"),
            get_ad_creatives=Mock(name="get_ad_creatives"),
        )
        self.registered = []
        self.batches = []
        self.outcomes = {}
        self.save = Mock()

    def install_batches(self):
        def new_batch():
            callbacks = []
            batch = SimpleNamespace(callbacks=callbacks)

            def execute():
                for creative_id, success, failure in reversed(callbacks):
                    error = self.outcomes.get(creative_id)
                    if error is None:
                        success({"access_token": "private-success-token",
                                 "object_story_spec": {"message": "private-story"}})
                    else:
                        failure(SimpleNamespace(error=lambda error=error: error))
                return None

            batch.execute = Mock(side_effect=execute)
            self.batches.append(batch)
            return batch

        def creative(*, fbid, api):
            self.assertIs(api, self.api)

            def api_get(*, fields, batch, success, failure):
                self.assertEqual(fields, diagnostic.DEFAULT_ADCREATIVE_FIELDS)
                self.registered.append(fbid)
                batch.callbacks.append((fbid, success, failure))

            return SimpleNamespace(api_get=api_get)

        self.api.new_batch.side_effect = new_batch
        return patch.object(diagnostic, "AdCreative", side_effect=creative)

    def scan(self, ad_pages, creative_pages, *, cap=5000, stop_after_first=False):
        report = {"account_id": "912296209831774", "group": "d2a"}

        def chunks(method, fields, states, size, params=None):
            self.assertIsNone(states)
            self.assertEqual(size, 100)
            if method is self.account.get_ads:
                self.assertEqual(fields, diagnostic.AD_FIELDS)
                pages = ad_pages
            else:
                self.assertIs(method, self.account.get_ad_creatives)
                self.assertEqual(fields, diagnostic.LIGHT_FIELDS)
                self.assertEqual(params, {"summary": "false"})
                pages = creative_pages
            for page in pages:
                if isinstance(page, Exception):
                    raise page
                yield page

        with (
            self.install_batches(),
            patch.object(diagnostic, "get_data_chunked", side_effect=chunks),
        ):
            diagnostic.diagnose_account(
                self.account, report, max_creatives=cap,
                stop_after_first=stop_after_first, save=self.save,
            )
        return report

    def test_error_details_reads_trace_from_real_sdk_exception_body(self):
        self.assertEqual(diagnostic.error_details(facebook_error()), {
            "code": 100, "subcode": 2446211, "trace_id": "trace-123",
            "http_status": 400,
        })

    def test_http_quota_error_stops_even_without_meta_error_code(self):
        error = FacebookRequestError("rate limited", {}, 429, {}, {})
        details = diagnostic.error_details(error)
        self.assertEqual(details["http_status"], 429)
        self.assertTrue(diagnostic.must_stop(details))

    def test_failure_callbacks_keep_identity_even_when_responses_are_reversed(self):
        self.outcomes = {
            "101": facebook_error(trace_id="trace-first"),
            "102": facebook_error(subcode=999, trace_id="trace-second"),
        }
        report = self.scan(
            [[{"id": "201", "name": "First ad", "creative": {"id": "101"}}],
             [{"id": "202", "name": "Second ad", "creative": {"id": "101"}},
              {"id": "203", "name": "Other ad", "creative": {"id": "102"}}]],
            [[{"id": "101", "name": "First creative"}],
             [{"id": "102", "name": "Second creative"}, {"id": "103"}]],
        )
        failures = {item["creative_id"]: item for item in report["failures"]}
        self.assertEqual(report["checked"], 3)
        self.assertEqual(report["status"], "complete")
        self.assertTrue(report["mapping_complete"])
        self.assertTrue(report["creative_listing_complete"])
        self.assertEqual(failures["101"]["creative_name"], "First creative")
        self.assertEqual(failures["101"]["error"]["trace_id"], "trace-first")
        self.assertEqual([ad["id"] for ad in failures["101"]["ads"]], ["201", "202"])
        self.assertTrue(failures["101"]["target_error"])
        self.assertEqual(failures["102"]["error"]["trace_id"], "trace-second")
        self.assertEqual(failures["102"]["ads"][0]["id"], "203")
        self.assertFalse(failures["102"]["target_error"])

    def test_callbacks_from_same_batch_do_not_capture_last_creative(self):
        self.outcomes = {"101": facebook_error(trace_id="one"),
                         "102": facebook_error(trace_id="two")}
        report = self.scan([], [[{"id": "101"}, {"id": "102"}]])
        self.assertEqual(
            {f["creative_id"]: f["error"]["trace_id"] for f in report["failures"]},
            {"101": "one", "102": "two"},
        )

    def test_stop_after_target_finishes_current_batch_and_skips_following_batch(self):
        self.outcomes = {"1": facebook_error()}
        report = self.scan([], [[{"id": str(i)} for i in range(1, 62)]],
                           stop_after_first=True)
        self.assertEqual(self.registered, [str(i) for i in range(1, 51)])
        self.assertEqual(report["checked"], 50)
        self.assertEqual(report["status"], "target_found_stopped_early")
        self.assertFalse(report["creative_listing_complete"])
        self.assertEqual(len(self.batches), 1)

    def test_other_errors_do_not_trigger_target_stop(self):
        self.outcomes = {"101": facebook_error(subcode=999)}
        report = self.scan([], [[{"id": "101"}], [{"id": "102"}]],
                           stop_after_first=True)
        self.assertEqual(self.registered, ["101", "102"])
        self.assertEqual(report["status"], "complete")

    def test_duplicate_creatives_across_pages_are_checked_once(self):
        report = self.scan([], [[{"id": "101"}], [{"id": "101"}, {"id": "102"}]])
        self.assertEqual(self.registered, ["101", "102"])
        self.assertEqual(report["checked"], 2)

    def test_missing_batch_callbacks_cannot_be_reported_as_success(self):
        batch = Mock()
        batch.execute.return_value = None
        self.api.new_batch.return_value = batch
        report = {"account_id": "123", "checked": 0, "failures": [], "ad_mapping": {}}
        with patch.object(diagnostic, "AdCreative"), self.assertRaisesRegex(
            RuntimeError, "incomplete_batch_response"
        ):
            diagnostic.check_batch(self.account, [{"id": "101"}], report)
        self.assertEqual(report["checked"], 0)

    def test_scan_cap_is_incomplete_and_does_not_submit_extra_creatives(self):
        report = self.scan([], [[{"id": str(i)} for i in range(5)]], cap=2)
        self.assertEqual(self.registered, ["0", "1"])
        self.assertEqual(report["checked"], 2)
        self.assertEqual(report["status"], "incomplete")
        self.assertFalse(report["creative_listing_complete"])
        self.assertIn({"stage": "scan", "reason": "max_creatives_reached"}, report["errors"])

    def test_auth_or_quota_batch_failure_preserves_target_and_ad_mapping(self):
        for code in (190, 17):
            with self.subTest(code=code):
                self.outcomes = {"101": facebook_error(), "102": facebook_error(code=code)}
                report = self.scan(
                    [[{"id": "201", "name": "Find me", "creative": {"id": "101"}}]],
                    [[{"id": "101"}, {"id": "102"}], [{"id": "103"}]],
                )
                self.assertEqual(report["status"], "incomplete")
                self.assertEqual(report["checked"], 2)
                target = next(f for f in report["failures"] if f["target_error"])
                self.assertEqual(target["ads"][0]["id"], "201")
                self.assertTrue(report["mapping_complete"])
                self.assertIn({"stage": "scan", "reason": "auth_or_quota_error"}, report["errors"])

    def test_interrupted_mapping_keeps_pages_and_does_not_scan_on_auth_error(self):
        report = self.scan(
            [[{"id": "201", "creative": {"id": "101"}}], facebook_error(code=190)],
            [[{"id": "101"}]],
        )
        self.assertFalse(report["mapping_complete"])
        self.assertEqual(report["ad_mapping"]["101"][0]["id"], "201")
        self.assertEqual(report["errors"][0]["stage"], "ad_mapping")
        self.assertEqual(self.registered, [])

    def test_listing_failure_preserves_already_identified_creative_and_ads(self):
        self.outcomes = {"101": facebook_error()}
        report = self.scan(
            [[{"id": "201", "creative": {"id": "101"}}]],
            [[{"id": "101"}], facebook_error(code=17)],
        )
        self.assertEqual(report["failures"][0]["creative_id"], "101")
        self.assertEqual(report["failures"][0]["ads"][0]["id"], "201")
        self.assertEqual(report["errors"][-1]["stage"], "creative_scan")
        self.assertEqual(report["status"], "incomplete")
        self.assertFalse(report["creative_listing_complete"])

    def test_nonquota_mapping_failure_allows_creative_evidence_but_remains_incomplete(self):
        self.outcomes = {"101": facebook_error()}
        report = self.scan(
            [[{"id": "201", "creative": {"id": "101"}}], facebook_error(subcode=999)],
            [[{"id": "101"}]],
        )
        self.assertFalse(report["mapping_complete"])
        self.assertTrue(report["creative_listing_complete"])
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["failures"][0]["ads"][0]["id"], "201")

    def test_target_early_stop_does_not_mask_partial_ad_mapping(self):
        self.outcomes = {"101": facebook_error()}
        report = self.scan(
            [[{"id": "201", "name": "Linked ad", "creative": {"id": "101"}}],
             facebook_error(code=100, subcode=999)],
            [[{"id": "101"}], [{"id": "102"}]],
            stop_after_first=True,
        )
        self.assertEqual(self.registered, ["101"])
        self.assertEqual(report["status"], "incomplete")
        self.assertFalse(report["mapping_complete"])
        self.assertFalse(report["creative_listing_complete"])
        self.assertEqual(report["failures"][0]["creative_id"], "101")
        self.assertTrue(report["failures"][0]["target_error"])
        self.assertEqual(report["failures"][0]["ads"][0]["id"], "201")
        self.assertEqual(report["errors"][0]["stage"], "ad_mapping")
        self.assertEqual(report["errors"][0]["subcode"], 999)

    def test_partial_reports_omit_mapping_success_payloads_and_credentials(self):
        self.outcomes = {"101": facebook_error()}
        report = self.scan(
            [[{"id": "201", "name": "Linked ad", "creative": {"id": "101"}},
              {"id": "202", "name": "Unrelated private ad", "creative": {"id": "102"}}]],
            [[{"id": "101"}, {"id": "102"}], facebook_error(code=17)],
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            diagnostic.write_reports(output, [report])
            raw = (output / "report.json").read_text(encoding="utf-8")
            summary = (output / "summary.md").read_text(encoding="utf-8")
        public = json.loads(raw)[0]
        self.assertNotIn("ad_mapping", public)
        self.assertEqual(public["failures"][0]["ads"][0]["id"], "201")
        self.assertEqual(public["status"], "incomplete")
        for private in ("private-request-token", "private-success-token", "private-story",
                        "private-error-message", "Unrelated private ad", "access_token"):
            self.assertNotIn(private, raw + summary)

    def test_summary_escapes_names_and_marks_missing_ad_mapping(self):
        self.outcomes = {"101": facebook_error()}
        report = self.scan([], [[{"id": "101", "name": "<script>|line\nnext"}]])
        summary = diagnostic.render_summary([report])
        self.assertIn("&lt;script&gt;&#124;line next", summary)
        self.assertNotIn("<script>", summary)
        self.assertIn("No accessible linked ad found", summary)

    def test_summary_links_every_mapped_ad_and_distinguishes_other_errors(self):
        self.outcomes = {"101": facebook_error(subcode=999)}
        report = self.scan(
            [[{"id": "201", "name": "one|two", "creative": {"id": "101"}},
              {"id": "202", "creative": {"id": "101"}}]],
            [[{"id": "101"}]],
        )
        summary = diagnostic.render_summary([report])
        self.assertIn("selected_ad_ids=201", summary)
        self.assertIn("selected_ad_ids=202", summary)
        self.assertIn("one&#124;two", summary)
        self.assertIn("No (other error)", summary)
        self.assertIn("Target error not reproduced", summary)

    def test_summary_treats_markdown_names_as_text(self):
        self.outcomes = {"101": facebook_error()}
        report = self.scan([], [[{"id": "101", "name": "[click](https://example.com) *bold* `code`"}]])
        summary = diagnostic.render_summary([report])
        self.assertNotIn("[click]", summary)
        self.assertNotIn("*bold*", summary)
        self.assertNotIn("`code`", summary)


class DiagnosticCliTests(unittest.TestCase):
    def test_select_accounts_normalizes_prefixes_and_deduplicates(self):
        group = {"token": "private"}
        with patch.object(diagnostic, "get_for_group", return_value=(group, ["act_123", "123", "456"])):
            self.assertEqual(diagnostic.select_accounts("d2a", ""), (group, ["123", "456"]))
            self.assertEqual(diagnostic.select_accounts("d2a", " act_123 "), (group, ["123"]))
            with self.assertRaisesRegex(SystemExit, "does not belong"):
                diagnostic.select_accounts("d2a", "12")

    def test_only_d2_family_groups_are_allowed_before_config_is_read(self):
        with patch.object(diagnostic, "get_for_group") as get_group:
            for group in ("d1c", "D2", "d2ab", "subscription", ""):
                with self.subTest(group=group), self.assertRaises(SystemExit):
                    diagnostic.select_accounts(group, "")
            get_group.assert_not_called()

    def test_invalid_or_empty_account_configuration_is_rejected(self):
        for accounts in ([], ["abc"], ["１２３"], ["123", "bad"]):
            with self.subTest(accounts=accounts), patch.object(
                diagnostic, "get_for_group", return_value=({}, accounts)
            ), self.assertRaises(SystemExit):
                diagnostic.select_accounts("d2", "")

    def test_local_cli_is_blocked_before_credentials_or_reports_are_accessed(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("sys.argv", ["diagnose_creatives", "--group", "d2a"]),
            patch.object(diagnostic, "select_accounts") as select,
            patch.object(diagnostic, "get_ads_account") as account,
            patch.object(diagnostic, "write_reports") as save,
            self.assertRaises(SystemExit),
        ):
            diagnostic.main()
        select.assert_not_called()
        account.assert_not_called()
        save.assert_not_called()

    def test_unexpected_terminal_fault_retains_partial_evidence_and_sanitizes_error(self):
        reports_written = []

        def save(_directory, reports):
            reports_written.append(json.loads(json.dumps(reports)))

        def crash(_account, report, **kwargs):
            report.update(checked=1, failures=[{
                "creative_id": "101", "creative_name": "Broken", "target_error": True,
                "error": {"code": 100, "subcode": 2446211, "trace_id": "trace"},
                "ads": [{"id": "201", "name": "Keep this evidence"}],
            }])
            raise RuntimeError("private-terminal-token")

        with (
            patch("sys.argv", ["diagnose_creatives"]),
            patch.object(diagnostic, "enforce_local_facebook_group"),
            patch.object(diagnostic, "select_accounts", return_value=({"token": "private"}, ["123"])),
            patch.object(diagnostic, "get_ads_account", return_value=object()),
            patch.object(diagnostic, "diagnose_account", side_effect=crash),
            patch.object(diagnostic, "write_reports", side_effect=save),
            patch("builtins.print"),
        ):
            result = diagnostic.main()
        final = reports_written[-1][0]
        self.assertEqual(result, 1)
        self.assertEqual(final["status"], "incomplete")
        self.assertEqual(final["failures"][0]["ads"][0]["id"], "201")
        self.assertEqual(final["errors"], [{"stage": "diagnostic", "type": "RuntimeError"}])
        self.assertNotIn("private-terminal-token", json.dumps(reports_written))


if __name__ == "__main__":
    unittest.main()
