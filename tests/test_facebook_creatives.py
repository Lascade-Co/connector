import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from facebook_business.exceptions import FacebookRequestError

from facebook_ads import creatives


class CreativeRefreshTests(unittest.TestCase):
    def _run(self, state, account_id="act-1", mode="auto", account=None):
        account = account or SimpleNamespace(get_ad_creatives=object())
        with (
            patch.object(creatives.dlt.current, "resource_state", return_value=state),
            patch.dict(
                creatives.os.environ,
                {"FB_CREATIVE_REFRESH_MODE": mode},
                clear=True,
            ),
        ):
            return list(
                creatives.iter_creatives(
                    account=account,
                    account_id=account_id,
                    fields=("id", "name", "status"),
                    states=None,
                )
            )

    def test_creative_state_is_namespaced_by_account(self):
        state = {}
        rows_by_account = {
            "act-1": [[{"id": "shared", "name": "One", "status": "ACTIVE"}]],
            "act-2": [[{"id": "shared", "name": "Two", "status": "PAUSED"}]],
        }

        def full_scan(_account, _fields, _states):
            account_id = current_account[0]
            yield from rows_by_account[account_id]

        current_account = ["act-1"]
        with patch.object(creatives, "_iter_full_creatives", side_effect=full_scan):
            self._run(state, account_id="act-1")
            current_account[0] = "act-2"
            self._run(state, account_id="act-2")

        accounts = state["accounts"]
        self.assertEqual(set(accounts), {"act-1", "act-2"})
        self.assertNotEqual(
            accounts["act-1"]["fingerprints"]["shared"],
            accounts["act-2"]["fingerprints"]["shared"],
        )

    def test_empty_state_forces_a_full_reconciliation_even_in_incremental_mode(self):
        state = {}
        full_rows = [[{"id": "creative-1", "name": "First", "status": "ACTIVE"}]]
        with (
            patch.object(creatives, "_iter_full_creatives", return_value=iter(full_rows)) as full,
            patch.object(creatives, "get_data_chunked") as light_scan,
        ):
            rows = self._run(state, mode="incremental")

        self.assertEqual(rows, full_rows)
        full.assert_called_once()
        light_scan.assert_not_called()
        self.assertIn("creative-1", state["accounts"]["act-1"]["fingerprints"])

    def test_invalid_refresh_mode_fails_instead_of_guessing(self):
        with patch.dict(
            creatives.os.environ,
            {"FB_CREATIVE_REFRESH_MODE": "typo"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "auto, incremental, or full"):
                creatives.get_creative_refresh_mode()

    def test_full_reconciliation_starts_at_250_and_adaptively_halves(self):
        sizes = []

        def chunks(_method, _fields, _states, chunk_size, _params):
            sizes.append(chunk_size)
            if chunk_size == 250:
                raise FacebookRequestError(
                    "too much data",
                    {},
                    500,
                    {},
                    {
                        "error": {
                            "code": 1,
                            "message": "Please reduce the amount of data you're asking for",
                        }
                    },
                )
            yield [{"id": "creative-1"}]

        account = SimpleNamespace(get_ad_creatives=object())
        with (
            patch.dict(creatives.os.environ, {}, clear=True),
            patch.object(creatives, "get_data_chunked", side_effect=chunks),
        ):
            rows = list(creatives._iter_full_creatives(account, ("id",), None))

        self.assertEqual(sizes, [250, 125])
        self.assertEqual(rows, [[{"id": "creative-1"}]])

    def test_adaptive_restart_does_not_replay_already_yielded_creatives(self):
        attempts = 0

        def chunks(_method, _fields, _states, chunk_size, _params):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield [{"id": "creative-1"}]
                raise FacebookRequestError(
                    "too much data",
                    {},
                    500,
                    {},
                    {
                        "error": {
                            "code": 1,
                            "message": "Please reduce the amount of data you're asking for",
                        }
                    },
                )
            self.assertEqual(chunk_size, 125)
            yield [{"id": "creative-1"}, {"id": "creative-2"}]

        account = SimpleNamespace(get_ad_creatives=object())
        with (
            patch.dict(creatives.os.environ, {}, clear=True),
            patch.object(creatives, "get_data_chunked", side_effect=chunks),
        ):
            rows = list(creatives._iter_full_creatives(account, ("id",), None))

        self.assertEqual(rows, [[{"id": "creative-1"}], [{"id": "creative-2"}]])

    def test_full_reconciliation_propagates_quota_errors_unchanged(self):
        error = FacebookRequestError(
            "rate limited",
            {},
            429,
            {},
            {"error": {"code": 17, "message": "User request limit reached"}},
        )
        account = SimpleNamespace(get_ad_creatives=object())
        with patch.object(creatives, "get_data_chunked", side_effect=error):
            with self.assertRaises(FacebookRequestError) as raised:
                list(creatives._iter_full_creatives(account, ("id",), None))

        self.assertIs(raised.exception, error)

    def test_partial_initial_scan_forces_full_reconciliation_on_next_run(self):
        first = {"id": "creative-1", "name": "First", "status": "ACTIVE"}
        missing = {"id": "creative-2", "name": "Second", "status": "ACTIVE"}
        state = {}
        quota_error = FacebookRequestError(
            "rate limited",
            {},
            429,
            {},
            {"error": {"code": 17, "message": "User request limit reached"}},
        )

        def partial_full_scan(_account, _fields, _states):
            yield [first]
            raise quota_error

        with patch.object(
            creatives, "_iter_full_creatives", side_effect=partial_full_scan
        ):
            with self.assertRaises(FacebookRequestError) as raised:
                self._run(state)

        self.assertIs(raised.exception, quota_error)
        fingerprints = state["accounts"]["act-1"]["fingerprints"]
        self.assertEqual(
            fingerprints,
            {"creative-1": creatives._creative_fingerprint(first)},
        )
        self.assertTrue(
            state["accounts"]["act-1"]["full_reconciliation_incomplete"]
        )

        with (
            patch.object(
                creatives,
                "_iter_full_creatives",
                return_value=iter([[first, missing]]),
            ) as full_scan,
            patch.object(creatives, "get_data_chunked") as light_scan,
        ):
            rows = self._run(state)

        full_scan.assert_called_once()
        light_scan.assert_not_called()
        self.assertEqual([item["id"] for item in rows[0]], ["creative-1", "creative-2"])
        self.assertEqual(
            set(state["accounts"]["act-1"]["fingerprints"]),
            {"creative-1", "creative-2"},
        )
        self.assertFalse(
            state["accounts"]["act-1"]["full_reconciliation_incomplete"]
        )

    def test_partial_manual_full_scan_forces_next_automatic_run_to_full_mode(self):
        old = {"id": "creative-1", "name": "Old", "status": "ACTIVE"}
        state = {
            "accounts": {
                "act-1": {
                    "fingerprints": {
                        "creative-1": creatives._creative_fingerprint(old)
                    }
                }
            }
        }
        quota_error = FacebookRequestError(
            "rate limited",
            {},
            429,
            {},
            {"error": {"code": 17, "message": "User request limit reached"}},
        )

        def partial_full_scan(_account, _fields, _states):
            yield [old]
            raise quota_error

        with patch.object(
            creatives, "_iter_full_creatives", side_effect=partial_full_scan
        ):
            with self.assertRaises(FacebookRequestError):
                self._run(state, mode="full")

        self.assertTrue(
            state["accounts"]["act-1"]["full_reconciliation_incomplete"]
        )

        with (
            patch.object(
                creatives, "_iter_full_creatives", return_value=iter([[old]])
            ) as full_scan,
            patch.object(creatives, "get_data_chunked") as light_scan,
        ):
            self._run(state, mode="auto")

        full_scan.assert_called_once()
        light_scan.assert_not_called()
        self.assertFalse(
            state["accounts"]["act-1"]["full_reconciliation_incomplete"]
        )

    def test_explicit_full_mode_reconciles_even_when_state_exists(self):
        old = {"id": "creative-1", "name": "Old", "status": "ACTIVE"}
        state = {
            "accounts": {
                "act-1": {"fingerprints": {"creative-1": creatives._creative_fingerprint(old)}}
            }
        }
        refreshed = {"id": "creative-1", "name": "New", "status": "PAUSED"}
        with patch.object(
            creatives, "_iter_full_creatives", return_value=iter([[refreshed]])
        ) as full:
            rows = self._run(state, mode="full")

        self.assertEqual(rows, [[refreshed]])
        full.assert_called_once()
        self.assertEqual(
            state["accounts"]["act-1"]["fingerprints"]["creative-1"],
            creatives._creative_fingerprint(refreshed),
        )

    def test_incremental_mode_hydrates_only_new_or_fingerprint_changed_creatives(self):
        stable = {"id": "stable", "name": "Stable", "status": "ACTIVE"}
        old_changed = {"id": "changed", "name": "Old", "status": "ACTIVE"}
        new_changed = {"id": "changed", "name": "New", "status": "PAUSED"}
        new_item = {"id": "new", "name": "New item", "status": "ACTIVE"}
        state = {
            "accounts": {
                "act-1": {
                    "fingerprints": {
                        "stable": creatives._creative_fingerprint(stable),
                        "changed": creatives._creative_fingerprint(old_changed),
                    }
                }
            }
        }

        def hydrate(_account, light_items, _fields):
            return [{**item, "object_story_spec": {"hydrated": True}} for item in light_items]

        with (
            patch.object(
                creatives,
                "get_data_chunked",
                return_value=iter([[stable, new_changed, new_item]]),
            ),
            patch.object(creatives, "_hydrate_creative_batch", side_effect=hydrate) as hydrate_batch,
            patch.object(creatives, "_iter_full_creatives") as full,
        ):
            rows = self._run(state, mode="incremental")

        full.assert_not_called()
        hydrated_light_items = hydrate_batch.call_args.args[1]
        self.assertEqual([item["id"] for item in hydrated_light_items], ["changed", "new"])
        self.assertEqual([item["id"] for item in rows[0]], ["changed", "new"])
        fingerprints = state["accounts"]["act-1"]["fingerprints"]
        self.assertEqual(fingerprints["stable"], creatives._creative_fingerprint(stable))
        self.assertEqual(fingerprints["changed"], creatives._creative_fingerprint(new_changed))
        self.assertEqual(fingerprints["new"], creatives._creative_fingerprint(new_item))

    def test_failed_hydration_does_not_mark_changed_creatives_complete(self):
        old = {"id": "changed", "name": "Old", "status": "ACTIVE"}
        changed = {"id": "changed", "name": "New", "status": "PAUSED"}
        new_item = {"id": "new", "name": "New item", "status": "ACTIVE"}
        old_fingerprint = creatives._creative_fingerprint(old)
        state = {
            "accounts": {
                "act-1": {"fingerprints": {"changed": old_fingerprint}}
            }
        }
        with (
            patch.object(
                creatives,
                "get_data_chunked",
                return_value=iter([[changed, new_item]]),
            ),
            patch.object(
                creatives,
                "_hydrate_creative_batch",
                side_effect=RuntimeError("batch failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "batch failed"):
                self._run(state, mode="incremental")

        fingerprints = state["accounts"]["act-1"]["fingerprints"]
        self.assertEqual(fingerprints["changed"], old_fingerprint)
        self.assertNotIn("new", fingerprints)

    def test_incomplete_sdk_batch_response_does_not_update_fingerprints(self):
        old = {"id": "changed", "name": "Old", "status": "ACTIVE"}
        changed = {"id": "changed", "name": "New", "status": "PAUSED"}
        new_item = {"id": "new", "name": "New item", "status": "ACTIVE"}
        old_fingerprint = creatives._creative_fingerprint(old)
        state = {
            "accounts": {
                "act-1": {"fingerprints": {"changed": old_fingerprint}}
            }
        }

        batch = Mock()
        batch.execute.return_value = None
        api = SimpleNamespace(new_batch=Mock(return_value=batch))
        account = SimpleNamespace(
            get_ad_creatives=object(),
            get_api=Mock(return_value=api),
        )

        def ad_creative(fbid, api):
            def api_get(*, fields, batch, success, failure):
                if fbid == "changed":
                    success(SimpleNamespace(json=lambda: {"id": fbid, "name": "New"}))
                # Meta returning no callback for `new` simulates an incomplete
                # SDK batch response without reporting an explicit failure.

            return SimpleNamespace(api_get=api_get)

        with (
            patch.object(
                creatives,
                "get_data_chunked",
                return_value=iter([[changed, new_item]]),
            ),
            patch.object(creatives, "AdCreative", side_effect=ad_creative),
        ):
            with self.assertRaisesRegex(RuntimeError, "incomplete creative batch"):
                self._run(state, mode="incremental", account=account)

        batch.execute.assert_called_once_with()
        fingerprints = state["accounts"]["act-1"]["fingerprints"]
        self.assertEqual(fingerprints["changed"], old_fingerprint)
        self.assertNotIn("new", fingerprints)


if __name__ == "__main__":
    unittest.main()
