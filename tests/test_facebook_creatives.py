import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

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
