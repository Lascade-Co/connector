"""Read-only field isolation for one creative, executed only in GitHub Actions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.page import Page
from facebook_business.exceptions import FacebookRequestError

from facebook_ads.helpers import get_ads_account
from facebook_ads.settings import DEFAULT_ADCREATIVE_FIELDS
from pipelines.facebook.diagnose_creatives import cell, error_details, must_stop, select_accounts
from utils import enforce_local_facebook_group


PAGE_TASK_NAMES = frozenset(
    value for name, value in vars(Page.Tasks).items()
    if not name.startswith("_") and isinstance(value, str)
)


class StopProbing(Exception):
    """Authentication or quota failures require stopping further requests."""


class CreativeProbe:
    def __init__(self, account, report: dict, output_dir: Path):
        self.account = account
        self.report = report
        self.output_dir = output_dir
        self.values = {}  # In memory only; never serialize successful payloads.
        self.creative = AdCreative(report["creative_id"], api=account.get_api())

    def save(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "report.json").write_text(json.dumps(self.report, indent=2), encoding="utf-8")
        lines = [
            "# Focused Facebook creative diagnostic", "",
            f"Creative: {self.report['creative_id']}; ad: {self.report['ad_id']}; "
            f"account: {self.report['account_id']}.", "",
            f"Status: {cell(self.report['status'])}", "",
            "Successful values are not stored; results show returned field names and error metadata.", "",
            "| Probe | Result | Error code / subcode | Returned fields |",
            "| --- | --- | --- | --- |",
        ]
        for result in self.report["probes"]:
            error = result.get("error", {})
            lines.append("| " + " | ".join([
                cell(result["name"]), cell(result["status"]),
                cell(f"{error.get('code', '')} / {error.get('subcode', '')}"),
                cell(", ".join(result.get("returned_fields", []))),
            ]) + " |")
        lines += ["", "```json", json.dumps(self.report.get("observations", {}), indent=2), "```", ""]
        (self.output_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    def probe(self, name, read, fields=None, stop_on_access=True):
        result = {"name": name}
        if fields is not None:
            result["requested_fields"] = list(fields)
        self.report["probes"].append(result)
        try:
            payload = read()
            if hasattr(payload, "export_all_data"):
                payload = payload.export_all_data()
            if not isinstance(payload, dict):
                raise RuntimeError("unexpected_response_shape")
            self.values[name] = payload
            result.update(status="success", returned_fields=sorted(payload))
            return payload
        except FacebookRequestError as exc:
            error = error_details(exc)
            message = (exc.api_error_message() or "").lower()
            if "unsupported get request" in message or "cannot be loaded due to missing permissions" in message:
                error["category"] = "object_missing_or_inaccessible"
            elif "permission" in message:
                error["category"] = "permission_related"
            result.update(status="error", error=error)
            if must_stop(error) and (
                error.get("http_status") == 429 or stop_on_access or error.get("code") not in {10, 200}
            ):
                raise StopProbing from None
        except Exception as exc:
            result.update(status="incomplete", error_type=type(exc).__name__)
            raise
        finally:
            self.save()
            print(f"{name}: {result.get('status', 'incomplete')}", flush=True)
        return None

    def batch_read(self):
        batch = self.account.get_api().new_batch()
        results = []
        errors = []
        self.creative.api_get(
            fields=DEFAULT_ADCREATIVE_FIELDS, batch=batch,
            success=lambda response: results.append(response.json()),
            failure=lambda response: errors.append(response.error()),
        )
        retry = batch.execute()
        if errors:
            raise errors[0]
        if retry is not None or len(results) != 1:
            raise RuntimeError("incomplete_batch_response")
        return results[0]

    def direct(self, name, fields, *, stop_on_access=True):
        # SDK api_get merges into its object; use a fresh object for each probe.
        return self.probe(name, lambda: AdCreative(
            self.report["creative_id"], api=self.account.get_api()
        ).api_get(fields=fields), fields=fields, stop_on_access=stop_on_access)

    def read_ad(self, fields):
        return Ad(self.report["ad_id"], api=self.account.get_api()).api_get(fields=fields)

    def run(self):
        self.save()
        try:
            association = self.probe("ad_association", lambda: self.read_ad(["id", "account_id", "creative"]))
            matches = bool(association and str((association.get("creative") or {}).get("id")) == self.report["creative_id"])
            self.report["observations"]["ad_still_uses_creative"] = matches
            if association and str(association.get("account_id")) != self.report["account_id"]:
                raise RuntimeError("ad_account_mismatch")
            if association and str(association.get("id")) != self.report["ad_id"]:
                raise RuntimeError("ad_identity_mismatch")
            self.direct("basic", ["id", "name"])
            full = self.direct("full_direct", DEFAULT_ADCREATIVE_FIELDS)
            self.probe("full_batch", self.batch_read, fields=DEFAULT_ADCREATIVE_FIELDS)
            for field in DEFAULT_ADCREATIVE_FIELDS:
                self.direct(f"field:{field}", list(dict.fromkeys(("id", field))))
            failed = [
                field for field in DEFAULT_ADCREATIVE_FIELDS
                if f"field:{field}" not in self.values
            ]
            self.report["observations"]["failing_default_fields"] = failed
            if full is None:
                if failed:
                    self.direct("without_failing_fields", [f for f in DEFAULT_ADCREATIVE_FIELDS if f not in failed])
                else:
                    for field in DEFAULT_ADCREATIVE_FIELDS:
                        self.direct(f"without:{field}", [f for f in DEFAULT_ADCREATIVE_FIELDS if f != field])
            for field in ("asset_feed_spec", "degrees_of_freedom_spec"):
                self.direct(f"optional:{field}", ["id", field], stop_on_access=False)
            if full is None:
                for field in ("asset_feed_spec", "degrees_of_freedom_spec"):
                    self.direct(f"full_with:{field}", (*DEFAULT_ADCREATIVE_FIELDS, field), stop_on_access=False)
                self.direct("story_page_only", ["id", "object_story_spec{page_id}"], stop_on_access=False)
                self.direct("story_explicit_fields", [
                    "id", "object_story_spec{page_id,instagram_user_id,link_data,video_data,photo_data,template_data,text_data}",
                ], stop_on_access=False)
            if matches:
                for label, fields in (
                    ("expanded_full", DEFAULT_ADCREATIVE_FIELDS),
                    ("expanded_story", ("id", "object_story_spec")),
                ):
                    self.probe(label, lambda fields=fields: self.read_ad(["id", "creative{" + ",".join(fields) + "}"]))
            self.inspect_story_access()
            self.report["status"] = "complete"
        except StopProbing:
            self.report["status"] = "incomplete_auth_or_quota"
        except Exception as exc:
            self.report["status"] = "incomplete"
            self.report["error_type"] = type(exc).__name__
        finally:
            self.save()

    def inspect_story_access(self):
        story = (self.values.get("field:object_story_spec") or {}).get("object_story_spec") or {}
        page_id = str(story.get("page_id", ""))
        if not page_id:
            post_id = str((self.values.get("field:effective_object_story_id") or {}).get("effective_object_story_id", ""))
            if "_" in post_id:
                page_id = post_id.partition("_")[0]
        if page_id.isascii() and page_id.isdigit():
            self.report["observations"]["target_page_id"] = page_id
            self.probe("story_page_access", lambda: Page(page_id, api=self.account.get_api()).api_get(fields=["id"]), stop_on_access=False)
            assigned = self.probe("assigned_page_access", lambda: self.account.get_api().call(
                "GET", ("me", "assigned_pages"), params={"pages": json.dumps([page_id]), "fields": "id,tasks", "limit": 100}
            ).json(), stop_on_access=False)
            self.report["observations"]["page_assignment"] = None
            if (
                assigned is not None and isinstance(assigned.get("data"), list)
                and all(
                    isinstance(row, dict) and str(row.get("id", "")).isascii()
                    and str(row.get("id", "")).isdigit() for row in assigned["data"]
                )
                and not (assigned.get("paging") or {}).get("next")
            ):
                self.report["observations"]["page_assignment"] = any(
                    str(row.get("id")) == page_id for row in assigned.get("data", [])
                )
                self.report["observations"]["target_page_tasks"] = sorted({
                    task for row in assigned["data"] if str(row.get("id")) == page_id
                    for task in (row.get("tasks") or []) if isinstance(task, str) and task in PAGE_TASK_NAMES
                })
        permissions = self.probe("token_permissions", lambda: self.account.get_api().call(
            "GET", ("me", "permissions"), params={"limit": 100}
        ).json(), stop_on_access=False)
        if permissions is not None and isinstance(permissions.get("data"), list) and all(isinstance(row, dict) for row in permissions["data"]):
            self.report["observations"]["permission_listing_complete"] = not bool((permissions.get("paging") or {}).get("next"))
            relevant = {"ads_read", "ads_management", "pages_show_list", "pages_read_engagement", "pages_manage_ads", "business_management"}
            self.report["observations"]["token_permissions"] = {
                row["permission"]: row.get("status") for row in permissions.get("data", [])
                if row.get("permission") in relevant
            }
        # Compare first assets without exposing text, links, or media hashes.
        feed = (self.values.get("optional:asset_feed_spec") or {}).get("asset_feed_spec") or {}
        video = story.get("video_data") or {}
        base = story.get("link_data") or video or story.get("template_data") or {}
        if video:
            base = {
                **video,
                "name": video.get("title"),
                "description": video.get("link_description"),
                "link": ((video.get("call_to_action") or {}).get("value") or {}).get("link"),
            }
        comparisons = {}
        for asset, asset_key, story_key in (
            ("bodies", "text", "message"), ("titles", "text", "name"),
            ("descriptions", "text", "description"), ("link_urls", "website_url", "link"),
            ("images", "hash", "image_hash"), ("videos", "video_id", "video_id"),
        ):
            entries = feed.get(asset) or []
            comparisons[asset] = None  # Missing values mean untested, never a match.
            if entries and asset_key in entries[0] and base.get(story_key) is not None:
                comparisons[asset] = entries[0][asset_key] == base[story_key]
        self.report["observations"]["first_asset_matches_story"] = comparisons


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", default="d2a")
    parser.add_argument("--account-id", default="912296209831774")
    parser.add_argument("--creative-id", default="2851905205182644")
    parser.add_argument("--ad-id", default="120251493238380573")
    parser.add_argument("--output-dir", type=Path, default=Path("diagnostic-report"))
    args = parser.parse_args()
    enforce_local_facebook_group(args.group)
    group, accounts = select_accounts(args.group, args.account_id)
    if len(accounts) != 1 or any(not value.isascii() or not value.isdigit() for value in (args.creative_id, args.ad_id)):
        parser.error("Choose one account and numeric creative/ad IDs")
    report = dict(
        account_id=accounts[0], group=args.group, creative_id=args.creative_id,
        ad_id=args.ad_id, api_version="v25.0", status="incomplete", probes=[], observations={},
    )
    account = get_ads_account(accounts[0], group["token"], 60.0, "v25.0")
    probe = CreativeProbe(account, report, args.output_dir)
    probe.run()
    print(json.dumps(report["observations"]), flush=True)
    return int(report["status"] != "complete")


if __name__ == "__main__":
    raise SystemExit(main())
