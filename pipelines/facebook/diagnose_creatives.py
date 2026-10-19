"""GitHub Actions-only, read-only diagnosis of Meta creative read failures."""

from __future__ import annotations

import argparse
import functools
import html
import json
from pathlib import Path

from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.exceptions import FacebookRequestError

from facebook_ads.helpers import get_ads_account, get_data_chunked
from facebook_ads.rate_limit import RATE_LIMIT_CODES
from facebook_ads.settings import DEFAULT_ADCREATIVE_FIELDS
from utils import enforce_local_facebook_group, get_for_group


TARGET_CODE = 100
TARGET_SUBCODE = 2446211
LIGHT_FIELDS = ("id", "name")
AD_FIELDS = ("id", "name", "creative", "campaign_id", "adset_id", "effective_status")


def error_details(error: FacebookRequestError) -> dict:
    # Never stringify SDK exceptions: they may include request credentials.
    body = error.body()
    metadata = (body.get("error") or {}) if isinstance(body, dict) else {}
    return {
        "code": error.api_error_code(),
        "subcode": error.api_error_subcode(),
        "trace_id": metadata.get("fbtrace_id"),
        "http_status": error.http_status(),
    }


def is_target(error: dict) -> bool:
    return error.get("code") == TARGET_CODE and error.get("subcode") == TARGET_SUBCODE


def must_stop(error: dict) -> bool:
    return error.get("http_status") == 429 or error.get("code") in RATE_LIMIT_CODES | {10, 190, 200}


def check_batch(account, items: list[dict], report: dict) -> bool:
    """Keep the creative ID in every failure callback, unlike the ETL exception."""
    batch = account.get_api().new_batch()
    completed = set()
    stop = False

    def success(_response, creative_id):
        completed.add(creative_id)
        report["checked"] += 1

    def failure(response, item):
        nonlocal stop
        creative_id = str(item["id"])
        completed.add(creative_id)
        report["checked"] += 1
        error = error_details(response.error())
        report["failures"].append({
            "creative_id": creative_id,
            "creative_name": item.get("name", ""),
            "target_error": is_target(error),
            "error": error,
            "ads": report["ad_mapping"].get(creative_id, []),
        })
        print(f"Creative {creative_id}: error {error['code']} / {error['subcode']}", flush=True)
        stop = stop or must_stop(error)

    for item in items:
        creative_id = str(item["id"])
        AdCreative(fbid=creative_id, api=account.get_api()).api_get(
            fields=DEFAULT_ADCREATIVE_FIELDS,
            batch=batch,
            success=functools.partial(success, creative_id=creative_id),
            failure=functools.partial(failure, item=item),
        )
    remaining = batch.execute()
    if remaining is not None or len(completed) != len(items):
        raise RuntimeError("incomplete_batch_response")
    return stop


def diagnose_account(account, report: dict, *, max_creatives: int, stop_after_first: bool, save):
    """Scan current accessible ads and creatives without touching dlt state or destinations."""
    report.update(
        checked=0, failures=[], errors=[], ad_mapping={},
        mapping_complete=False, creative_listing_complete=False, status="incomplete",
    )
    save()
    try:
        # Capture ad names before hydration, so they survive a later quota stop.
        for chunk in get_data_chunked(account.get_ads, AD_FIELDS, None, 100):
            for ad in chunk:
                creative_id = str((ad.get("creative") or {}).get("id", ""))
                if creative_id:
                    report["ad_mapping"].setdefault(creative_id, []).append({
                        key: ad.get(key) for key in AD_FIELDS if key != "creative"
                    })
            save()
        report["mapping_complete"] = True
    except FacebookRequestError as exc:
        error = error_details(exc)
        report["errors"].append({"stage": "ad_mapping", **error})
        save()
        if must_stop(error):
            return

    seen = set()
    try:
        for chunk in get_data_chunked(
            account.get_ad_creatives, LIGHT_FIELDS, None, 100, {"summary": "false"}
        ):
            fresh = []
            for item in chunk:
                creative_id = str(item["id"])
                if creative_id not in seen:
                    seen.add(creative_id)
                    fresh.append(item)
            for offset in range(0, len(fresh), 50):
                allowance = max_creatives - report["checked"]
                if allowance <= 0:
                    report["errors"].append({"stage": "scan", "reason": "max_creatives_reached"})
                    return
                items = fresh[offset:offset + min(50, allowance)]
                stop = check_batch(account, items, report)
                save()
                print(f"Checked {report['checked']} creatives in account {report['account_id']}", flush=True)
                if stop:
                    report["errors"].append({"stage": "scan", "reason": "auth_or_quota_error"})
                    return
                if stop_after_first and any(f["target_error"] for f in report["failures"]):
                    report["status"] = (
                        "target_found_stopped_early" if report["mapping_complete"] else "incomplete"
                    )
                    return
                if len(items) < min(50, len(fresh) - offset):
                    report["errors"].append({"stage": "scan", "reason": "max_creatives_reached"})
                    return
        report["creative_listing_complete"] = True
        report["status"] = "complete" if report["mapping_complete"] else "incomplete"
    except FacebookRequestError as exc:
        report["errors"].append({"stage": "creative_scan", **error_details(exc)})
    finally:
        save()


def select_accounts(group_name: str, account_filter: str) -> tuple[dict, list[str]]:
    if group_name not in {"d2", "d2a", "d2b", "d2c"}:
        raise SystemExit("Choose d2, d2a, d2b, or d2c.")
    group, accounts = get_for_group(group_name, "facebook")
    accounts = list(dict.fromkeys(str(a).removeprefix("act_") for a in accounts))
    selected = account_filter.strip().removeprefix("act_")
    if selected:
        if selected not in accounts:
            raise SystemExit("The requested account does not belong to the selected group.")
        accounts = [selected]
    if not accounts or any(not account.isascii() or not account.isdigit() for account in accounts):
        raise SystemExit("The selected group must contain numeric ad account IDs.")
    return group, accounts


def cell(value) -> str:
    escaped = html.escape(str(value)).replace("\n", " ").replace("\r", " ")
    for character in "|`[]*_\\":
        escaped = escaped.replace(character, f"&#{ord(character)};")
    return escaped


def render_summary(reports: list[dict]) -> str:
    lines = ["# Facebook creative diagnostic", "", "Read-only checks for error 100 / 2446211.", ""]
    for report in reports:
        lines += [
            f"## Account {report['account_id']} ({cell(report['group'])})", "",
            f"Status: **{cell(report.get('status', 'incomplete'))}**. "
            f"Creatives checked: {report.get('checked', 0)}. "
            f"Ad mapping complete: {report.get('mapping_complete', False)}.", "",
            "| Target error? | Creative ID | Creative name | Ad ID | Ad name | Ads Manager |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for failure in report.get("failures", []):
            for ad in failure["ads"] or [{}]:
                ad_id = str(ad.get("id", ""))
                link = (
                    f"[Open ad](https://adsmanager.facebook.com/adsmanager/manage/ads?"
                    f"act={report['account_id']}&selected_ad_ids={ad_id})"
                    if ad_id.isascii() and ad_id.isdigit() else "No accessible linked ad found"
                )
                lines.append("| " + " | ".join([
                    "Yes" if failure["target_error"] else "No (other error)",
                    cell(failure["creative_id"]), cell(failure["creative_name"]),
                    cell(ad_id), cell(ad.get("name", "")), link,
                ]) + " |")
        if not any(f["target_error"] for f in report.get("failures", [])):
            lines += ["", "**Target error not reproduced.** This does not rule out a listing-only failure."]
        if report.get("status") != "complete":
            lines += ["", "This was not an exhaustive scan of all accessible creatives and ads."]
        if report.get("errors"):
            lines += ["", "Stopping/mapping errors: `" + cell(json.dumps(report["errors"])) + "`."]
        lines.append("")
    return "\n".join(lines)


def write_reports(output_dir: Path, reports: list[dict]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    # Do not persist successful creative payloads, tokens, or story contents.
    public = [{k: v for k, v in report.items() if k != "ad_mapping"} for report in reports]
    (output_dir / "report.json").write_text(json.dumps(public, indent=2), encoding="utf-8")
    (output_dir / "summary.md").write_text(render_summary(public), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", default="d2a")
    parser.add_argument("--account-id", default="912296209831774")
    parser.add_argument("--max-creatives", type=int, default=5000)
    parser.add_argument("--scan-all", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("diagnostic-report"))
    args = parser.parse_args()
    # No local diagnostic runs, even for d1c. Validate before reading credentials.
    enforce_local_facebook_group(args.group)
    if args.max_creatives < 1:
        parser.error("--max-creatives must be positive")
    group, accounts = select_accounts(args.group, args.account_id)
    reports = []
    save = functools.partial(write_reports, args.output_dir, reports)
    save()
    for account_id in accounts:
        report = {"account_id": account_id, "group": args.group, "fields": list(DEFAULT_ADCREATIVE_FIELDS)}
        reports.append(report)
        save()
        try:
            account = get_ads_account(account_id, group["token"], 60.0, "v25.0")
            diagnose_account(
                account, report, max_creatives=args.max_creatives,
                stop_after_first=not args.scan_all, save=save,
            )
        except Exception as exc:
            report["status"] = "incomplete"
            report.setdefault("errors", []).append({"stage": "diagnostic", "type": type(exc).__name__})
        finally:
            save()
        print(f"Account {account_id}: {report['status']}; checked={report.get('checked', 0)}; "
              f"target_failures={sum(f['target_error'] for f in report.get('failures', []))}")
    return int(any(r["status"] == "incomplete" for r in reports))


if __name__ == "__main__":
    raise SystemExit(main())
