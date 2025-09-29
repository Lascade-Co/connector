from facebook_ads import facebook_ads_source, facebook_insights_source
import os


def ads_src(cred):  # structure data
    return facebook_ads_source(account_id=cred["account_id"], access_token=cred["token"])


def insights_src(cred):  # metrics
    # Allow manual backfill to override initial window via env
    backfill_days = os.getenv("FB_BACKFILL_DAYS")
    kwargs = {}
    if backfill_days:
        try:
            days_int = int(backfill_days)
            if days_int > 0:
                kwargs["initial_load_past_days"] = days_int
        except ValueError:
            # ignore invalid input; fallback to default in source
            pass

    return facebook_insights_source(
        account_id=cred["account_id"],
        access_token=cred["token"],
        attribution_window_days_lag=7,
        **kwargs,
    )
