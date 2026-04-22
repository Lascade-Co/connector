from facebook_ads import facebook_ads_source, facebook_insights_source
import os


def ads_src(cred):  # structure data
    return facebook_ads_source(account_id=cred["account_id"], access_token=cred["token"])


def insights_src(cred):  # metrics
    backfill_days = os.getenv("SUB_FB_BACKFILL_DAYS")
    kwargs = {}
    if backfill_days:
        try:
            days_int = int(backfill_days)
            if days_int > 0:
                kwargs["initial_load_past_days"] = days_int
        except ValueError:
            pass

    return facebook_insights_source(
        account_id=cred["account_id"],
        access_token=cred["token"],
        attribution_window_days_lag=7,
        **kwargs,
    )