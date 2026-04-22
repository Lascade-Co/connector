from facebook_ads import facebook_ads_source, facebook_insights_source
from facebook_ads.settings import DEFAULT_INSIGHT_FIELDS
import os

# Pull conversions + conversion_values too, so we can compute subscription revenue.
INSIGHT_FIELDS = DEFAULT_INSIGHT_FIELDS + ("conversions", "conversion_values")


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
        fields=INSIGHT_FIELDS,
        **kwargs,
    )