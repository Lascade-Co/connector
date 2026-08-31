from facebook_ads import facebook_ads_source, facebook_insights_source
from facebook_ads.settings import DEFAULT_INSIGHT_FIELDS
import os

# Meta reports the account's own billing currency per row, which is what the
# AED normalization keys off. Scoped to this pipeline so the esim and
# subscription datasets keep their current columns.
INSIGHT_FIELDS = DEFAULT_INSIGHT_FIELDS + ("account_currency",)


def ads_src(cred):  # structure data
    return facebook_ads_source(
        account_id=cred["account_id"], access_token=cred["token"]
    )


def insights_src(
    cred, *, report_start_date=None, report_end_date=None, pre_flatten=None
):  # metrics
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
        fields=INSIGHT_FIELDS,
        report_start_date=report_start_date,
        report_end_date=report_end_date,
        pre_flatten=pre_flatten,
        **kwargs,
    )
