from facebook_ads import facebook_ads_source, facebook_insights_source
from facebook_ads.settings import DEFAULT_INSIGHT_FIELDS
import os

# Meta reports the account's own billing currency per row, which is what the
# AED normalization keys off. Meta reports the Subscribe event (count and
# value) only in conversions/conversion_values, never in actions. Scoped to
# this pipeline so the esim and subscription datasets keep their current columns.
INSIGHT_FIELDS = DEFAULT_INSIGHT_FIELDS + (
    "account_currency",
    "conversions",
    "conversion_values",
)

# Delivered-country Insights. ``ctr`` only, no ``cpc``/``cpm``/``cpp``: the
# degraded-report guard pairs those with ``spend``, and a small per-country
# slice can round spend to "0" beside a positive cpm, refusing normal windows.
# ``ctr`` is exact on integer counts and keeps the impressions/clicks sentinels.
COUNTRY_INSIGHT_FIELDS = (
    "account_id",
    "account_name",
    "account_currency",
    "campaign_id",
    "campaign_name",
    "date_start",
    "date_stop",
    "spend",
    "impressions",
    "clicks",
    "ctr",
    "actions",
    "action_values",
)


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


def country_insights_src(cred, *, report_start_date, report_end_date, pre_flatten):
    return facebook_insights_source(
        account_id=cred["account_id"],
        access_token=cred["token"],
        attribution_window_days_lag=7,
        fields=COUNTRY_INSIGHT_FIELDS,
        level="campaign",
        breakdowns="ads_insights_country",
        report_start_date=report_start_date,
        report_end_date=report_end_date,
        pre_flatten=pre_flatten,
    )
