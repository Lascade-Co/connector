from facebook_ads import facebook_ads_source, facebook_insights_source


def ads_src(cred):  # structure data
    return facebook_ads_source(account_id=cred["account_id"], access_token=cred["token"])


def insights_src(cred):  # metrics
    return facebook_insights_source(
        account_id=cred["account_id"],
        access_token=cred["token"],
        attribution_window_days_lag=7
    )
