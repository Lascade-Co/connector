LOG_TABLE = "server_logs_log"

# Maps each table to the field used to track the last pulled record.
TABLE_TO_FIELD_MAPPING = {
    "reports_report": ("updated_at", None),
    "reports_domain": ("updated_at", None),
    "users_application": ("updated_at", None),
    "users_appuser": ("updated_at", None),
    "users_conversions": ("updated_at", None),
    "users_deviceuser": ("id", None),
    "users_provider": ("updated_at", None),
    "users_providerservice": ("updated_at", None),
    "users_usersession": ("id", None),
    "users_attribution": ("id", None),
}
