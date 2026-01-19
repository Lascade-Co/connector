LOG_TABLE = "server_logs_log"

# Maps each table to the field used to track the last pulled record.
TABLE_TO_FIELD_MAPPING = {
    "reports_report": "updated_at",
    "users_application": "updated_at",
    "users_appuser": "updated_at",
    "users_conversions": "updated_at",
    "users_deviceuser": "id",
    "users_provider": "updated_at",
    "users_providerservice": "updated_at",
    "users_usersession": "id",
    "users_attribution": "id",
}

SELECTED_TABLES = list(TABLE_TO_FIELD_MAPPING.keys())
