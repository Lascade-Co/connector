# Maps each table to the field used to track the last pulled record.
TABLE_TO_FIELD_MAPPING = {
    "auth_user": "id",
    "common_app": "updated_at",
    "common_coupon": "updated_at",
    "common_trackablepayload": "created_at",
    "sso_appinstance": "updated_at",
    "travel_animator_animationstate": "updated_at",
    "travel_animator_annotation": "id",
    "travel_animator_annotationmedia": "id",
    "travel_animator_glbfile": "id",
    "travel_animator_map": "updated_at",
    "travel_animator_marker": "id",
    "travel_animator_routepoint": "updated_at",
    "travel_animator_savedroute": "updated_at",
    "travel_animator_texture": "updated_at",
    "travel_animator_threedmodel": "updated_at",
    "travel_animator_threedmodel_allowed_users": "id",
    "travel_animator_usdzfile": "id"
}

SELECTED_TABLES = list(TABLE_TO_FIELD_MAPPING.keys())
