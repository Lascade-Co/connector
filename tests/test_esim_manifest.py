import unittest

from pipelines.esim.manifest import parse_manifest


def _dataset(name: str, schema_version: str) -> dict:
    return {
        "name": name,
        "endpoint": f"/internal/analytics/exports/{name}/",
        "primary_key": "id",
        "watermark_field": "updated_at",
        "default_limit": 500,
        "strategy": "incremental",
        "schema_version": schema_version,
        "available": True,
    }


class OrdersManifestTests(unittest.TestCase):
    def test_accepts_rolling_versions_and_adds_only_new_column_hints(self):
        money_columns = {
            "subtotal_eur",
            "profit_eur",
            "item_count",
            "distinct_plan_count",
            "is_cart",
        }
        for version in ("1.1", "1.2", "1.3", "1.4", "1.5"):
            parsed = parse_manifest([_dataset("orders", version)])[0]
            self.assertEqual(parsed["schema_version"], version)
            expected = set() if version == "1.1" else money_columns
            if version == "1.5":
                expected = expected | {"conversion_route", "acquisition_route"}
            self.assertEqual(set(parsed["columns"] or {}), expected)

    def test_rejects_unreviewed_major_version(self):
        """A MAJOR bump can change or drop existing columns, so it must stay gated."""
        with self.assertRaisesRegex(ValueError, "unsupported schema_version"):
            parse_manifest([_dataset("orders", "2.0")])

    def test_rejects_malformed_version(self):
        for value in ("1", "1.2.3", "1.x", "", None):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "unsupported schema_version"):
                    parse_manifest([_dataset("orders", value)])

    def test_accepts_unreviewed_minor_of_reviewed_major_with_warning(self):
        """An additive MINOR bump must not take the whole run down.

        Regression test: the backend shipped orders 1.4 while this connector
        pinned an exact set ending at 1.3, and the resulting raise killed every
        dataset in the run, not just orders.
        """
        with self.assertLogs("pipelines.esim.manifest", level="WARNING") as logs:
            parsed = parse_manifest([_dataset("orders", "1.99")])[0]

        self.assertEqual(parsed["schema_version"], "1.99")
        self.assertIn("SCHEMA DRIFT", "".join(logs.output))
        # The warning must name the file to edit, or it is not actionable in a CI log.
        self.assertIn("pipelines/esim/constants.py", "".join(logs.output))

    def test_reviewed_versions_log_no_drift_warning(self):
        for version in ("1.1", "1.2", "1.3", "1.4", "1.5"):
            with self.subTest(version=version):
                with self.assertNoLogs("pipelines.esim.manifest", level="WARNING"):
                    parse_manifest([_dataset("orders", version)])

    def test_reviewed_user_and_session_versions_are_pinned(self):
        for name, version in (("users", "1.2"), ("sessions", "1.2")):
            with self.subTest(name=name):
                with self.assertNoLogs("pipelines.esim.manifest", level="WARNING"):
                    parsed = parse_manifest([_dataset(name, version)])[0]
                self.assertEqual(parsed["schema_version"], version)

    def test_session_1_2_adds_explicit_funnel_column_hints(self):
        parsed = parse_manifest([_dataset("sessions", "1.2")])[0]

        self.assertEqual(
            set(parsed["columns"]),
            {
                "brand_code",
                "visitor_id",
                "route",
                "raw_route",
                "referrer_host",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "plans_viewed",
                "plan_added",
                "checkout_viewed",
                "purchased",
                "plans_viewed_at",
                "plan_added_at",
                "checkout_viewed_at",
                "purchased_at",
            },
        )

    def test_older_backend_versions_do_not_receive_future_column_hints(self):
        session_1_0 = parse_manifest([_dataset("sessions", "1.0")])[0]
        session_1_1 = parse_manifest([_dataset("sessions", "1.1")])[0]
        user_1_1 = parse_manifest([_dataset("users", "1.1")])[0]
        order_1_4 = parse_manifest([_dataset("orders", "1.4")])[0]

        self.assertEqual(set(session_1_0["columns"] or {}), {"route"})
        self.assertNotIn("plans_viewed", session_1_1["columns"])
        self.assertNotIn("purchased", session_1_1["columns"])
        self.assertIsNone(user_1_1["columns"])
        self.assertNotIn("conversion_route", order_1_4["columns"])
        self.assertNotIn("acquisition_route", order_1_4["columns"])

    def test_user_and_session_major_bumps_are_rejected(self):
        for name in ("users", "sessions"):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "unsupported schema_version"):
                    parse_manifest([_dataset(name, "2.0")])


if __name__ == "__main__":
    unittest.main()
