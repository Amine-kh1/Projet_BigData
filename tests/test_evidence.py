"""Cas unitaires du vérificateur; les objets simulés ne sont jamais des preuves."""

import copy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from producer import generate  # noqa: E402
from reference_results import summarize  # noqa: E402
from verify_evidence import verify  # noqa: E402


class EvidenceVerifierTests(unittest.TestCase):
    def setUp(self):
        events, restaurants, manifest = generate(1000, 42)
        unique = {(row["order_id"], row["status"]): row for row in events}
        self.reference = {"raw": summarize(events, restaurants),
                          "deduplicated": summarize(list(unique.values()), restaurants),
                          "duplicate_count": manifest["duplicate_count"]}
        def cities(mode):
            return [{"city": row["city"], "revenue_mad": row["revenue_mad"], "delivered_events": row["delivered_count"],
                     "average_basket_mad": row["average_basket_mad"]} for row in self.reference[mode]["cities"]]
        self.fixture = {
            "environment": {"spark_version": "unit-test-fixture", "master": "fixture"},
            "reconciliation_rdd_dataframe_sql": True,
            "quality": {"input_lines": 1000, "valid_rows_rdd": 1000, "valid_rows_dataframe": 1000,
                        "rejected_rows": 0, "duplicates_removed": manifest["duplicate_count"], "silver_rows": len(unique),
                        "missing_restaurant_events": 0, "city_mismatch_events": 0, "conflicting_duplicate_keys": 0},
            "raw_city_kpis": cities("raw"), "clean_city_kpis": cities("deduplicated"),
            "raw_top5_restaurants": [{"restaurant_id": row["restaurant_id"], "delivered_events": row["delivered_count"]}
                                     for row in self.reference["raw"]["top5_restaurants"]],
            "raw_revenue_category_city": [{"city": row["city"], "category": row["category"],
                                            "revenue_mad": row["revenue_mad"], "delivered_events": row["delivered_count"]}
                                           for row in self.reference["raw"]["categories_by_city"]],
        }

    def test_accepts_consistent_fixture(self):
        result = verify(self.fixture, self.reference)
        self.assertEqual(result["status"], "PASSED")
        self.assertGreater(result["checks_count"], 50)

    def test_rejects_changed_total_and_rank(self):
        fixture = copy.deepcopy(self.fixture)
        fixture["clean_city_kpis"][0]["revenue_mad"] = "0.00"
        fixture["raw_top5_restaurants"].reverse()
        result = verify(fixture, self.reference)
        self.assertEqual(result["status"], "FAILED")
        self.assertTrue(any(error["check"] == "raw.top5_ordered" for error in result["errors"]))
        self.assertTrue(any(error["check"] == "clean.total_revenue_cents" for error in result["errors"]))

    def test_rejects_missing_category_and_duplicate_city(self):
        fixture = copy.deepcopy(self.fixture)
        fixture["raw_revenue_category_city"].pop()
        self.assertEqual(verify(fixture, self.reference)["status"], "FAILED")
        fixture["raw_city_kpis"].append(fixture["raw_city_kpis"][0])
        with self.assertRaisesRegex(ValueError, "dupliquée"):
            verify(fixture, self.reference)


if __name__ == "__main__":
    unittest.main(verbosity=2)
