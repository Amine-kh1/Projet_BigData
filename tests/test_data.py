"""Contrôles métier du jeu synthétique et du calcul indépendant."""

from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from producer import CITIES, generate, write_jsonl  # noqa: E402
from reference_results import build_reference, cents  # noqa: E402


class SyntheticHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events, cls.restaurants, cls.manifest = generate(50_000, 42)

    def test_exact_volume_and_duplicate_contract(self):
        self.assertEqual(len(self.events), 50_000)
        seen = {}
        for event in self.events:
            key = (event["order_id"], event["status"])
            if key in seen:
                self.assertEqual(event, seen[key])
            seen[key] = event
        self.assertEqual(len(seen), 49_000)
        self.assertEqual(self.manifest["duplicate_count"], 1_000)

    def test_reference_uniqueness_and_foreign_keys(self):
        reference = {r["restaurant_id"]: r for r in self.restaurants}
        self.assertEqual(len(reference), 200)
        self.assertEqual(Counter(r["city"] for r in self.restaurants), Counter({city: 40 for city in CITIES}))
        for event in self.events:
            self.assertIn(event["restaurant_id"], reference)
            self.assertEqual(event["city"], reference[event["restaurant_id"]]["city"])

    def test_order_dimensions_and_lifecycle(self):
        orders = defaultdict(dict)
        fixed = ("restaurant_id", "customer_id", "city", "amount_mad", "items_count", "distance_km", "courier_id")
        for event in self.events:
            order = orders[event["order_id"]]
            if order:
                first = next(iter(order.values()))
                self.assertEqual(tuple(first[field] for field in fixed), tuple(event[field] for field in fixed))
            order[event["status"]] = event
            self.assertGreaterEqual(cents(event["amount_mad"]), 0)
            self.assertLessEqual(abs(Decimal(str(event["amount_mad"])).as_tuple().exponent), 2)
        for states in orders.values():
            self.assertIn("CREATED", states)
            self.assertFalse("DELIVERED" in states and "CANCELLED" in states)
            if "DELIVERED" in states:
                self.assertIn("PICKED_UP", states)
                self.assertLess(states["CREATED"]["event_time"], states["PICKED_UP"]["event_time"])
                self.assertLess(states["PICKED_UP"]["event_time"], states["DELIVERED"]["event_time"])
            for event in states.values():
                self.assertIsNotNone(datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")).tzinfo)

    def test_seed_reproducible_and_small_counts(self):
        self.assertEqual(generate(175, 7), generate(175, 7))
        for n in (1, 2, 10, 101):
            events, _, manifest = generate(n, 42)
            self.assertEqual(len(events), n)
            self.assertEqual(manifest["event_count"], n)
        with self.assertRaises(ValueError):
            generate(0)

    def test_independent_reference_reconciles_totals(self):
        with tempfile.TemporaryDirectory() as directory:
            orders_path = Path(directory) / "orders.jsonl"
            restaurants_path = Path(directory) / "restaurants.json"
            write_jsonl(orders_path, self.events)
            write_jsonl(restaurants_path, self.restaurants)
            result = build_reference(orders_path, restaurants_path)
        self.assertEqual(result["duplicate_count"], 1000)
        for mode in ("raw", "deduplicated"):
            summary = result[mode]
            self.assertEqual(summary["total_revenue_cents"], sum(row["revenue_cents"] for row in summary["cities"]))
            self.assertEqual(summary["total_revenue_cents"], sum(row["revenue_cents"] for row in summary["categories_by_city"]))
            self.assertEqual(summary["delivered_count"], sum(row["delivered_count"] for row in summary["cities"]))
        self.assertGreater(result["raw"]["total_revenue_cents"], result["deduplicated"]["total_revenue_cents"])

    def test_reference_rejects_conflicting_duplicates(self):
        event = dict(self.events[0])
        conflict = {**event, "amount_mad": event["amount_mad"] + 1}
        with tempfile.TemporaryDirectory() as directory:
            orders_path = Path(directory) / "orders.jsonl"
            restaurants_path = Path(directory) / "restaurants.json"
            write_jsonl(orders_path, [event, conflict])
            write_jsonl(restaurants_path, self.restaurants)
            with self.assertRaisesRegex(ValueError, "contradictoires"):
                build_reference(orders_path, restaurants_path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
