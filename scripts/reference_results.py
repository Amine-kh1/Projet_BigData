#!/usr/bin/env python3
"""Calcul de contrôle indépendant, en Python standard et en centimes.

Ces résultats valident le sens métier des agrégations Spark. Ils ne constituent
ni une exécution de Spark, ni une mesure de performances distribuées.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path


def cents(value: object) -> int:
    return int((Decimal(str(value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def money(value: int) -> str:
    return f"{Decimal(value) / 100:.2f}"


def summarize(events: list[dict], restaurants: list[dict]) -> dict:
    reference = {r["restaurant_id"]: r for r in restaurants}
    if len(reference) != len(restaurants):
        raise ValueError("restaurant_id dupliqué dans le référentiel")
    cities: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    category_cities: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    restaurant_counts: Counter[str] = Counter()
    for event in events:
        restaurant = reference.get(event["restaurant_id"])
        if restaurant is None:
            raise ValueError(f"Restaurant absent du référentiel : {event['restaurant_id']}")
        if event["city"] != restaurant["city"]:
            raise ValueError("Ville incohérente entre commande et restaurant")
        if event["status"] != "DELIVERED":
            continue
        amount = cents(event["amount_mad"])
        cities[event["city"]][0] += amount
        cities[event["city"]][1] += 1
        key = (event["city"], restaurant["category"])
        category_cities[key][0] += amount
        category_cities[key][1] += 1
        restaurant_counts[event["restaurant_id"]] += 1
    city_rows = []
    for city, (amount, count) in sorted(cities.items()):
        city_rows.append({"city": city, "revenue_cents": amount, "revenue_mad": money(amount), "delivered_count": count,
                          "average_basket_mad": str((Decimal(amount) / 100 / count).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))})
    category_rows = [{"city": city, "category": category, "revenue_cents": amount, "revenue_mad": money(amount), "delivered_count": count}
                     for (city, category), (amount, count) in sorted(category_cities.items())]
    top = [{"restaurant_id": restaurant_id, "name": reference[restaurant_id]["name"],
            "city": reference[restaurant_id]["city"], "delivered_count": count}
           for restaurant_id, count in sorted(restaurant_counts.items(), key=lambda item: (-item[1], item[0]))[:5]]
    total = sum(row["revenue_cents"] for row in city_rows)
    return {"event_count": len(events), "delivered_count": sum(restaurant_counts.values()), "total_revenue_cents": total,
            "total_revenue_mad": money(total), "cities": city_rows, "categories_by_city": category_rows, "top5_restaurants": top}


def read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def build_reference(orders_path: Path, restaurants_path: Path) -> dict:
    events = read_jsonl(orders_path)
    restaurants = read_jsonl(restaurants_path)
    deduplicated: dict[tuple[str, str], dict] = {}
    for event in events:
        key = (event["order_id"], event["status"])
        if key in deduplicated and deduplicated[key] != event:
            raise ValueError(f"Doublons contradictoires pour {key}")
        deduplicated[key] = event
    raw = summarize(events, restaurants)
    clean = summarize(list(deduplicated.values()), restaurants)
    return {
        "verification_method": "Python standard indépendant, sommes exactes en centimes; aucune mesure Spark.",
        "raw": raw,
        "deduplicated": clean,
        "duplicate_count": len(events) - len(deduplicated),
        "duplicate_revenue_overstatement_mad": money(raw["total_revenue_cents"] - clean["total_revenue_cents"]),
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=Path, default=root / "data/orders_history.jsonl")
    parser.add_argument("--restaurants", type=Path, default=root / "data/restaurants.json")
    parser.add_argument("--output", type=Path, default=root / "evidence/reference_results.json")
    args = parser.parse_args()
    result = build_reference(args.orders, args.restaurants)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"raw": {key: result["raw"][key] for key in ("event_count", "delivered_count", "total_revenue_mad")},
                      "deduplicated": {key: result["deduplicated"][key] for key in ("event_count", "delivered_count", "total_revenue_mad")},
                      "duplicate_count": result["duplicate_count"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
