#!/usr/bin/env python3
"""Générateur pédagogique de remplacement pour les parties 1 à 3.

Le sujet mentionne un producer.py fourni, mais celui-ci ne figure pas parmi les
pièces reçues. Ce fichier crée donc un jeu SYNTHÉTIQUE, reproductible, sans
prétendre reproduire des données réelles. Il fonctionne sans dépendance externe.

Un événement correspond à un changement d'état, pas à une commande distincte.
Les doublons sont des copies exactes d'un couple (order_id, status). Le retard
est simulé en ordonnant les émissions selon event_time + délai; la date métier
reste intacte. Sans horodatage d'arrivée enregistré, ce fichier batch ne constitue
pas une preuve de retard de traitement Kafka/Streaming.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random


BASE_TIME = datetime(2026, 11, 15, 8, 0, 0, tzinfo=timezone.utc)
CITIES = {
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Marrakech": (31.6295, -7.9811),
    "Fès": (34.0331, -5.0003),
    "Tanger": (35.7595, -5.8340),
}
CATEGORIES = ("pizza", "tacos", "sushi", "marocain", "burger")


def iso_utc(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def make_restaurants(seed: int = 42) -> list[dict]:
    """200 établissements fictifs : 40 par ville, 8 par catégorie et ville."""
    rng = random.Random(seed + 100_003)
    cities = list(CITIES)
    result = []
    for index in range(200):
        city = cities[index % len(cities)]
        category = CATEGORIES[(index // len(cities)) % len(CATEGORIES)]
        lat, lon = CITIES[city]
        result.append({
            "restaurant_id": f"R-{index + 1:03d}",
            "name": f"Maison {category.capitalize()} {index + 1:03d}",
            "city": city,
            "category": category,
            "latitude": round(lat + rng.uniform(-0.025, 0.025), 6),
            "longitude": round(lon + rng.uniform(-0.025, 0.025), 6),
        })
    return result


def generate(n: int = 50_000, seed: int = 42) -> tuple[list[dict], list[dict], dict]:
    if n < 1:
        raise ValueError("n doit être supérieur ou égal à 1")
    rng = random.Random(seed)
    restaurants = make_restaurants(seed)
    duplicate_count = min(round(n * 0.02), n // 2)
    unique_count = n - duplicate_count
    scheduled: list[tuple[datetime, int, dict]] = []
    order_index = 0
    unique_events: list[dict] = []
    event_times: list[datetime] = []

    while len(unique_events) < unique_count:
        order_index += 1
        restaurant = rng.choice(restaurants)
        created = BASE_TIME + timedelta(seconds=(order_index - 1) * 2)
        amount_cents = rng.randint(3500, 42000)
        city_index = list(CITIES).index(restaurant["city"])
        common = {
            "order_id": f"ORD-{order_index:07d}",
            "restaurant_id": restaurant["restaurant_id"],
            "customer_id": f"C-{rng.randint(1, 15000):05d}",
            "city": restaurant["city"],
            "amount_mad": amount_cents / 100,
            "items_count": rng.randint(1, 6),
            "distance_km": round(rng.uniform(0.3, 11.0), 2),
            "courier_id": f"L-{city_index + 1:02d}-{rng.randint(1, 40):02d}",
        }
        if rng.random() < 0.15:
            states = [("CREATED", created), ("CANCELLED", created + timedelta(minutes=rng.randint(2, 10)))]
        else:
            pickup = created + timedelta(minutes=rng.randint(6, 18))
            delivered = pickup + timedelta(minutes=rng.randint(6, 18))
            states = [("CREATED", created), ("PICKED_UP", pickup), ("DELIVERED", delivered)]
        for status, event_time in states:
            if len(unique_events) == unique_count:
                break  # Historique arrêté : la dernière commande peut être en cours.
            event = {**common, "status": status, "event_time": iso_utc(event_time)}
            unique_events.append(event)
            event_times.append(event_time)

    delayed_count = round(unique_count * 0.05)
    delayed_indices = set(rng.sample(range(unique_count), delayed_count))
    emission_times = []
    for index, (event, event_time) in enumerate(zip(unique_events, event_times)):
        delay = rng.randint(1, 15 * 60) if index in delayed_indices else 0
        emission_time = event_time + timedelta(seconds=delay)
        emission_times.append(emission_time)
        scheduled.append((emission_time, index, event))

    duplicate_indices = rng.sample(range(unique_count), duplicate_count)
    for sequence, index in enumerate(duplicate_indices, start=unique_count):
        scheduled.append((emission_times[index] + timedelta(seconds=rng.randint(1, 60)), sequence, dict(unique_events[index])))
    scheduled.sort(key=lambda item: (item[0], item[1]))
    events = [event for _, _, event in scheduled]
    status_counts = dict(sorted(Counter(event["status"] for event in events).items()))
    unique_status_counts = dict(sorted(Counter(event["status"] for event in unique_events).items()))
    manifest = {
        "provenance": "Données synthétiques; générateur pédagogique de remplacement, non fourni avec le sujet.",
        "seed": seed,
        "base_event_time_utc": iso_utc(BASE_TIME),
        "event_count": len(events),
        "unique_order_status_count": unique_count,
        "order_count": order_index,
        "duplicate_count": duplicate_count,
        "duplicate_fraction_of_output": duplicate_count / n,
        "delayed_unique_event_count": delayed_count,
        "delay_max_seconds": 900,
        "delay_note": "Ordre d'émission simulé selon event_time + délai. Aucun arrival_time n'est persisté; le batch seul ne prouve pas la latence d'arrivée.",
        "restaurant_count": len(restaurants),
        "status_counts_raw": status_counts,
        "status_counts_deduplicated": unique_status_counts,
        "event_time_min": min(event["event_time"] for event in events),
        "event_time_max": max(event["event_time"] for event in events),
        "reference_format": "JSON Lines (un objet par ligne), malgré l'extension restaurants.json",
        "currency": "MAD",
    }
    return events, restaurants, manifest


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["file"], default="file", help="Seul le jalon batch est implémenté.")
    parser.add_argument("--n", type=int, default=50_000, help="Nombre total d'événements, doublons inclus.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent / "data")
    args = parser.parse_args()
    if args.n < 1:
        parser.error("--n doit être supérieur ou égal à 1")
    events, restaurants, manifest = generate(args.n, args.seed)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output_dir / "orders_history.jsonl", events)
    write_jsonl(args.output_dir / "restaurants.json", restaurants)
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{len(events)} événements, {manifest['duplicate_count']} doublons et {len(restaurants)} restaurants écrits dans {args.output_dir}")


if __name__ == "__main__":
    main()
