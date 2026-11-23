#!/usr/bin/env python3
"""Compare les vraies sorties Spark à un calcul métier indépendant.

Sans results.json, l'état est explicitement SKIPPED et --require-spark renvoie
le code 2. Un calcul de référence seul n'est jamais présenté comme une preuve
d'exécution Spark. Une divergence renvoie 1; une réconciliation complète 0.
"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sys


def amount_in_cents(value: object) -> int:
    amount = Decimal(str(value))
    if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")):
        raise ValueError(f"Montant invalide ou comportant plus de deux décimales : {value!r}")
    return int(amount * 100)


def index_rows(rows: list[dict], keys: tuple[str, ...], label: str) -> dict:
    indexed = {}
    for row in rows:
        key = tuple(row[field] for field in keys)
        if key in indexed:
            raise ValueError(f"{label} contient une clé dupliquée : {key}")
        indexed[key] = row
    return indexed


def verify(spark: dict, reference: dict) -> dict:
    """Fonction sans I/O, testable avec des preuves réelles ou des cas unitaires."""
    errors = []
    checks = []

    def same(label, actual, expected):
        checks.append(label)
        if actual != expected:
            errors.append({"check": label, "actual": actual, "expected": expected})

    same("reconciliation_rdd_dataframe_sql", spark["reconciliation_rdd_dataframe_sql"], True)
    quality = spark["quality"]
    same("input_lines", quality["input_lines"], reference["raw"]["event_count"])
    same("valid_rows_rdd", quality["valid_rows_rdd"], reference["raw"]["event_count"])
    same("valid_rows_dataframe", quality["valid_rows_dataframe"], reference["raw"]["event_count"])
    same("rejected_rows", quality["rejected_rows"], 0)
    same("duplicates_removed", quality["duplicates_removed"], reference["duplicate_count"])
    same("silver_rows", quality["silver_rows"], reference["deduplicated"]["event_count"])
    for key in ("missing_restaurant_events", "city_mismatch_events", "conflicting_duplicate_keys"):
        same(key, quality[key], 0)

    for prefix, spark_field, ref_field in (("raw", "raw_city_kpis", "raw"), ("clean", "clean_city_kpis", "deduplicated")):
        actual = index_rows(spark[spark_field], ("city",), spark_field)
        expected = index_rows(reference[ref_field]["cities"], ("city",), ref_field)
        same(f"{prefix}.city_keys", sorted(actual), sorted(expected))
        for city in sorted(set(actual) & set(expected)):
            left, right = actual[city], expected[city]
            same(f"{prefix}.{city[0]}.revenue_cents", amount_in_cents(left["revenue_mad"]), right["revenue_cents"])
            same(f"{prefix}.{city[0]}.delivered_events", left["delivered_events"], right["delivered_count"])
            same(f"{prefix}.{city[0]}.average_basket_cents", amount_in_cents(left["average_basket_mad"]), amount_in_cents(right["average_basket_mad"]))
        same(f"{prefix}.total_revenue_cents", sum(amount_in_cents(row["revenue_mad"]) for row in actual.values()), reference[ref_field]["total_revenue_cents"])
        same(f"{prefix}.total_delivered_events", sum(row["delivered_events"] for row in actual.values()), reference[ref_field]["delivered_count"])

    actual_top = [(row["restaurant_id"], row["delivered_events"]) for row in spark["raw_top5_restaurants"]]
    expected_top = [(row["restaurant_id"], row["delivered_count"]) for row in reference["raw"]["top5_restaurants"]]
    same("raw.top5_ordered", actual_top, expected_top)

    actual_categories = index_rows(spark["raw_revenue_category_city"], ("city", "category"), "raw_revenue_category_city")
    expected_categories = index_rows(reference["raw"]["categories_by_city"], ("city", "category"), "raw.categories_by_city")
    same("raw.category_city_keys", sorted(actual_categories), sorted(expected_categories))
    for key in sorted(set(actual_categories) & set(expected_categories)):
        left, right = actual_categories[key], expected_categories[key]
        label = ".".join(key)
        same(f"raw.{label}.revenue_cents", amount_in_cents(left["revenue_mad"]), right["revenue_cents"])
        same(f"raw.{label}.delivered_events", left["delivered_events"], right["delivered_count"])

    environment = spark.get("environment", {})
    if not environment.get("spark_version") or not environment.get("master"):
        errors.append({"check": "spark_environment_present", "actual": environment, "expected": "spark_version et master renseignés"})
    return {"status": "PASSED" if not errors else "FAILED", "checks_count": len(checks), "errors": errors,
            "scope": "Réconciliation métier exacte des KPI bruts et Silver, top 5 brut et catégorie-ville brute; ne remplace pas les captures du cluster ou du DAG.",
            "spark_environment": environment}


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=root / "evidence/results.json")
    parser.add_argument("--reference", type=Path, default=root / "evidence/reference_results.json")
    parser.add_argument("--output", type=Path, default=root / "evidence/verification.json")
    parser.add_argument("--require-spark", action="store_true", help="Échouer si aucune sortie Spark n'existe.")
    args = parser.parse_args()
    try:
        if not args.reference.exists():
            raise FileNotFoundError(f"Référence absente : {args.reference}; lancer scripts/reference_results.py")
        reference = json.loads(args.reference.read_text(encoding="utf-8"))
        if not args.results.exists():
            result = {"status": "SKIPPED", "reason": "results.json absent : aucune exécution Spark à vérifier.",
                      "reference_available": True, "checks_count": 0}
            exit_code = 2 if args.require_spark else 0
        else:
            result = verify(json.loads(args.results.read_text(encoding="utf-8")), reference)
            exit_code = 0 if result["status"] == "PASSED" else 1
    except (OSError, KeyError, TypeError, ValueError, InvalidOperation) as exc:
        result = {"status": "FAILED", "reason": f"{type(exc).__name__}: {exc}", "checks_count": 0}
        exit_code = 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
