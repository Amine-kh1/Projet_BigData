"""Lancement : python scripts/run_batch.py [options]."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analysis import run_analysis


def main():
    parser = argparse.ArgumentParser(description="Parties 2 et 3 : RDD, DataFrames, SQL et export Parquet")
    parser.add_argument("--orders", default="s3a://lake/bronze/history/orders_history.jsonl")
    parser.add_argument("--restaurants", default="s3a://lake/reference/restaurants.json")
    parser.add_argument("--silver", default="s3a://lake/silver/orders_history")
    parser.add_argument("--evidence-dir", default="/workspace/evidence")
    parser.add_argument("--master", default=None, help="Par défaut SPARK_MASTER_URL ou spark://spark-master:7077")
    parser.add_argument("--app-name", default="Jalon1-Orders-Batch")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--hold-seconds", type=int, default=0,
                        help="Maintenir Spark UI ouvert après le calcul (0 par défaut)")
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions doit être >= 1")
    if args.hold_seconds < 0:
        parser.error("--hold-seconds doit être >= 0")
    run_analysis(args)


if __name__ == "__main__":
    main()
