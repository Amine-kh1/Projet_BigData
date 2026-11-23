"""Check the actual standalone deployment and save measured evidence for Part 1."""
import argparse
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hold-seconds", type=int, default=0,
                        help="Keep the application UI open for a screenshot.")
    args = parser.parse_args()
    evidence = Path("evidence")
    events = (evidence / "spark-events").resolve()
    events.mkdir(parents=True, exist_ok=True)
    master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    builder = (SparkSession.builder.appName("Partie1-VerificationCluster")
               .master(master)
               .config("spark.cores.max", "4")
               .config("spark.executor.cores", "2")
               .config("spark.executor.memory", "1g")
               .config("spark.eventLog.enabled", "true")
               .config("spark.eventLog.dir", events.as_uri())
               .config("spark.sql.session.timeZone", "UTC"))
    if master.startswith("spark://"):
        builder = (builder.config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "jupyter"))
                   .config("spark.driver.bindAddress", "0.0.0.0"))
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try:
        # An action ensures Executors are actually able to execute Python tasks.
        observed_sum = sc.parallelize(range(1000), 4).sum()
        assert observed_sum == 499500
        result = {
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "master": sc.master,
            "app_name": sc.appName,
            "application_id": sc.applicationId,
            "default_parallelism": sc.defaultParallelism,
            "python_task_sum": observed_sum,
            "spark_ui": sc.uiWebUrl,
        }
        if master.startswith("spark://"):
            with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as response:
                state = json.load(response)
            workers = [worker for worker in state["workers"] if worker["state"] == "ALIVE"]
            result["active_workers"] = len(workers)
            result["workers"] = workers
            if len(workers) != 2:
                raise RuntimeError(f"Expected 2 live Workers, found {len(workers)}")
        client = boto3.client(
            "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("S3_ACCESS_KEY", "miniouser"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY", "miniosecret123"),
            region_name="us-east-1",
        )
        buckets = [bucket["Name"] for bucket in client.list_buckets()["Buckets"]]
        if "lake" not in buckets:
            raise RuntimeError("The lake bucket does not exist")
        result["buckets"] = buckets
        target = evidence / "partie1_cluster.json"
        target.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if args.hold_seconds > 0:
            print(f"Spark UI available for {args.hold_seconds}s at {sc.uiWebUrl}")
            time.sleep(args.hold_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
