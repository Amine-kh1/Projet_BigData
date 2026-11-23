"""Parties 2 et 3 : traitements exécutables et preuves issues de Spark.

Aucun chiffre de performance ni nombre de stages n'est codé en dur.
"""

import contextlib
import csv
import io
import json
import os
import platform
import statistics
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from operator import add
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

from pyspark.sql import SparkSession, functions as F, types as T

from src.common import (
    ORDER_SCHEMA, RESTAURANT_SCHEMA, distance_band, native_distance_band,
    parse_order, valid_order_condition,
)


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def write_json(path, value):
    Path(path).write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )


def write_csv(path, rows, fields):
    with Path(path).open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def build_spark(args):
    event_location = os.getenv("SPARK_EVENTLOG_DIR", str(Path(args.evidence_dir) / "spark-events"))
    event_dir = Path(url2pathname(urlsplit(event_location).path)) if event_location.startswith("file://") else Path(event_location)
    event_dir.mkdir(parents=True, exist_ok=True)
    master = args.master or os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    builder = (
        SparkSession.builder.master(master).appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.default.parallelism", os.getenv("SPARK_DEFAULT_PARALLELISM", "4"))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_dir.resolve().as_uri())
        .config("spark.ui.retainedJobs", "500")
        .config("spark.ui.retainedStages", "1000")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if not master.startswith("local"):
        builder = (
            builder.config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "jupyter"))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.cores.max", "4")
            .config("spark.executor.cores", "2")
            .config("spark.executor.memory", "1g")
        )
    if args.orders.startswith("s3a://") or args.silver.startswith("s3a://"):
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "miniouser"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "miniosecret123"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class SparkMeasurements:
    """Associe une action à ses vrais jobs et stages via le StatusTracker."""

    def __init__(self, spark):
        self.sc = spark.sparkContext
        self.records = []

    def action(self, label, callback):
        group = f"jalon1-{len(self.records):03d}-{label}"
        self.sc.setJobGroup(group, label)
        started = time.perf_counter()
        try:
            result = callback()
        finally:
            elapsed = time.perf_counter() - started
            tracker = self.sc.statusTracker()
            jobs = sorted(tracker.getJobIdsForGroup(group))
            stage_ids = set()
            for job_id in jobs:
                job = tracker.getJobInfo(job_id)
                if job is not None:
                    stage_ids.update(int(stage_id) for stage_id in job.stageIds)
            stages = []
            for stage_id in sorted(stage_ids):
                stage = tracker.getStageInfo(stage_id)
                stages.append({
                    "stage_id": stage_id,
                    "tasks": stage.numTasks if stage is not None else None,
                    "completed_tasks": stage.numCompletedTasks if stage is not None else None,
                    "failed_tasks": stage.numFailedTasks if stage is not None else None,
                    "name": stage.name if stage is not None else None,
                })
            self.records.append({"label": label, "seconds": elapsed, "job_ids": jobs,
                                 "stage_ids": sorted(stage_ids), "stage_count": len(stage_ids),
                                 "stages": stages})
            self.sc.setLocalProperty("spark.jobGroup.id", None)
            self.sc.setLocalProperty("spark.job.description", None)
        return result


def _rdd_city_stats(delivered):
    return delivered.map(lambda r: (r["city"], (r["amount_cents"], 1))).reduceByKey(
        lambda left, right: (left[0] + right[0], left[1] + right[1])
    )


def _city_rows(pairs):
    return [{"city": city, "delivered_events": count,
             "revenue_mad": Decimal(cents) / 100,
             "average_basket_mad": (Decimal(cents) / (100 * count)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)}
            for city, (cents, count) in sorted(pairs)]


def _capture_explain(df):
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        df.explain(True)
    return stream.getvalue()


def run_analysis(args):
    evidence = Path(args.evidence_dir)
    evidence.mkdir(parents=True, exist_ok=True)
    spark = build_spark(args)
    measurements = SparkMeasurements(spark)
    sc = spark.sparkContext
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        raw_lines = sc.textFile(args.orders)
        parsed = raw_lines.map(parse_order)
        orders_rdd = parsed.filter(lambda pair: pair[0]).map(lambda pair: pair[1])
        delivered_rdd = orders_rdd.filter(lambda row: row["status"] == "DELIVERED")
        revenue_rdd = delivered_rdd.map(lambda row: (row["city"], row["amount_cents"])).reduceByKey(add)
        city_stats_rdd = _rdd_city_stats(delivered_rdd)
        top_rdd = delivered_rdd.map(lambda row: (row["restaurant_id"], 1)).reduceByKey(add)

        line_count = measurements.action("quality_input_lines", raw_lines.count)
        valid_rdd_count = measurements.action("quality_valid_rdd", orders_rdd.count)
        revenue_pairs = measurements.action("rdd_revenue_by_city", revenue_rdd.collect)
        city_pairs = measurements.action("rdd_sum_count_one_pass", city_stats_rdd.collect)
        top_pairs = measurements.action("rdd_top5", lambda: top_rdd.takeOrdered(5, key=lambda x: (-x[1], x[0])))
        city_rows = _city_rows(city_pairs)
        top_rows = [{"restaurant_id": key, "delivered_events": value} for key, value in top_pairs]
        assert dict(revenue_pairs) == {city: value[0] for city, value in city_pairs}

        (evidence / "rdd_lineage.txt").write_text(
            "=== Chiffre d'affaires : map + reduceByKey ===\n"
            + revenue_rdd.toDebugString().decode("utf-8")
            + "\n\n=== Panier moyen : somme et nombre en un seul reduceByKey ===\n"
            + city_stats_rdd.toDebugString().decode("utf-8")
            + "\n\n=== Top 5 : reduceByKey puis takeOrdered ===\n"
            + top_rdd.toDebugString().decode("utf-8") + "\n",
            encoding="utf-8",
        )

        raw_df = spark.read.schema(ORDER_SCHEMA).option("mode", "PERMISSIVE").json(args.orders)
        valid_df = raw_df.filter(valid_order_condition()).drop("_corrupt_record")
        valid_df_count = measurements.action("quality_valid_dataframe", valid_df.count)
        if valid_df_count != valid_rdd_count:
            raise ValueError("Les règles de validation RDD et DataFrame produisent des effectifs différents.")
        delivered_df = valid_df.filter(F.col("status") == "DELIVERED")
        revenue_df = delivered_df.groupBy("city").agg(F.sum("amount_mad").alias("revenue_mad"))
        valid_df.createOrReplaceTempView("orders_history")
        revenue_sql = spark.sql("""
            SELECT city, SUM(amount_mad) AS revenue_mad
            FROM orders_history WHERE status = 'DELIVERED' GROUP BY city
        """)
        df_rows = measurements.action("dataframe_revenue_by_city", revenue_df.collect)
        sql_rows = measurements.action("sql_revenue_by_city", revenue_sql.collect)
        rdd_revenues = {city: Decimal(cents) / 100 for city, cents in revenue_pairs}
        df_revenues = {row.city: row.revenue_mad for row in df_rows}
        sql_revenues = {row.city: row.revenue_mad for row in sql_rows}
        if not rdd_revenues == df_revenues == sql_revenues:
            raise AssertionError("Réconciliation du CA RDD / DataFrame / SQL échouée")

        restaurants = spark.read.schema(RESTAURANT_SCHEMA).json(args.restaurants)
        restaurant_count = measurements.action("quality_restaurants_count", restaurants.count)
        unique_restaurants = measurements.action("quality_restaurants_unique", lambda: restaurants.select("restaurant_id").distinct().count())
        if unique_restaurants != restaurant_count:
            raise ValueError("Le référentiel contient des restaurant_id dupliqués : jointure multiplicative.")
        if measurements.action("quality_restaurants_null", lambda: restaurants.filter(
            F.col("restaurant_id").isNull() | F.col("category").isNull() | F.col("city").isNull()
        ).count()):
            raise ValueError("Le référentiel contient des clés ou catégories nulles.")
        reference = restaurants.select("restaurant_id", F.col("name").alias("restaurant_name"),
                                       F.col("city").alias("restaurant_city"), "category", "latitude", "longitude")
        joined = valid_df.join(F.broadcast(reference), "restaurant_id", "left")
        join_plan = _capture_explain(joined)
        (evidence / "explain_join_extended.txt").write_text(join_plan, encoding="utf-8")
        missing_restaurants = measurements.action("quality_missing_reference", lambda: joined.filter(F.col("category").isNull()).count())
        city_mismatches = measurements.action("quality_city_mismatches", lambda: joined.filter(
            F.col("restaurant_city").isNotNull() & (F.col("city") != F.col("restaurant_city"))
        ).count())
        categories_df = joined.filter(F.col("status") == "DELIVERED").groupBy("city", "category").agg(
            F.sum("amount_mad").alias("revenue_mad"), F.count("*").alias("delivered_events")
        )
        categories = [row.asDict() for row in measurements.action("dataframe_revenue_category_city", categories_df.collect)]
        categories.sort(key=lambda row: (row["city"], row["category"] or ""))

        # Les doublons d'un même changement d'état ne doivent pas gonfler Silver.
        # Une variation de données derrière une même clé est une erreur explicite.
        business_keys = ["order_id", "status"]
        distinct_payloads = valid_df.dropDuplicates(valid_df.columns)
        conflicting_keys = measurements.action("quality_conflicting_duplicates", lambda: distinct_payloads.groupBy(
            *business_keys).count().filter(F.col("count") > 1).count())
        if conflicting_keys:
            raise ValueError(f"{conflicting_keys} clés (order_id,status) possèdent des contenus contradictoires.")
        clean_orders = valid_df.dropDuplicates(business_keys)
        clean_count = measurements.action("quality_clean_count", clean_orders.count)
        silver = clean_orders.join(F.broadcast(reference), "restaurant_id", "left").withColumn(
            "distance_band", native_distance_band())
        measurements.action("write_silver_partitioned", lambda: silver.write.mode("overwrite").partitionBy("city").parquet(args.silver))
        reread_silver = spark.read.parquet(args.silver)
        silver_count = measurements.action("verify_silver_readback", reread_silver.count)
        if silver_count != clean_count:
            raise AssertionError("Le nombre de lignes relues en Parquet est incorrect.")
        clean_city_df = reread_silver.filter(F.col("status") == "DELIVERED").groupBy("city").agg(
            F.sum("amount_mad").alias("revenue_mad"), F.count("*").alias("delivered_events"),
            F.round(F.avg("amount_mad"), 2).alias("average_basket_mad"))
        clean_city_rows = sorted([row.asDict() for row in measurements.action("clean_revenue_by_city", clean_city_df.collect)], key=lambda row: row["city"])
        (evidence / "silver_schema.txt").write_text(reread_silver._jdf.schema().treeString(), encoding="utf-8")
        write_json(evidence / "silver_files.json", sorted(reread_silver.inputFiles()))

        # Même calcul métier et mêmes sources non persistées pour les trois API.
        # Un tour de chauffe par API, puis alternance des mesures pour limiter l'effet d'ordre.
        benchmark_callbacks = {
            "RDD": lambda: delivered_rdd.map(lambda row: (row["city"], row["amount_cents"])).reduceByKey(add).collect(),
            "DataFrame": lambda: delivered_df.groupBy("city").agg(F.sum("amount_mad").alias("revenue_mad")).collect(),
            "SQL": lambda: spark.sql("SELECT city, SUM(amount_mad) AS revenue_mad FROM orders_history WHERE status = 'DELIVERED' GROUP BY city").collect(),
        }
        api_times = {name: [] for name in benchmark_callbacks}
        for name, callback in benchmark_callbacks.items():
            measurements.action(f"benchmark_warmup_{name}", callback)
        api_names = list(benchmark_callbacks)
        for repetition in range(args.repetitions):
            for name in api_names[repetition % 3:] + api_names[:repetition % 3]:
                measurements.action(f"benchmark_{name}_{repetition + 1}", benchmark_callbacks[name])
                api_times[name].append(measurements.records[-1]["seconds"])

        # Ici le cache est commun aux deux variantes et le résultat de distance_band
        # est consommé par groupBy : Catalyst ne peut pas éliminer la projection.
        band_input = valid_df.select("distance_km").cache()
        measurements.action("band_cache_materialization", band_input.count)
        band_udf = F.udf(distance_band, T.StringType())
        udf_query = band_input.withColumn("distance_band", band_udf("distance_km")).groupBy("distance_band").count()
        native_query = band_input.withColumn("distance_band", native_distance_band()).groupBy("distance_band").count()
        (evidence / "explain_distance_udf.txt").write_text(_capture_explain(udf_query), encoding="utf-8")
        (evidence / "explain_distance_native.txt").write_text(_capture_explain(native_query), encoding="utf-8")
        band_callbacks = {"python_udf": udf_query.collect, "native_when": native_query.collect}
        band_counts = {}
        band_times = {name: [] for name in band_callbacks}
        for name, callback in band_callbacks.items():
            band_counts[name] = sorted((row.distance_band or "NULL", row["count"]) for row in measurements.action(f"band_warmup_{name}", callback))
        if band_counts["python_udf"] != band_counts["native_when"]:
            raise AssertionError("Les bandes UDF et natives ne concordent pas.")
        for repetition in range(args.repetitions):
            names = list(band_callbacks)
            if repetition % 2:
                names.reverse()
            for name in names:
                measurements.action(f"band_{name}_{repetition + 1}", band_callbacks[name])
                band_times[name].append(measurements.records[-1]["seconds"])
        band_input.unpersist()

        def summaries(values):
            return {name: {"samples_seconds": samples, "median_seconds": statistics.median(samples)}
                    for name, samples in values.items()}

        result = {
            "executed_at_utc": started_at,
            "environment": {"spark_version": spark.version, "python_version": platform.python_version(),
                            "master": sc.master, "app_name": sc.appName, "application_id": sc.applicationId,
                            "default_parallelism": sc.defaultParallelism, "spark_ui_url": sc.uiWebUrl,
                            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions")},
            "paths": {"orders": args.orders, "restaurants": args.restaurants, "silver": args.silver},
            "quality": {"input_lines": line_count, "valid_rows_rdd": valid_rdd_count,
                        "valid_rows_dataframe": valid_df_count, "rejected_rows": line_count - valid_rdd_count,
                        "restaurant_count": restaurant_count, "missing_restaurant_events": missing_restaurants,
                        "city_mismatch_events": city_mismatches, "conflicting_duplicate_keys": conflicting_keys,
                        "duplicates_removed": valid_df_count - clean_count, "silver_rows": silver_count},
            "raw_city_kpis": city_rows, "raw_top5_restaurants": top_rows,
            "raw_revenue_category_city": categories, "clean_city_kpis": clean_city_rows,
            "reconciliation_rdd_dataframe_sql": True,
            "distance_bands": dict(band_counts["native_when"]),
            "benchmark": {"repetitions": args.repetitions,
                          "method": "Un tour de chauffe puis tours alternés ; agrégats collectés ; sources non persistées ; cache OS/JVM potentiellement chaud. Même CA brut DELIVERED, sérialisation comprise.",
                          "api": summaries(api_times),
                          "distance": summaries(band_times),
                          "distance_method": "Même colonne distance_km persistée et matérialisée ; groupBy(distance_band).count().collect() force le calcul de la bande ; un tour de chauffe puis alternance."},
            "join": {"broadcast_hint": True, "physical_strategy": "BroadcastHashJoin" if "BroadcastHashJoin" in join_plan else "voir explain_join_extended.txt"},
            "actions": measurements.records,
            "limits": ["Jeu synthétique de démonstration : ce benchmark n'établit pas des performances de production.",
                       "Les KPI pédagogiques comptent les événements DELIVERED bruts, doublons compris. Silver déduplique par (order_id,status).",
                       "Le champ tenant_id est absent du schéma du jalon 1 ; ces analyses ne démontrent aucune isolation multi-tenant.",
                       "Un event log et des identifiants de stages ne remplacent pas une capture d'écran de Spark UI."]
        }
        write_json(evidence / "results.json", result)
        write_json(evidence / "stage_metrics.json", measurements.records)
        write_csv(evidence / "kpi_city_raw.csv", city_rows, ["city", "delivered_events", "revenue_mad", "average_basket_mad"])
        write_csv(evidence / "top5_restaurants_raw.csv", top_rows, ["restaurant_id", "delivered_events"])
        write_csv(evidence / "revenue_category_city_raw.csv", categories, ["city", "category", "revenue_mad", "delivered_events"])
        write_csv(evidence / "kpi_city_clean.csv", clean_city_rows, ["city", "delivered_events", "revenue_mad", "average_basket_mad"])
        print(json.dumps({"quality": result["quality"], "raw_city_kpis": city_rows,
                          "raw_top5_restaurants": top_rows, "benchmark": result["benchmark"],
                          "reconciliation_rdd_dataframe_sql": True}, ensure_ascii=False, indent=2, default=_json_default))
        return result
    finally:
        hold_seconds = getattr(args, "hold_seconds", 0)
        if hold_seconds:
            print(f"Spark UI disponible pendant {hold_seconds} s : {sc.uiWebUrl}", flush=True)
            time.sleep(hold_seconds)
        spark.stop()
