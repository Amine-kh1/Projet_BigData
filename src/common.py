"""Schémas et règles métier partagés par les deux API Spark."""

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql import types as T

STATUSES = {"CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"}

ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("restaurant_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("amount_mad", T.DecimalType(12, 2)),
    T.StructField("items_count", T.IntegerType()),
    T.StructField("distance_km", T.DoubleType()),
    T.StructField("courier_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("_corrupt_record", T.StringType()),
])

RESTAURANT_SCHEMA = T.StructType([
    T.StructField("restaurant_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
])


def parse_order(line):
    """Retourne (valide, objet), sans perdre silencieusement les rejets.

    Le montant est exprimé en centimes pour une addition exacte dans les RDD.
    Les clés métier restent (order_id, status), comme dans le sujet.
    """
    try:
        row = json.loads(line, parse_float=Decimal)
        if not isinstance(row, dict):
            raise ValueError("objet JSON attendu")
        for key in ("order_id", "restaurant_id", "customer_id", "city", "courier_id"):
            if not isinstance(row.get(key), str) or not row[key].strip():
                raise ValueError(f"champ manquant : {key}")
        if row.get("status") not in STATUSES:
            raise ValueError("statut inconnu")
        amount = Decimal(str(row["amount_mad"]))
        if not amount.is_finite() or amount < 0 or amount >= Decimal("10000000000"):
            raise ValueError("montant invalide")
        if amount != amount.quantize(Decimal("0.01")):
            raise ValueError("plus de deux décimales")
        items = row.get("items_count")
        if isinstance(items, bool) or not isinstance(items, int) or items <= 0:
            raise ValueError("nombre d'articles invalide")
        distance = Decimal(str(row["distance_km"]))
        if not distance.is_finite() or distance < 0:
            raise ValueError("distance invalide")
        datetime.fromisoformat(row["event_time"].replace("Z", "+00:00"))
        row["amount_cents"] = int(amount * 100)
        return True, row
    except (ValueError, KeyError, TypeError, InvalidOperation, AttributeError) as exc:
        return False, str(exc)


def valid_order_condition():
    required = F.lit(True)
    for key in ("order_id", "restaurant_id", "customer_id", "city", "courier_id"):
        required = required & F.col(key).isNotNull() & (F.length(F.trim(F.col(key))) > 0)
    return (
        required
        & F.col("_corrupt_record").isNull()
        & F.col("status").isin(sorted(STATUSES))
        & F.col("amount_mad").isNotNull() & (F.col("amount_mad") >= 0)
        & F.col("items_count").isNotNull() & (F.col("items_count") > 0)
        & F.col("distance_km").isNotNull() & ~F.isnan("distance_km")
        & (F.col("distance_km") >= 0)
        & (F.col("distance_km") < float("inf"))
        & F.col("event_time").isNotNull()
    )


def distance_band(distance_km):
    """Bornes : 2 et 5 appartiennent à la bande moyenne."""
    if distance_km is None or distance_km != distance_km or distance_km < 0:
        return None
    if distance_km < 2:
        return "court"
    if distance_km <= 5:
        return "moyen"
    return "long"


def native_distance_band(column="distance_km"):
    distance = F.col(column)
    return (
        F.when(distance.isNull() | F.isnan(distance) | (distance < 0), F.lit(None).cast("string"))
        .when(distance < 2, "court")
        .when(distance <= 5, "moyen")
        .otherwise("long")
    )
