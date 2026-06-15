from __future__ import annotations

import argparse
import sys
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, from_json, to_timestamp
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--checkpoint", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(f"Food events reader: topic={args.topic}, output={args.output}", flush=True)
    spark = SparkSession.builder.appName("food-events-kafka-flatten").getOrCreate()

    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("observed_at", StringType(), True),
            StructField(
                "market",
                StructType(
                    [
                        StructField("market_id", IntegerType(), True),
                        StructField("country_iso3", StringType(), True),
                        StructField("admin1", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("geo", StructType([StructField("lat", DoubleType(), True), StructField("lon", DoubleType(), True)]), True),
                    ]
                ),
                True,
            ),
            StructField(
                "commodity",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("category", StringType(), True),
                        StructField("unit", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "price",
                StructType(
                    [
                        StructField("local_currency", StringType(), True),
                        StructField("local_value", DoubleType(), True),
                        StructField("usd_value", DoubleType(), True),
                        StructField("price_type", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "supply",
                StructType(
                    [
                        StructField("stock_level_days", IntegerType(), True),
                        StructField("shipment_tons", DoubleType(), True),
                        StructField("supplier_count", IntegerType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "quality_checks",
                ArrayType(StructType([StructField("type", StringType(), True), StructField("status", StringType(), True)])),
                True,
            ),
            StructField("risk", StructType([StructField("level", StringType(), True), StructField("reason", StringType(), True)]), True),
        ]
    )

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{args.username}" '
            f'password="{args.password}";',
        )
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = source.select(from_json(col("value").cast("string"), schema).alias("event"))
    flat = (
        parsed.select(
            col("event.event_id").alias("event_id"),
            col("event.event_type").alias("event_type"),
            to_timestamp(col("event.observed_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("observed_at"),
            col("event.market.market_id").alias("market_id"),
            col("event.market.country_iso3").alias("country_iso3"),
            col("event.market.admin1").alias("admin1"),
            col("event.market.name").alias("market_name"),
            col("event.market.geo.lat").alias("latitude"),
            col("event.market.geo.lon").alias("longitude"),
            col("event.commodity.name").alias("commodity"),
            col("event.commodity.category").alias("category"),
            col("event.commodity.unit").alias("unit"),
            col("event.price.local_currency").alias("local_currency"),
            col("event.price.local_value").alias("local_value"),
            col("event.price.usd_value").alias("usd_value"),
            col("event.price.price_type").alias("price_type"),
            col("event.supply.stock_level_days").alias("stock_level_days"),
            col("event.supply.shipment_tons").alias("shipment_tons"),
            col("event.supply.supplier_count").alias("supplier_count"),
            explode_outer(col("event.quality_checks")).alias("quality_check"),
            col("event.risk.level").alias("risk_level"),
            col("event.risk.reason").alias("risk_reason"),
        )
        .select(
            "event_id",
            "event_type",
            "observed_at",
            "market_id",
            "country_iso3",
            "admin1",
            "market_name",
            "latitude",
            "longitude",
            "commodity",
            "category",
            "unit",
            "local_currency",
            "local_value",
            "usd_value",
            "price_type",
            "stock_level_days",
            "shipment_tons",
            "supplier_count",
            col("quality_check.type").alias("quality_check_type"),
            col("quality_check.status").alias("quality_check_status"),
            "risk_level",
            "risk_reason",
        )
    )

    query = (
        flat.writeStream.trigger(once=True)
        .format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        raise
