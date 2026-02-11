from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from config import configuration

SILVER_IMPRESSIONS = "s3a://clickstream-event-data/delta/silver/impressions"
SILVER_CLICKS      = "s3a://clickstream-event-data/delta/silver/clicks"
SILVER_CONVERSIONS = "s3a://clickstream-event-data/delta/silver/conversions"

GOLD_ATTRIBUTION_PATH = "s3a://clickstream-event-data/delta/gold/attribution"
GOLD_ATTRIBUTION_CHK  = "s3a://clickstream-event-data/checkpoints/gold/attribution/"

CLICK_WINDOW     = "30 minutes"
CONV_WINDOW      = "24 hours"
WATERMARK_DELAY  = "26 hours"     # must be >= click+conv windows if you want late matching
TRIGGER          = "30 seconds"


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Gold_attribution_foreachBatch_merge")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = build_spark()

    # ------------------------------------------------------------
    # 0) foreachBatch UPSERT (MERGE) sink
    # ------------------------------------------------------------
    def upsert_attribution(microbatch_df, batch_id: int):
        if microbatch_df.rdd.isEmpty():
            return

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS delta.`{GOLD_ATTRIBUTION_PATH}` (
              join_key BIGINT,
              user_id INT,
              campaign_id INT,
              ad_id INT,
              location STRING,
              devices STRING,

              impression_timestamp TIMESTAMP,
              click_timestamp TIMESTAMP,
              conversion_timestamp TIMESTAMP,

              impression_event_id STRING,
              click_event_id STRING,
              conversion_event_id STRING,

              impression_cost_micros BIGINT,
              purchase_value DOUBLE,
              currency STRING,

              has_click BOOLEAN,
              has_conversion BOOLEAN,
              revenue_on_ad_spend DOUBLE,
              event_date DATE
            )
            USING DELTA
            PARTITIONED BY (event_date)
            LOCATION '{GOLD_ATTRIBUTION_PATH}'
        """)

        target = DeltaTable.forPath(spark, GOLD_ATTRIBUTION_PATH)

        # One row per impression_event_id
        (target.alias("t")
            .merge(
                microbatch_df.alias("s"),
                "t.impression_event_id = s.impression_event_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # ------------------------------------------------------------
    # 1) Read Silver streams (normalized columns)
    # ------------------------------------------------------------
    impr = (
        spark.readStream.format("delta").load(SILVER_IMPRESSIONS)
        .select(
            F.col("join_key").cast("long").alias("join_key"),
            F.col("user_id").cast("int").alias("user_id"),
            F.col("campaign_id").cast("int").alias("campaign_id"),
            F.col("ad_id").cast("int").alias("ad_id"),
            F.col("location"),
            F.col("devices"),
            F.col("event_ts").alias("impression_timestamp"),
            F.col("event_id").alias("impression_event_id"),
            F.col("cost_micros").cast("long").alias("impression_cost_micros"),
        )
        .filter(F.col("join_key").isNotNull() & F.col("impression_timestamp").isNotNull())
        .withWatermark("impression_timestamp", WATERMARK_DELAY)
    )

    clk = (
        spark.readStream.format("delta").load(SILVER_CLICKS)
        .select(
            F.col("join_key").cast("long").alias("join_key"),
            F.col("event_ts").alias("click_timestamp"),
            F.col("event_id").alias("click_event_id"),
            F.col("impression_event_id").alias("impression_event_id_ref"),
        )
        .filter(F.col("join_key").isNotNull() & F.col("click_timestamp").isNotNull())
        .withWatermark("click_timestamp", WATERMARK_DELAY)
    )

    conv = (
        spark.readStream.format("delta").load(SILVER_CONVERSIONS)
        .select(
            F.col("join_key").cast("long").alias("join_key"),
            F.col("event_ts").alias("conversion_timestamp"),
            F.col("event_id").alias("conversion_event_id"),
            F.col("click_event_id").alias("click_event_id_ref"),
            F.col("purchase_value").cast("double").alias("purchase_value"),
            F.col("currency"),
        )
        .filter(F.col("join_key").isNotNull() & F.col("conversion_timestamp").isNotNull())
        .withWatermark("conversion_timestamp", WATERMARK_DELAY)
    )

    # ------------------------------------------------------------
    # 2) Impression -> Click matching (time-bounded), keep ALL impressions
    # ------------------------------------------------------------
    impr_click_matches = (
        impr.alias("i")
        .join(
            clk.alias("c"),
            on=(
                (F.col("i.join_key") == F.col("c.join_key")) &
                (F.col("c.click_timestamp") >= F.col("i.impression_timestamp")) &
                (F.col("c.click_timestamp") <= (F.col("i.impression_timestamp") + F.expr(f"INTERVAL {CLICK_WINDOW}")))
            ),
            how="leftOuter"
        )
        .select(
            F.col("i.join_key").alias("join_key"),
            F.col("i.user_id").alias("user_id"),
            F.col("i.campaign_id").alias("campaign_id"),
            F.col("i.ad_id").alias("ad_id"),
            F.col("i.location").alias("location"),
            F.col("i.devices").alias("devices"),
            F.col("i.impression_timestamp").alias("impression_timestamp"),
            F.col("i.impression_event_id").alias("impression_event_id"),
            F.col("i.impression_cost_micros").alias("impression_cost_micros"),
            F.col("c.click_timestamp").alias("click_timestamp"),
            F.col("c.click_event_id").alias("click_event_id"),
        )
    )

    # Keep earliest click per impression_event_id
    impr_click = (
        impr_click_matches
        .groupBy(
            "join_key", "user_id", "campaign_id", "ad_id",
            "location", "devices",
            "impression_event_id", "impression_timestamp", "impression_cost_micros"
        )
        .agg(
            F.min("click_timestamp").alias("click_timestamp"),
            F.min("click_event_id").alias("click_event_id"),
        )
    )

    # ------------------------------------------------------------
    # 3) Click -> Conversion matching (time-bounded)
    #    IMPORTANT: filter clicked rows BEFORE join (no isNotNull in join condition)
    # ------------------------------------------------------------
    clicked = (
        impr_click
        .filter(F.col("click_timestamp").isNotNull())
        .withWatermark("click_timestamp", WATERMARK_DELAY)
    )

    no_click = impr_click.filter(F.col("click_timestamp").isNull())

    click_conv_matches = (
        clicked.alias("ic")
        .join(
            conv.alias("cv"),
            on=(
                (F.col("ic.join_key") == F.col("cv.join_key")) &
                (F.col("cv.conversion_timestamp") >= F.col("ic.click_timestamp")) &
                (F.col("cv.conversion_timestamp") <= (F.col("ic.click_timestamp") + F.expr(f"INTERVAL {CONV_WINDOW}")))
            ),
            how="leftOuter"
        )
        .select(
            "ic.*",
            F.col("cv.conversion_timestamp").alias("conversion_timestamp"),
            F.col("cv.conversion_event_id").alias("conversion_event_id"),
            F.col("cv.purchase_value").alias("purchase_value"),
            F.col("cv.currency").alias("currency"),
        )
    )

    # Add back impressions that never had clicks (they can't have conversions)
    no_click_padded = (
        no_click
        .withColumn("conversion_timestamp", F.lit(None).cast("timestamp"))
        .withColumn("conversion_event_id", F.lit(None).cast("string"))
        .withColumn("purchase_value", F.lit(None).cast("double"))
        .withColumn("currency", F.lit(None).cast("string"))
    )

    base = click_conv_matches.unionByName(no_click_padded, allowMissingColumns=True)

    # If multiple conversions exist, keep earliest conversion per impression
    base = (
        base.groupBy(
            "join_key", "user_id", "campaign_id", "ad_id",
            "location", "devices",
            "impression_event_id", "impression_timestamp", "impression_cost_micros",
            "click_timestamp", "click_event_id"
        )
        .agg(
            F.min("conversion_timestamp").alias("conversion_timestamp"),
            F.min("conversion_event_id").alias("conversion_event_id"),
            F.first("purchase_value", ignorenulls=True).alias("purchase_value"),
            F.first("currency", ignorenulls=True).alias("currency"),
        )
    )

    # ------------------------------------------------------------
    # 4) Final attribution row (one row per impression_event_id)
    # ------------------------------------------------------------
    attribution = (
        base
        .withColumn("has_click", F.col("click_timestamp").isNotNull())
        .withColumn("has_conversion", F.col("conversion_timestamp").isNotNull())
        .withColumn("purchase_value", F.coalesce(F.col("purchase_value"), F.lit(0.0)))
        .withColumn(
            "revenue_on_ad_spend",
            F.when(
                F.col("impression_cost_micros") > 0,
                F.col("purchase_value") / (F.col("impression_cost_micros") / F.lit(1_000_000.0))
            ).otherwise(F.lit(None))
        )
        .withColumn("event_date", F.to_date("impression_timestamp"))
        .select(
            "join_key",
            "user_id", "campaign_id", "ad_id",
            "location", "devices",
            "impression_timestamp", "click_timestamp", "conversion_timestamp",
            "impression_event_id", "click_event_id", "conversion_event_id",
            "impression_cost_micros",
            "purchase_value", "currency",
            "has_click", "has_conversion",
            "revenue_on_ad_spend",
            "event_date",
        )
    )

    # ------------------------------------------------------------
    # 5) Write via foreachBatch MERGE
    # ------------------------------------------------------------
    (
        attribution.writeStream
        .foreachBatch(upsert_attribution)
        .outputMode("append")
        .option("checkpointLocation", GOLD_ATTRIBUTION_CHK)
        .trigger(processingTime=TRIGGER)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()