from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import configuration


SILVER_IMPRESSIONS = "s3a://clickstream-event-data/delta/silver/impressions"
SILVER_CLICKS      = "s3a://clickstream-event-data/delta/silver/clicks"
SILVER_CONVERSIONS = "s3a://clickstream-event-data/delta/silver/conversions"

GOLD_ATTRIBUTION_PATH     = "s3a://clickstream-event-data/delta/gold/attribution"
GOLD_ATTRIBUTION_CHK      = "s3a://clickstream-event-data/checkpoints/gold/attribution/"


CLICK_WINDOW = "30 minutes"
CONV_WINDOW  = "24 hours"
WATERMARK_DELAY = "26 hours"
TRIGGER = "30 seconds"

def main():
    spark = SparkSession.builder \
            .appName('Gold_attribution') \
            .config('spark.jars.packages', "io.delta:delta-spark_2.12:3.2.0,"
                                           "org.apache.hadoop:hadoop-aws:3.3.1,"
                                           "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
            .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .config("spark.sql.shuffle.partitions", 24) \
            .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    gold_attribution_impressions = spark.readStream.format('delta').load(SILVER_IMPRESSIONS)\
                                   .select(
                                        'user_id',
                                        'campaign_id',
                                        'ad_id',
                                        'join_key',
                                        'location',
                                        'devices',
                                        F.col('event_ts').alias('impression_timestamp'),
                                        F.col('event_id').alias('impression_event_id'),
                                        F.col('cost_micros').cast('long').alias('impression_cost_micros'))\
                                   .filter(F.col('join_key').isNotNull() & F.col('impression_timestamp').isNotNull()) \
                                   .withWatermark('impression_timestamp', WATERMARK_DELAY)

    gold_attribution_clicks = spark.readStream.format('delta').load(SILVER_CLICKS) \
                             .select(
                                    'user_id',
                                    'campaign_id',
                                    'ad_id',
                                    'join_key',
                                    F.col('impression_event_id').alias('impression_event_id_ref'),
                                    F.col('event_ts').alias('click_timestamp'),
                                    F.col('event_id').alias('click_event_id')) \
                            .filter(F.col('join_key').isNotNull() & F.col('click_timestamp').isNotNull()) \
                            .withWatermark('click_timestamp', WATERMARK_DELAY)

    gold_attribution_conversions = spark.readStream.format('delta').load(SILVER_CONVERSIONS) \
                                   .select(
                                        'user_id',
                                        'campaign_id',
                                        'ad_id',
                                        'join_key',
                                        F.col('click_event_id').alias('click_event_id_ref'),
                                        F.col('event_ts').alias('conversion_timestamp'),
                                        F.col('event_id').alias('conversion_event_id'),
                                        F.col('purchase_value').cast('double').alias('purchase_value'),
                                        'currency') \
                                   .filter(F.col('join_key').isNotNull() & F.col('conversion_timestamp').isNotNull()) \
                                   .withWatermark('conversion_timestamp', WATERMARK_DELAY)

    # Impressions to clicks that is time_bound
    impression_click_matches = (gold_attribution_impressions.alias('i')
                                .join(gold_attribution_clicks.alias('c'),
                            on = (
                                  (F.col('i.join_key')==F.col('c.join_key')) &
                                  (F.col('c.click_timestamp') >= F.col('i.impression_timestamp')) &
                                  (F.col('c.click_timestamp') <= F.col('i.impression_timestamp') + F.expr(f'INTERVAL {CLICK_WINDOW}'))
                                ),
                            how='leftOuter'
                        )
                        .select(
                            F.col("i.join_key"),
                            F.col("i.user_id"), F.col("i.campaign_id"), F.col("i.ad_id"),
                            F.col("i.location"), F.col("i.devices"),
                            F.col("i.impression_timestamp"),
                            F.col("c.click_timestamp"),
                            F.col("i.impression_event_id"),
                            F.col("c.click_event_id"),
                            F.col("i.impression_cost_micros"),
                        ))

    # Any impression will have more than one click. Choosing the earliest click per imperssion row
    impression_click = impression_click_matches \
                        .groupBy(
                            'join_key',
                            'user_id',
                            'campaign_id',
                            'ad_id',
                            'location',
                            'devices',
                            'impression_event_id',
                            'impression_timestamp',
                            'impression_cost_micros') \
                        .agg(F.min('click_timestamp').alias('click_timestamp'),
                             F.min('click_event_id').alias('click_event_id'))

    # Impression_clicks to Conversion that is time-bound
    click_conversion_matches = (impression_click.alias('ic')
                               .join(gold_attribution_conversions.alias('cv'),
                                     on=(
                                         (F.col('ic.join_key')==F.col('cv.join_key')) &
                                         (F.col('ic.click_timestamp').isNotNull()) &
                                         (F.col('cv.conversion_timestamp') >= F.col('ic.click_timestamp')) &
                                         (F.col('cv.conversion_timestamp') <= F.col('ic.click_timestamp') + F.expr(f'INTERVAL {CONV_WINDOW}'))
                                     ),
                                     how='leftOuter'
                               )
                               .select(
                                    'ic.*',
                                    F.col('cv.conversion_timestamp').alias('conversion_timestamp'),
                                    F.col('cv.conversion_event_id').alias('conversion_event_id'),
                                    F.col('cv.purchase_value').alias('purchase_value'),
                                    F.col('cv.currency').alias('currency'),) )
    # Create attribution
    attribution = click_conversion_matches \
                    .withColumn('has_click', F.col('click_timestamp').isNotNull()) \
                    .withColumn('has_conversion',F.col('conversion_timestamp').isNotNull()) \
                    .withColumn('purchase_value', F.coalesce(F.col('purchase_value'), F.lit(0.0))) \
                    .withColumn('revenue_on_ad_spend', F.when(F.col('impression_cost_micros')> 0,
                                                              F.col('purchase_value')/(F.col('impression_cost_micros')/F.lit(1_000_000.0))) \
                            .otherwise(F.lit(None))) \
                    .withColumn('event_date', F.to_date('impression_timestamp')) \
                    .select(
                        'join_key',
                        'user_id',
                        'campaign_id',
                        'ad_id',
                        'location',
                        'devices',
                        'impression_event_id',
                        'click_event_id',
                        'conversion_event_id',
                        'impression_cost_micros',
                        'purchase_value',
                        'currency',
                        'has_click',
                        'has_conversion',
                        'revenue_on_ad_spend',
                        'event_date'
                    )

    attribution.writeStream \
        .format('delta') \
        .outputMode('append') \
        .option('checkpointLocation', GOLD_ATTRIBUTION_CHK) \
        .partitionBy('event_date') \
        .trigger(processingTime=TRIGGER) \
        .start(GOLD_ATTRIBUTION_PATH)

    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    main()