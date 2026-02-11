from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import configuration
from delta.tables import DeltaTable

SILVER_IMPRESSIONS = "s3a://clickstream-event-data/delta/silver/impressions"
SILVER_CLICKS      = "s3a://clickstream-event-data/delta/silver/clicks"
SILVER_CONVERSIONS = "s3a://clickstream-event-data/delta/silver/conversions"

GOLD_KPIS_PATH     = "s3a://clickstream-event-data/delta/gold/kpis_hourly"
GOLD_CHK_KPIS      = "s3a://clickstream-event-data/checkpoints/gold/kpis_hourly/"

WATERMARK_DELAY = "2 hours"
TRIGGER = "30 seconds"


def main():
    spark = SparkSession.builder \
            .appName('Gold_kpis_hourly') \
            .config('spark.jars.packages',"io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
           .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
           .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
           .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
           .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
           .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
           .config("spark.sql.shuffle.partitions", 24) \
           .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    def upsert_kpis(microbatch_df, batch_id):
        spark.sql(f"""
                CREATE TABLE IF NOT EXISTS delta.`{GOLD_KPIS_PATH}` (
                  start_window TIMESTAMP,
                  end_window TIMESTAMP,
                  campaign_id INT,
                  ad_id INT,
                  total_active_users BIGINT,
                  impressions BIGINT,
                  clicks BIGINT,
                  conversions BIGINT,
                  spend_micros BIGINT,
                  amount_spent DOUBLE,
                  total_revenue DOUBLE,
                  click_thru_rate DOUBLE,
                  conversion_rate DOUBLE,
                  revenue_on_ad_spend DOUBLE,
                  event_date DATE
                )
                USING DELTA
                PARTITIONED BY (event_date)
                LOCATION '{GOLD_KPIS_PATH}'
            """)

        target = DeltaTable.forPath(spark, GOLD_KPIS_PATH)

        (
            target.alias("t")
            .merge(
                microbatch_df.alias("s"),
                """
                t.start_window = s.start_window AND
                t.end_window   = s.end_window   AND
                t.campaign_id  = s.campaign_id  AND
                t.ad_id        = s.ad_id
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


    gold_kpi_impressions = spark.readStream.format('delta').load(SILVER_IMPRESSIONS) \
                      .select(
                            F.col('event_ts').alias('event_ts'),
                            F.col('user_id').cast('int').alias('user_id'),
                            F.col('campaign_id').cast('int').alias('campaign_id'),
                            F.col('ad_id').cast('int').alias('ad_id'),
                            F.col('cost_micros').cast('long').alias('cost_micros'),)\
                      .withColumn('impressions', F.lit(1)) \
                      .withColumn('clicks', F.lit(0)) \
                      .withColumn('conversions', F.lit(0)) \
                      .withColumn('revenue', F.lit(0.0)) \
                      .withWatermark('event_ts', WATERMARK_DELAY)

    gold_kpi_clicks = spark.readStream.format('delta').load(SILVER_CLICKS) \
                  .select(
                        F.col('event_ts').alias('event_ts'),
                        F.col('user_id').cast('int').alias('user_id'),
                        F.col('campaign_id').cast('int').alias('campaign_id'),
                        F.col('ad_id').cast('int').alias('ad_id'),) \
                  .withColumn('impressions', F.lit(0)) \
                  .withColumn('clicks', F.lit(1)) \
                  .withColumn('conversions', F.lit(0)) \
                  .withColumn('cost_micros', F.lit(0).cast('long')) \
                  .withColumn('revenue', F.lit(0.0)) \
                  .withWatermark('event_ts', WATERMARK_DELAY)

    gold_kpi_conversions = spark.readStream.format('delta').load(SILVER_CONVERSIONS) \
                       .select(
                            F.col('event_ts').alias('event_ts'),
                            F.col('user_id').cast('int').alias('user_id'),
                            F.col('campaign_id').cast('int').alias('campaign_id'),
                            F.col('ad_id').cast('int').alias('ad_id'),
                            F.col('purchase_value').cast('double').alias('revenue'),) \
                       .withColumn('impressions', F.lit(0)) \
                       .withColumn('clicks', F.lit(0)) \
                       .withColumn('conversions', F.lit(1)) \
                       .withColumn('cost_micros', F.lit(0).cast('long')) \
                       .withWatermark('event_ts', WATERMARK_DELAY)

    gold_kpi_base_df = gold_kpi_impressions.unionByName(gold_kpi_clicks).unionByName(gold_kpi_conversions)
    # Calculating KPI's

    gold_kpi_df = gold_kpi_base_df.groupBy(F.window('event_ts', '1 hour').alias('window'), 'campaign_id', 'ad_id') \
                    .agg(
                        F.sum('impressions').alias('impressions'),
                        F.sum('clicks').alias('clicks'),
                        F.sum('conversions').alias('conversions'),
                        ##F.countDistinct('user_id').alias('total_active_users'),
                        F.approx_count_distinct('user_id', 0.03).alias('total_active_users'),
                        F.sum('cost_micros').alias('spend_micros'),
                        F.sum('revenue').alias('total_revenue'),
                    ) \
                    .withColumn('amount_spent', F.col('spend_micros')/F.lit(1_000_000.0)) \
                    .withColumn('click_thru_rate',
                                F.when(F.col('impressions') >0 , F.col('clicks')/F.col('impressions')).otherwise(F.lit(0.0)),
                                ) \
                    .withColumn('conversion_rate',
                                F.when(F.col('clicks')>0, F.col('conversions')/F.col('clicks')).otherwise(F.lit(0.0)),
                                ) \
                    .withColumn('revenue_on_ad_spend',
                                F.when(F.col('amount_spent')>0, F.col('total_revenue')/F.col('amount_spent')).otherwise(F.lit(0.0)),
                                ) \
                    .select(
                        F.col('window.start').alias('start_window'),
                        F.col('window.end').alias('end_window'),
                        'campaign_id',
                        'ad_id',
                        'total_active_users',
                        'impressions',
                        'clicks',
                        'conversions',
                        'spend_micros',
                        'amount_spent',
                        'total_revenue',
                        'click_thru_rate',
                        'conversion_rate',
                        'revenue_on_ad_spend',
                    ) \
                    .withColumn('event_date', F.to_date('start_window'))


    gold_kpi_df.writeStream.foreachBatch(upsert_kpis) \
                .outputMode('update') \
                .option('checkpointLocation', GOLD_CHK_KPIS) \
                .trigger(processingTime=TRIGGER) \
                .start()

    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    main()