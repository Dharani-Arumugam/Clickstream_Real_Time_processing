from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import configuration

BRONZE_ROOT = "s3a://clickstream-event-data/bronze"
BRONZE_GLOB = f"{BRONZE_ROOT}/kafka_topic=*/ingest_date=*"

SILVER_IMPRESSIONS = "s3a://clickstream-event-data/delta/silver/impressions"
SILVER_CLICKS      = "s3a://clickstream-event-data/delta/silver/clicks"
SILVER_CONVERSIONS = "s3a://clickstream-event-data/delta/silver/conversions"

CHK_IMPRESSIONS = "s3a://clickstream-event-data/checkpoints/silver/impressions/"
CHK_CLICKS      = "s3a://clickstream-event-data/checkpoints/silver/clicks/"
CHK_CONVERSIONS = "s3a://clickstream-event-data/checkpoints/silver/conversions/"


WATERMARK_DELAY = "2 hours"
TRIGGER = "30 seconds"

def start_streaming_topic(topic_df, output_path, checkpoint_path):
    return (
        topic_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER)
        .start(output_path)
    )

def add_event_timestamp(df):

    ts_from_string = F.to_timestamp(F.col('event_timestamp'))
    ts_from_epoch = (F.col('event_time_epoch_ms')/ F.lit(1000)).cast('timestamp')
    # If event_timestamp parses OK → use it, else if event_time_epoch_ms exists → use it, else fallback to Kafka’s ingestion timestamp (kafka_ingest_ts)
    # We don’t want rows with null time because watermarking/windows require a timestamp. Using Kafka ingest time is a reasonable last option.
    return(
        df.withColumn('event_ts', F.coalesce(ts_from_string, ts_from_epoch, F.col('kafka_ingest_ts')))
        .withColumn('event_date', F.to_date(F.col('event_ts')))
        .withColumn('event_hour', F.date_trunc('hour', F.col('event_ts')))
    )


def silver_transform(df):
    return (
       df.filter((F.col('is_duplicate') == F.lit(False)) | (F.col('is_duplicate').isNull()))
         .transform(add_event_timestamp)
         .filter(F.col('event_ts').isNotNull())
         .filter(F.col('event_id').isNotNull())
         .withWatermark('event_ts', WATERMARK_DELAY)
         .dropDuplicates(['event_id'])
    )
def main():
    spark = SparkSession.builder \
           .appName("SilverParquetToDelta") \
           .config("spark.jars.packages",
                   "io.delta:delta-spark_2.12:3.2.0,"
                   "org.apache.hadoop:hadoop-aws:3.3.1") \
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

    # Read Bronze data for schema reference (batch read)
    bronze_schema = (
        spark.read
        .option("basePath", BRONZE_ROOT)
        .parquet(BRONZE_GLOB)
        .schema
    )

    # Read Bronze as file stream (streaming read)
    bronze_stream = (
        spark.readStream
        .schema(bronze_schema)
        .option("basePath", BRONZE_ROOT)
        .parquet(BRONZE_GLOB)
    )

    # silver base
    silver_base = silver_transform(bronze_stream)


    # Split into 3 separate silvers
    impressions = silver_base.filter(F.col('kafka_topic')== F.lit('impressions'))
    clicks      = silver_base.filter(F.col('kafka_topic')== F.lit('clicks'))
    conversions = silver_base.filter(F.col('kafka_topic')== F.lit('conversions'))

    q1 = start_streaming_topic(impressions, SILVER_IMPRESSIONS, CHK_IMPRESSIONS)
    q2 = start_streaming_topic(clicks, SILVER_CLICKS, CHK_CLICKS)
    q3 = start_streaming_topic(conversions, SILVER_CONVERSIONS, CHK_CONVERSIONS)

    #Keep all 3 running
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()