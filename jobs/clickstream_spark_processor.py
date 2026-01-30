from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date, from_json, lit
from pyspark.sql.types import (StructType, StructField, StringType, LongType, IntegerType, DoubleType, BooleanType)
from config import configuration
from pyspark.sql.streaming import StreamingQueryListener
import json
KAFKA_BOOTSTRAP_SERVERS = 'kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092'
TOPICS = 'impressions,clicks,conversions'

# S3 file path
BRONZE_PATH = "s3a://clickstream-event-data/bronze_data/"
CHECKPOINT_PATH = "s3a://clickstream-event-data/bronze_data/checkpoints/"
# Structured Schema definition - Impressions schema
impression_schema = StructType([
    StructField('event_type', StringType()),
    StructField('event_id', StringType()),
    StructField('event_timestamp', StringType()),
    StructField('event_time_epoch_ms', LongType()),
    StructField('user_id', IntegerType()),
    StructField('campaign_id', IntegerType()),
    StructField('ad_id', IntegerType()),
    StructField('join_key', LongType()),
    StructField('location', StringType()),
    StructField('devices', StringType()),
    StructField('cost_micros', IntegerType()),
    StructField('is_late', BooleanType()),
    StructField('late_by_sec', IntegerType()),
    StructField('is_duplicate', BooleanType()),
])

# Structured Schema definition - clicks schema
clicks_schema = StructType([
    StructField('event_type', StringType()),
    StructField('event_id', StringType()),
    StructField('event_timestamp', StringType()),
    StructField('event_time_epoch_ms', LongType()),
    StructField('user_id', IntegerType()),
    StructField('campaign_id', IntegerType()),
    StructField('ad_id', IntegerType()),
    StructField('join_key', LongType()),
    StructField('impression_event_id', IntegerType()),
    StructField('clicks_per_cost_micros', IntegerType()),
    StructField('is_late', BooleanType()),
    StructField('late_by_sec', IntegerType()),
    StructField('is_duplicate', BooleanType()),
])

# Structured Schema definition - conversions schema
conversions_schema = StructType([
    StructField('event_type', StringType()),
    StructField('event_id', StringType()),
    StructField('event_timestamp', StringType()),
    StructField('event_time_epoch_ms', LongType()),
    StructField('user_id', IntegerType()),
    StructField('campaign_id', IntegerType()),
    StructField('ad_id', IntegerType()),
    StructField('join_key', LongType()),
    StructField('click_event_id', IntegerType()),
    StructField('purchase_value', IntegerType()),
    StructField('currency', StringType()),
    StructField('is_late', BooleanType()),
    StructField('late_by_sec', IntegerType()),
    StructField('is_duplicate', BooleanType()),
])


class QListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"✅ Query started: id={event.id}, name={event.name}")

    def onQueryProgress(self, event):
        p = event.progress
        print("📦 Batch progress:",
              f"batchId={p['batchId']},",
              f"inputRows={p['numInputRows']},",
              f"processedRowsPerSecond={p.get('processedRowsPerSecond')},",
              f"inputRowsPerSecond={p.get('inputRowsPerSecond')}")
        # Optional: show sink + sources
        # print(json.dumps(p, indent=2))

    def onQueryTerminated(self, event):
        print(f"🛑 Query terminated: {event.id}, {event.exception}")


def main():
    spark = SparkSession.builder \
            .appName('Bronze-ClickStream_events') \
            .config("spark.jars.package",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
            .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.sql.shuffle.partitions", 24) \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.streams.addListener(QListener())

    raw_data = spark.readStream.format('kafka') \
               .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
               .option('subscribe', TOPICS) \
               .option('startingOffsets', 'earliest') \
               .option('failOnDataLoss', 'false') \
               .option('maxOffsetsPerTrigger','20000') \
               .load()

    base_events = raw_data.select(
                  col('topic').alias('kafka_topic'),
                  col('partition').alias('kafka_partition'),
                  col('offset').alias('kafka_offset'),
                  col('timestamp').alias('kafka_ingest_ts'),
                  col('key').cast('string').alias('kafka_key'),
                  col('value').cast('string').alias('raw_json'),
    ).withColumn('ingest_date', to_date(col('kafka_ingest_ts')))

    impr_event = base_events.filter(col('kafka_topic')==lit('impressions')) \
                .withColumn("d", from_json(col('raw_json'), impression_schema)) \
                .select('kafka_topic', 'kafka_partition', 'kafka_offset', 'kafka_ingest_ts', 'kafka_key','raw_json', 'ingest_date', col('d.*'))
    click_event =  base_events.filter(col('kafka_topic')==lit('clicks')) \
                  .withColumn("d", from_json(col('raw_json'), clicks_schema)) \
                  .select('kafka_topic', 'kafka_partition', 'kafka_offset', 'kafka_ingest_ts','kafka_key','raw_json', 'ingest_date', col('d.*'))
    conv_event = base_events.filter(col('kafka_topic')==lit('conversions')) \
                .withColumn("d", from_json(col('raw_json'), conversions_schema)) \
                .select('kafka_topic','kafka_partition','kafka_offset','kafka_ingest_ts','kafka_key','raw_json', 'ingest_date', col('d.*'))

    bronze_data = (
        impr_event
        .unionByName(click_event, allowMissingColumns=True)
        .unionByName(conv_event, allowMissingColumns=True)
    )


    querystream = bronze_data.writeStream \
                 .format('parquet') \
                 .outputMode('append') \
                 .option('path', BRONZE_PATH) \
                 .option('checkpointLocation', CHECKPOINT_PATH) \
                 .partitionBy("kafka_topic", 'ingest_date') \
                 .trigger(processingTime="30 seconds") \
                 .start()

    querystream.awaitTermination(60)

if __name__ == '__main__':
    main()