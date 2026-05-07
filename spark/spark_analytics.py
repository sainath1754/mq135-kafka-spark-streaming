from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    FloatType,
    StringType,
    LongType,
    IntegerType
)

KAFKA_HOST = '172.18.141.41:9092'
TOPIC = 'sensor-data'

ALERTS_PATH = '/home/sures/output/alerts'
WINDOW_PATH = '/home/sures/output/window'

ALERTS_CKPT = '/home/sures/output/checkpoint_alerts'
WINDOW_CKPT = '/home/sures/output/checkpoint_window'

spark = SparkSession.builder \
    .appName('IoT Analytics FINAL') \
    .config('spark.sql.shuffle.partitions', '2') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

schema = StructType() \
    .add('raw_dist', FloatType()) \
    .add('avg_dist', FloatType()) \
    .add('dist_status', StringType()) \
    .add('raw_co2', FloatType()) \
    .add('avg_co2', FloatType()) \
    .add('co2_status', StringType()) \
    .add('warmup_left', IntegerType()) \
    .add('timestamp', LongType())

raw_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_HOST) \
    .option('subscribe', TOPIC) \
    .option('startingOffsets', 'latest') \
    .option('failOnDataLoss', 'false') \
    .load()

parsed_df = raw_df \
    .selectExpr('CAST(value AS STRING) AS value') \
    .select(from_json(col('value'), schema).alias('d')) \
    .select('d.*')

alerts_df = parsed_df

alerts_console = alerts_df.writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', 'false') \
    .queryName('alerts_console') \
    .start()

alerts_file = alerts_df.writeStream \
    .outputMode('append') \
    .format('parquet') \
    .option('path', ALERTS_PATH) \
    .option('checkpointLocation', ALERTS_CKPT) \
    .option('maxRecordsPerFile', 500) \
    .queryName('alerts_file') \
    .start()

window_df = parsed_df \
    .withColumn('event_time', current_timestamp()) \
    .withWatermark('event_time', '10 seconds') \
    .groupBy(window(col('event_time'), '10 seconds')) \
    .agg(
        avg('raw_dist').alias('window_avg_dist'),
        avg('raw_co2').alias('window_avg_co2')
    )

window_console = window_df.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', 'false') \
    .queryName('window_console') \
    .start()

window_file = window_df.writeStream \
    .outputMode('append') \
    .format('parquet') \
    .option('path', WINDOW_PATH) \
    .option('checkpointLocation', WINDOW_CKPT) \
    .option('maxRecordsPerFile', 500) \
    .queryName('window_file') \
    .start()

print('Analytics running. Ctrl+C to stop.')

spark.streams.awaitAnyTermination()