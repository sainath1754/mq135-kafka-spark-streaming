from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    FloatType,
    StringType,
    LongType,
    IntegerType
)

KAFKA_HOST = '172.18.141.41:9092'
TOPIC = 'sensor-data'

OUTPUT_PATH = '/home/sures/output/consumer'

CHECKPOINT_FILE = '/home/sures/output/checkpoint_consumer_file'
CHECKPOINT_CONSOLE = '/home/sures/output/checkpoint_consumer_console'

spark = SparkSession.builder \
    .appName('MQ135 Consumer') \
    .config('spark.sql.shuffle.partitions', '2') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

schema = StructType() \
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
    .load()

parsed_df = raw_df \
    .selectExpr('CAST(value AS STRING) AS value') \
    .select(from_json(col('value'), schema).alias('d')) \
    .select('d.*')

console_query = parsed_df.writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', 'false') \
    .option('checkpointLocation', CHECKPOINT_CONSOLE) \
    .start()

file_query = parsed_df.writeStream \
    .outputMode('append') \
    .format('parquet') \
    .option('path', OUTPUT_PATH) \
    .option('checkpointLocation', CHECKPOINT_FILE) \
    .option('maxRecordsPerFile', 500) \
    .start()

print('MQ135 Consumer running...')

spark.streams.awaitAnyTermination()