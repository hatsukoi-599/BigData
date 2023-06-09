import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from log_analysis_with_streaming import log_analysis
from tools import parse_log
import time

start_time = time.time()
# Define your Spark Session
spark = SparkSession.builder.getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("ERROR")

# Subscribe to the Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "logs")
    .load()
)

# Parse the logs
logs_df = parse_log(df)
log_analysis(logs_df)
end_time = time.time()
print(end_time - start_time)
