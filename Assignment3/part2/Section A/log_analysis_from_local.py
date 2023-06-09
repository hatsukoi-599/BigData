from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import glob
from pyspark.sql.functions import regexp_extract, date_format, udf
from tools import parse_clf_time
from pyspark.sql import functions as F


def log_analysis():
    sc = SparkContext()
    spark = SparkSession(sc)
    # Loading and Viewing the NASA Log Dataset
    raw_data_files = glob.glob("resourses/*.gz")
    base_df = spark.read.text(raw_data_files)

    # Data Wrangling
    host_pattern = r"(^\S+\.[\S+\.]+\S+)\s"
    ts_pattern = r"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]"
    method_uri_protocol_pattern = r"\"(\S+)\s(\S+)\s*(\S*)\""
    status_pattern = r"\s(\d{3})\s"
    content_size_pattern = r"\s(\d+)$"

    logs_df = base_df.select(
        regexp_extract("value", host_pattern, 1).alias("host"),
        regexp_extract("value", ts_pattern, 1).alias("timestamp"),
        regexp_extract("value", method_uri_protocol_pattern, 1).alias("method"),
        regexp_extract("value", method_uri_protocol_pattern, 2).alias("endpoint"),
        regexp_extract("value", method_uri_protocol_pattern, 3).alias("protocol"),
        regexp_extract("value", status_pattern, 1).cast("integer").alias("status"),
        regexp_extract("value", content_size_pattern, 1)
        .cast("integer")
        .alias("content_size"),
    )

    logs_df = logs_df[logs_df["status"].isNotNull()]
    logs_df = logs_df.na.fill({"content_size": 0})

    # Handling Temporal Fields(Timestamp)
    udf_parse_time = udf(parse_clf_time)
    logs_df = logs_df.select(
        "*", udf_parse_time(logs_df["timestamp"]).cast("timestamp").alias("time")
    ).drop("timestamp")

    logs_df.cache()

    # Section A 3.1

    endpoint_counts_df = (
        logs_df.select(
            logs_df.endpoint, date_format("time", "EEEE").alias("day_of_week")
        )
        .groupBy("day_of_week", "endpoint")
        .count()
    )

    # Find the maximum count for each day of the week
    max_counts_df = endpoint_counts_df.groupBy("day_of_week").agg(
        F.max("count").alias("max_count")
    )

    # Join the 'endpoint_counts_df' and 'max_counts_df' to get the endpoint with the maximum count for each day
    weekday_endpoint_df = (
        max_counts_df.join(
            endpoint_counts_df,
            (endpoint_counts_df["day_of_week"] == max_counts_df["day_of_week"])
            & (endpoint_counts_df["count"] == max_counts_df["max_count"]),
            "inner",
        )
        .drop(endpoint_counts_df["day_of_week"])
        .drop(max_counts_df["max_count"])
    )

    # Show the result
    weekday_endpoint_df.show(100, False)

    # Section A 3.2
    logs_df_404 = logs_df.filter(logs_df.status == 404)
    year_freq = (
        logs_df_404.select(F.year("time").alias("year"))
        .groupBy("year")
        .count()
        .orderBy("count")
        .limit(10)
    )
    year_freq.show(100, False)


log_analysis()
