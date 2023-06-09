from pyspark.sql.functions import (
    from_unixtime,
    unix_timestamp,
    window,
    udf,
)
from tools import parse_clf_time
import pyspark.sql.functions as F


def foreach_batch_function(
    df, epoch_id, folder_name, sort_by=None, ascending=True, limit=None
):
    # Apply sorting if sort_by is specified
    if sort_by is not None:
        df = df.sort(sort_by, ascending=ascending)

    # Apply limit if it's specified
    if limit is not None:
        df = df.limit(limit)

    df.coalesce(1).write.mode("append").parquet(
        f"hdfs://localhost:9000/logs/{folder_name}/{epoch_id}"
    )


def log_analysis(logs_df):
    # Handling nulls in HTTP status
    logs_df = logs_df[logs_df["status"].isNotNull()]
    # Handling nulls in HTTP content size
    logs_df = logs_df.na.fill({"content_size": 0})
    udf_parse_time = udf(parse_clf_time)
    logs_df = logs_df.select(
        "*", udf_parse_time(logs_df["timestamp"]).cast("timestamp").alias("time")
    ).drop("timestamp")

    # Processing time window
    current_timestamp = unix_timestamp()
    logs_df = logs_df.withColumn("processing_time", from_unixtime(current_timestamp))

    # Define window and count occurrences of each status
    status_freq = logs_df.groupBy(
        window(logs_df.processing_time, "10 seconds"), logs_df.status
    ).count()

    host_freq = logs_df.groupBy(
        window(logs_df.processing_time, "10 seconds"), logs_df.host
    ).count()

    paths_df = logs_df.groupBy(
        window(logs_df.processing_time, "10 seconds"), logs_df.endpoint
    ).count()

    not200_df = logs_df.filter(logs_df["status"] != 200)

    error_endpoints_freq_df = not200_df.groupBy(
        window(logs_df.processing_time, "10 seconds"), logs_df.endpoint
    ).count()

    host_day_distinct_df = (
        logs_df.select(logs_df.host, F.dayofmonth("time").alias("day"))
        .dropDuplicates()
        .groupBy("day")
        .count()
    )

    not_found_df = logs_df.filter(logs_df["status"] == 404)

    endpoints_404_count_df = not_found_df.groupBy(
        window(logs_df.processing_time, "10 seconds"), logs_df.endpoint
    ).count()

    hosts_404_count_df = not_found_df.groupBy(
        window(logs_df.processing_time, "10 seconds"), logs_df.host
    ).count()

    errors_by_date_sorted_df = not_found_df.groupBy(
        window(logs_df.processing_time, "10 seconds"), F.dayofmonth("time").alias("day")
    ).count()

    query1 = (
        status_freq.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(df, epoch_id, "status_freq")
        )
        .option("truncate", "false")
        .start()
    )

    query2 = (
        host_freq.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(df, epoch_id, "host_freq")
        )
        .option("truncate", "false")
        .start()
    )

    query3 = (
        paths_df.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(df, epoch_id, "paths")
        )
        .option("truncate", "false")
        .start()
    )

    query4 = (
        error_endpoints_freq_df.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(
                df,
                epoch_id,
                "error_endpoints_freq",
                sort_by="count",
                ascending=False,
                limit=10,
            )
        )
        .option("truncate", "false")
        .start()
    )

    query5 = (
        host_day_distinct_df.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(
                df, epoch_id, "host_day_distinct", sort_by="count", ascending=False
            )
        )
        .option("truncate", "false")
        .start()
    )

    query6 = (
        endpoints_404_count_df.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(
                df,
                epoch_id,
                "endpoints_404_count",
                sort_by="count",
                ascending=False,
                limit=20,
            )
        )
        .option("truncate", "false")
        .start()
    )

    query7 = (
        hosts_404_count_df.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(
                df,
                epoch_id,
                "hosts_404_count",
                sort_by="count",
                ascending=False,
                limit=20,
            )
        )
        .option("truncate", "false")
        .start()
    )

    query8 = (
        errors_by_date_sorted_df.writeStream.outputMode("update")
        .foreachBatch(
            lambda df, epoch_id: foreach_batch_function(
                df, epoch_id, "errors_by_date_sorted", sort_by="day"
            )
        )
        .option("truncate", "false")
        .start()
    )

    query_list = [query1, query2, query3, query4, query5, query6, query7, query8]

    # Wait for all queries to finish
    for query in query_list:
        query.awaitTermination()

    # Unpersist the logs_df DataFrame
    logs_df.unpersist()
