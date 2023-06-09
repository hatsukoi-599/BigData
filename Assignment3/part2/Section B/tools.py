import pyarrow.parquet as pq
import pandas as pd
from pyspark.sql.functions import regexp_extract

month_map = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


def parse_clf_time(text):
    """Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
        int(text[7:11]),
        month_map[text[3:6]],
        int(text[0:2]),
        int(text[12:14]),
        int(text[15:17]),
        int(text[18:20]),
    )


def parse_log(df):
    # Regular expressions for log parsing
    host_pattern = r"(^\S+\.[\S+\.]+\S+)\s"
    ts_pattern = r"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})]"
    method_uri_protocol_pattern = r"\"(\S+)\s(\S+)\s*(\S*)\""
    status_pattern = r"\s(\d{3})\s"
    content_size_pattern = r"\s(\d+)$"

    return df.selectExpr("CAST(value AS STRING)").select(
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


def read_pq(path):
    table = pq.read_table(path)
    pd.set_option("display.max_colwidth", -1)
    df = table.to_pandas()
    print(df)
