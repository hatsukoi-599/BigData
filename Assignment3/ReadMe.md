# Big Data Processing with Spark and Kafka

- The aim of this project is to process and analyze log files by orchestrating a workflow that leverages Kafka, Spark Streaming, Parquet files, and the HDFS file system. We establish a pipeline that starts with a Kafka producer, followed by a Kafka consumer that employs Spark for data transformation. The transformed data is then converted into Parquet file format and stored in HDFS. Additionally, we provide scripts for local log analysis and word count analysis.

  ## Project Workflow:

  1. Log File -> Kafka (Producer)
  2. Kafka (Consumer which performs transformation using Spark)
  3. Create Parquet files
  4. Store the Parquet file in HDFS

  ## Environment:

  This project has been tested in the following environment:

  - Kafka 3.4.0
  - Spark 3.4.0
  - HDFS (Hadoop 3.3.4)

  ## How to Run

  ### Pre-requisites:

  Ensure you have the following installed and properly configured:

  - Kafka
  - PySpark
  - HDFS

  ### Steps:

  1. Start your Kafka server.

  2. Run `producer.py`: It reads log files line by line and publishes data to a Kafka topic named "logs". Make sure to provide the **correct log file path inside the script (you need to specify this on line 7)**.

     ```bash
     python producer.py
     ```

  3. Run `consumer.py`: This script reads data from the Kafka topic "logs", transforms the log data using PySpark, analyzes the logs and saves the processing time.

     ```bash
     spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 consumer.py
     ```

  The log analysis results are saved in Parquet file format to HDFS.

  1. Run `log_analysis_from_local.py` to analyze the log data directly from local files:

     ```
     python log_analysis_from_local.py
     ```

  **This script reads log files from a specified path (you need to specify this on line 13)**, performs data wrangling, and conducts exploratory data analysis. The script displays output for each day of the week, the endpoints with the highest counts, and yearly 404 status frequency.

  1. Run `word_count.py` to perform word count analysis on a text file:

     ```bash
     python word_count.py
     ```

  **This script reads a text file, removes stopwords, counts the frequency of each word, and writes the results to a specified output file. Please specify the file paths (for the input data, stopwords file, and output data) according to your data and desired output location.**

  ### Dataset:

  The dataset used for this project contains information similar to the NASA dataset mentioned in the assignment. The dataset includes a small version for testing purposes and a large version for the actual data processing. Access the dataset via the provided Google Drive link.

  https://drive.google.com/drive/folders/17D0wSo5qGNr8XA5753vF2BxsCjt-kPSm?usp=sharing

  ## Files

  - `producer.py` : Contains Kafka producer which reads log files and publishes data to a Kafka topic.
  - `consumer.py` : Contains Kafka consumer which reads data from Kafka topic, transforms and analyzes it using PySpark.
  - `log_analysis_with_streaming.py` : Contains functions for analyzing log data using PySpark streaming.
  - `log_analysis_from_local.py` : A standalone script to analyze log data directly from local files.
  - `word_count.py` : A standalone script to perform word count analysis on a text file.
  - `tools.py` : Contains utility functions for parsing logs and converting time formats.