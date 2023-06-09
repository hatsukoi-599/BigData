import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, explode, split, length, col, concat_ws

spark = SparkSession.builder \
    .appName("WordCountAnalysis") \
    .getOrCreate()

# Starting Time
startTime = time.time()

dataset = spark.read.text("/Users/shweta/Documents/COEN242-BigData/Assignment3/Part1/dataset/data_2.5GB.txt")
# lowercase_dataset = dataset.withColumn("value", dataset["value"].cast("string").lower())
lowercase_dataset = dataset.withColumn("value", lower(dataset["value"].cast("string")))

stopwords_file = "/Users/shweta/Documents/COEN242-BigData/Assignment3/Part1/dataset/stopwords.txt"

with open(stopwords_file, "r") as file:
    stopwords = [word.strip() for word in file.readlines()]

splitted_words = lowercase_dataset.select(split(lowercase_dataset.value, " ").alias("words"))
exploded_words = splitted_words.select(explode(splitted_words.words).alias("word"))
filtered_words = exploded_words.filter(~exploded_words.word.isin(stopwords) & (length(exploded_words.word) != 0) & (length(exploded_words.word) > 6))

word_counts = filtered_words.groupBy("word").count()

sorted_word_counts = word_counts.orderBy(word_counts["count"].desc())

top_100_words = sorted_word_counts.limit(100)

top_100_words.show()

output_file_path = "/Users/shweta/Documents/COEN242-BigData/Assignment3/Part1/Output/2.5GB/2"

# Convert the DataFrame into a single-column DataFrame with desired format
output_data = top_100_words.withColumn("output", concat_ws(": ", col("word"), col("count"))).select("output")

# Write the output text file
output_data.write.mode("overwrite").text(output_file_path)

# Execution Time calculation
print("Total Time of Execution : ",time.time() - startTime)

spark.stop()
