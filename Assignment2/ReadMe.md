#  Word Count with Stopwords Filtering and Top K

This is a Hadoop MapReduce application written in Java. The program performs a word count on a given text input, filters out common stop words, and then returns the top K frequently occurring words.

## Features

- **Word Count:** The program counts the frequency of each word in the input text file(s).
- **Stopwords Filtering:** The program filters out common stopwords, such as 'the', 'is', 'at', 'which', etc. These stopwords are loaded from a given file in HDFS.
- **Top K Frequent Words:** The program also identifies the top K frequently occurring words after filtering out the stopwords. The value of K is configurable. (Default value is 100)

## Experiment Environment

- **Hardware:** MacBook M1 Pro, 8-core processor, 512GB disk space, 16GB memory.
- **Software:** Hadoop 3.3.4, AdoptOpenJDK 8.

## How to Run

This program can be run directly using a pre-compiled JAR file, or you can compile the Java code yourself.

### Using Pre-compiled JAR

```bash
bashCopy code
$ hadoop fs -mkdir /user
$ hadoop fs -mkdir /user/username
$ hadoop fs -mkdir /user/username/input
$ hadoop fs -put /path/to/your/test.txt /user/username/input
$ hadoop fs -put /path/to/your/stopwords.txt /
$ hadoop jar /path/to/your/jarfile wordcount /user/username/input /user/username/output
```

- `/path/to/your/test.txt` is the path to your input text file in your local file system.
- `/path/to/your/jarfile` is the path to the pre-compiled JAR file in your local file system.

### Compiling Java Code

1. **Compile the Java code**

```bash
bashCopy code
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
$ hadoop com.sun.tools.javac.Main WordCount.java
$ jar cf wc.jar WordCount*.class
```

2. **Run the Hadoop job**

```
bashCopy code
$ hadoop jar wc.jar WordCount /input/path /output/path
```

- `/input/path` is the path to the input text file(s) in the HDFS.
- `/output/path` is the path where the output will be written in the HDFS.

3. **Configuration**

You can configure the stopwords file path and the value of K in the `WordCount.java` file:

```
javaCopy code
conf.set("stopwords.file", "hdfs:///stopwords.txt");
conf.setInt("K", 100);
```

- `hdfs:///stopwords.txt` is the path to the stopwords file in HDFS.
- `100` is the value of K, i.e., the program will return the top 100 frequently occurring words. You can set this to any value you like.

## Output

The output of the program will be written to the specified HDFS output path. The output will be a list of words and their corresponding frequencies, sorted by frequency in descending order.

## Note

Please note that the stopwords file must be in HDFS. The stopwords file should contain one stopword per line.

## Reference

[MapReduce Tutorial]:https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

