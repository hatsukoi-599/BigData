// Import the required libraries
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import java.net.URI;
import java.io.InputStreamReader;

// Main Class
public class WordCount {

    // Mapper Class
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopwords = new HashSet<>();

        // Constructor
        public TokenizerMapper() {
        }

        // Setup method to load stopwords
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopwordsFilePath = conf.get("stopwords.file");
            loadStopwords(context, stopwordsFilePath);
        }

        // Method to load stopwords
        private void loadStopwords(Context context, String stopwordsFilePath) throws IOException {
            Path path = new Path(stopwordsFilePath);
            FileSystem fs = FileSystem.get(context.getConfiguration());

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String stopword;
                while ((stopword = reader.readLine()) != null) {
                    stopwords.add(stopword.trim());
                }
            }
        }

        // Map method to tokenize input and emit word, one pairs, skipping stop words
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();
                if (!stopwords.contains(token) && token.length() > 6) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    // Reducer Class for summing up word counts
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        // Reduce method for summing up word counts
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Reducer Class for selecting top K words
    public static class TopKReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> frequencyMap = new HashMap<>();
        private int K;

        // Setup method to get K value from configuration
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            K = context.getConfiguration().getInt("K", 100);  // default value is 100
        }

        // Reduce method for collecting word counts into frequencyMap
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            frequencyMap.put(key.toString(), sum);
        }

        // Cleanup method for writing top K words to context
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            PriorityQueue<Entry<String, Integer>> pq = new PriorityQueue<>(Map.Entry.comparingByValue());
            for (Entry<String, Integer> entry : frequencyMap.entrySet()) {
                pq.offer(entry);
                if (pq.size() > K) {
                    pq.poll();
                }
            }
            while (!pq.isEmpty()) {
                Entry<String, Integer> entry = pq.poll();
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

     // Main method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Setting stopwords file path and K value
        conf.set("stopwords.file", "hdfs:///stopwords.txt");
        conf.setInt("K", 100);

        // Setting up job configuration
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Setting input and output file paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exiting based on job completion status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}