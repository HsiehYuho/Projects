package test.edu.upenn.cis455;

import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import edu.upenn.cis455.WholeFileInputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {

            // key: filename, value: bytes read from s3
            System.out.println("key: " + key);
            //System.out.println("value: " + new String(value.getBytes()));

            word.set(value.getBytes());
            context.write(word, one);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reducer received key" + key);
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        //BasicConfigurator.configure();

        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials(); // get amazon credentials from ~/.aws

        Configuration conf = new Configuration();
        // set credentials within conf
        //conf.set("fs.s3n.awsAccessKeyId", "AKIAIQK6PLHMYED5SNVA");
        //conf.set("fs.s3n.awsSecretAccessKey", "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");

        Job job = new Job(conf, "word count");

        // set jar
        job.setJarByClass(WordCount.class);

        // set mapper
        job.setMapperClass(TokenizerMapper.class);

        // set combiner
        //job.setCombinerClass(IntSumReducer.class);

        // set reducer
        job.setReducerClass(IntSumReducer.class);

        // set input format: read the whole file
        job.setInputFormatClass(WholeFileInputFormat.class);

        // set output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set input path
        //FileInputFormat.addInputPath(job, new Path("s3n://2018-spring-cis555-g09-htmlbucket"));
        FileInputFormat.addInputPath(job, new Path("./input"));
        FileOutputFormat.setOutputPath(job, new Path("./output"));
        job.waitForCompletion(true);
    }

}
