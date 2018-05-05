package edu.upenn.cis455.pagerank;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankSum {

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            context.write(new Text(input[0]), new Text(input[1]));
        }
    }

    public static class PageRankReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            Double res = 0.0;
            for (Text element: values){
                String e = element.toString();
                res += Double.parseDouble(e);
            }
            if (key == null) return ;
            String name = key.toString();
            if (name.endsWith("edu") || name.endsWith("gov") || name.endsWith("org")) res *= 5;
            context.write(key, new Text(Double.toString(res)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pageranksum");
        job.setJarByClass(PageRankSum.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}
