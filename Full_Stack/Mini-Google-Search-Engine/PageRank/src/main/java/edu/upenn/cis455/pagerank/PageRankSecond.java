package edu.upenn.cis455.pagerank;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import crawler.URLInfo;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class PageRankSecond {
    static org.apache.commons.logging.Log log = LogFactory.getLog(PageRankSecond.class);

    public static class PageRankMapper1 extends Mapper<Object, BytesWritable, Text, Text> {

        @Override
        public void map(Object key, BytesWritable value, Context context
        ) throws IOException, InterruptedException {

            ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;
            PageRankObj prObject = mapper.readValue(new String(value.getBytes()), PageRankObj.class);

            List<String> outboundList = prObject.getOutBoundUrls();
            URLInfo urlInfo = new URLInfo(prObject.getUrl());
            String parentHostName = urlInfo.getHostName();

            //remove dangling list
            if (outboundList == null) return;
            outboundList.remove(prObject.getUrl());
            double length = outboundList.size();

            for (String url : outboundList) {
                String childHostName = new URLInfo(url).getHostName();
                if (childHostName == null || childHostName.equals(parentHostName)) continue;
                context.write(new Text(parentHostName), new Text(childHostName + "\t" + length));
            }
        }
    }

    public static class PageRankMapper2 extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] input = value.toString().split("\t");
            context.write(new Text(input[0]), new Text(input[1]));
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            Double prValue = 0.0;
            HashMap<String, Double> outbound = new HashMap<>();
            try{
                for (Text e : values) {
                    String element = e.toString();
                    if (element.contains("\t")) {
                        String[] split = element.split("\t");
                        outbound.put(split[0], Double.parseDouble(split[1]));
                    } else {
                        prValue = Double.parseDouble(element)*0.85;
                    }
                }
                if (prValue == 0) prValue = 10.0;
                else prValue *= 100000;
                for (String element : outbound.keySet()) {
                    String score = Double.toString(prValue / outbound.get(element));
                    context.write(new Text(element), new Text(score));
                }
            } catch (NumberFormatException e) {}
        }
    }


    public static void driver(String[] args) throws Exception {
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        log = LogFactory.getLog(PageRankSecond.class);

        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsAccessKeyId", credentials.getAWSAccessKeyId());
        conf.set("fs.s3n.awsSecretAccessKey", credentials.getAWSSecretKey());

        Job job = Job.getInstance(conf, "pagerank");
        job.setJarByClass(PageRankSecond.class);

        job.setInputFormatClass(PageRankInputFormat.class);

        job.setMapperClass(PageRankMapper1.class);
        job.setMapperClass(PageRankMapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("s3n://2018-spring-cis555-g09-www.calflora.org/"),
                PageRankInputFormat.class, PageRankMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, PageRankMapper2.class);

        System.out.println("start! ");

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        System.out.println("complete! ");

        PageRankSum.main(new String[]{args[1], args[2]});

    }

    public static void main(String[] args) throws Exception {
        String path1 = "finalOutput-demo";
        String path2 = "output-demo";
        for (int i = 1; i < 6 ; i ++) {
            int next = i + 1;
            driver(new String[]{path1 + i, path2 + i, path1 + next});
        }

    }
}
