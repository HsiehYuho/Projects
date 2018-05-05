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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class PageRankFirst {
	static org.apache.commons.logging.Log log = LogFactory.getLog(PageRankFirst.class);

	public static class PageRankMapper1 extends Mapper<Object, BytesWritable, Text, Text> {

		@Override
		public void map(Object key, BytesWritable value, Context context
		) throws IOException, InterruptedException {

			ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;
			PageRankObj prObject = mapper.readValue(new String(value.getBytes()), PageRankObj.class);

			List<String> outboundList = prObject.getOutBoundUrls();
			URLInfo urlInfo = new URLInfo(prObject.getUrl());
			String parentHostName = urlInfo.getHostName();

			if (outboundList == null) return;
			//remove dangling list
			outboundList.remove(prObject.getUrl());
			double length = outboundList.size();
			for (String url : outboundList) {
				String childHostName = new URLInfo(url).getHostName();
				if (childHostName == null || childHostName.equals(parentHostName)) continue;
				context.write(new Text(parentHostName), new Text(childHostName + "\t" + length));
			}
		}
	}

	public static class PageRankMapper2 extends Mapper<Object, BytesWritable, Text, Text> {

		@Override
		public void map(Object key, BytesWritable value, Context context
		) throws IOException, InterruptedException {
			ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;
			PageRankObj prObject = mapper.readValue(new String(value.getBytes()), PageRankObj.class);
			URLInfo urlInfo = new URLInfo(prObject.getUrl());
			String parentHostName = urlInfo.getHostName();
			context.write(new Text(parentHostName), new Text(Double.toString(1.0)));
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
						prValue = Double.parseDouble(element);
					}
				}
				for (String element : outbound.keySet()) {
					String score = Double.toString(prValue / outbound.get(element));
					context.write(new Text(element), new Text(score));
				}
			} catch (NumberFormatException e) {}

		}
	}

	public static void main(String[] args) throws Exception {
		/**Configuration and job set up**/
		log = LogFactory.getLog(PageRankFirst.class);
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		Configuration conf = new Configuration();
		conf.set("fs.s3n.awsAccessKeyId", credentials.getAWSAccessKeyId());
		conf.set("fs.s3n.awsSecretAccessKey", credentials.getAWSSecretKey());
		Job job = Job.getInstance(conf, "pagerank");
		job.setJarByClass(PageRankFirst.class);
		job.setInputFormatClass(PageRankInputFormat.class);
		job.setMapperClass(PageRankMapper1.class);
		job.setMapperClass(PageRankMapper2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/**Mutiple input paths to compute page rank **/
		MultipleInputs.addInputPath(job, new Path("s3n://2018-spring-cis555-g09-news-pr-bucket/"),
				PageRankInputFormat.class, PageRankMapper2.class);
		MultipleInputs.addInputPath(job, new Path("s3n://2018-spring-cis555-g09-www.calflora.org/"),
				PageRankInputFormat.class, PageRankMapper1.class);
		FileOutputFormat.setOutputPath(job, new Path("output-demo"));

		System.out.println("start! ");
		job.waitForCompletion(true);
		System.out.println("complete! ");

		/** Sum all the pagerank values based on hostname*/
		PageRankSum.main(new String[]{"output-demo", "finalOutput-demo1"});

		/**start following iterations **/
		PageRankSecond.main(new String[]{});
	}
}

