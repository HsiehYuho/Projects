package test.edu.upenn.cis.stormlite.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import test.edu.upenn.cis.stormlite.PrintBolt;

/**
 * Simple word counter test case, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TestMapReduce {
	static Logger log = Logger.getLogger(TestMapReduce.class);

	private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCE_BOLT = "REDUCE_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";
    
    static void createSampleMapReduce(Config config) {
        // Job name
        config.put("job", "MyJob1");
        
        // IP:port for /workerstatus to be sent
        config.put("master", "127.0.0.1:8080");
        
        // Class with map function
        config.put("mapClass", "test.edu.upenn.cis.stormlite.mapreduce.GroupWords");
        // Class with reduce function
        config.put("reduceClass", "test.edu.upenn.cis.stormlite.mapreduce.GroupWords");
        
        // Numbers of executors (per node)
        config.put("spoutExecutors", "1");
        config.put("mapExecutors", "1");
        config.put("reduceExecutors", "1");
    }

    /**
     * Command line parameters:
     * 
     * Argument 0: worker index in the workerList (0, 1, ...)
     * Argument 1: non-empty parameter specifying that this is the requestor (so we know to send the JSON)
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        
        // Complete list of workers, comma-delimited
        config.put("workerList", "[127.0.0.1:8000,127.0.0.1:8001]");

        // Build the local worker
        if (args.length >= 1) {
            config.put("workerIndex", args[0]);
        } else
            config.put("workerIndex", "0");
        
        WorkerServer.createWorker(config);

        // If we're the Master, we need to initiate the computation
        if (args.length > 1) {
			// Let the server start up
    		System.out.println("Press [Enter] to launch query, once nodes are alive...");
    		(new BufferedReader(new InputStreamReader(System.in))).readLine();

			log.info("************ Creating the job request ***************");

			createSampleMapReduce(config);
			
	        FileSpout spout = new WordFileSpout();
	        MapBolt bolt = new MapBolt();
	        ReduceBolt bolt2 = new ReduceBolt();
	        PrintBolt printer = new PrintBolt();

	        TopologyBuilder builder = new TopologyBuilder();

	        // Only one source ("spout") for the words
	        builder.setSpout(WORD_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));
	        
	        // Parallel mappers, each of which gets specific words
	        builder.setBolt(MAP_BOLT, bolt, Integer.valueOf(config.get("mapExecutors"))).fieldsGrouping(WORD_SPOUT, new Fields("value"));
	        
	        // Parallel reducers, each of which gets specific words
	        builder.setBolt(REDUCE_BOLT, bolt2, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));

	        // Only use the first printer bolt for reducing to a single point
	        //builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);
            builder.setBolt(PRINT_BOLT, printer, 1).shuffleGrouping(REDUCE_BOLT);
	        
	        Topology topo = builder.createTopology();
	        
	        WorkerJob job = new WorkerJob(topo, config);
			
	        ObjectMapper mapper = new ObjectMapper();
	        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
			try {
				String[] workers = WorkerHelper.getWorkers(config);
	
				int i = 0;
				for (String dest: workers) {
				    System.out.println("~~~: " + dest);
			        config.put("workerIndex", String.valueOf(i++));
					if (sendJob(dest, "POST", config, "definejob",
							mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() !=
							HttpURLConnection.HTTP_OK) {
						throw new RuntimeException("Job definition request failed");
					}
				}
				for (String dest: workers) {
					if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != 
							HttpURLConnection.HTTP_OK) {
						throw new RuntimeException("Job execution request failed");
					}
				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
		        System.exit(0);
			}        
        }
        
		System.out.println("Press [Enter] to exit...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();

		WorkerServer.shutdown();
        System.exit(0);
		
    }
    
    public static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
		URL url = new URL(dest + "/" + job);
		
		log.info("Sending request to " + url.toString());
		
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);
		
		if (reqType.equals("POST")) {
			conn.setRequestProperty("Content-Type", "application/json");
			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		} else
			conn.getOutputStream();
		
		return conn;
    }
}
