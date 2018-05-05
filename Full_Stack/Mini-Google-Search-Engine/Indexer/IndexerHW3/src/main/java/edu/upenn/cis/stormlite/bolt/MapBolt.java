package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis455.mapreduce.worker.WorkerMain;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.Job;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "map"
 * on a per-tuple basis.
 * 
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

public class MapBolt implements IRichBolt {
	static Logger log = Logger.getLogger(MapBolt.class);

	Job mapJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	/**
	 * This tracks how many "end of stream" messages we've seen
	 */
	int neededVotesToComplete = 0;

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;

    static ObjectMapper mapper = new ObjectMapper();
    
    public MapBolt() {
    }
    
	/**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf,
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        // get mapper class
        if (!stormConf.containsKey("job"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("job");

        	try {
				mapJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }

        // TODO: determine how many end-of-stream requests are needed
        if (!stormConf.containsKey("spoutExecutors")) {
        	throw new RuntimeException("Mapper class doesn't know how many input spout executors");
        } else {
        	try {
        	    // #local spouts + #remote worker * #senderbolts(#mapbolts)
                int numOfWorkers = stormConf.get("workerList").split(",").length;
                int numOfLocalSpouts = Integer.parseInt(stormConf.get("spoutExecutors"));
                int numOfMapBolts = Integer.parseInt(stormConf.get("mapExecutors"));
				neededVotesToComplete = numOfLocalSpouts + (numOfWorkers - 1) * numOfMapBolts;
			} catch (Exception e) {
        		System.out.println("Integer parsing error");
			}
		}
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public synchronized void execute(Tuple input) {
		context.setStatus("MAPPING");
		log.debug(executorId + " is setting MAPPING");

    	if (!input.isEndOfStream()) {
	        String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);
	        
	        if (neededVotesToComplete == 0)
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
	        
	        // TODO:  call the mapper, and do bookkeeping to track work done
            context.incKeysRead();

            // get doc Object from S3
            StringBuilder sb = new StringBuilder();
            try {
                S3Object object = WorkerMain.s3.getObject(new GetObjectRequest(WorkerMain.html_bucket_name, value));
                InputStream inputStream = object.getObjectContent();

                // process the objectData stream
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                inputStream.close();
            } catch (Exception e) {
                System.out.println("Error reading from S3");
                return;
            }

            mapJob.map(value, sb.toString(), this.collector);
        } else if (input.isEndOfStream()) {
    		// TODO: determine what to do with EOS
            neededVotesToComplete--;
            log.debug("map bolt gets EOF, left: " + neededVotesToComplete);
            if (neededVotesToComplete == 0) {
                this.collector.emitEndOfStream();
                context.setStatus("WAITING");
                log.debug(executorId + " is setting WAITING");
            }
    	}
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
