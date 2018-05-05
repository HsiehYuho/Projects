package edu.upenn.cis.stormlite.bolt;

import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.amazon.s3.samplecode.UrlObj;
import edu.upenn.cis455.mapreduce.DBWrapper;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
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

public class ReduceBolt implements IRichBolt {
	static Logger log = Logger.getLogger(ReduceBolt.class);

	Job reduceJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    Fields schema = new Fields("key", "value");
	
	boolean sentEof = false;

    /** BerkeleyDB instance to hold intermediate result. */
    String dbEnv = null;
    DBWrapper db = null;

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;
    
    int neededVotesToComplete = 0;
    
    public ReduceBolt() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        this.dbEnv = WorkerServer.getEnvDirectory() + "/store/" + executorId;
        log.debug("Getting database: " + this.dbEnv);
        this.db = new DBWrapper(dbEnv);

        // get reducer class
        if (!stormConf.containsKey("job"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String reducerClass = stormConf.get("job");
        	
        	try {
				reduceJob = (Job)Class.forName(reducerClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + reducerClass);
			}
        }

        //TODO: determine how many EOS votes needed
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        } else {
            try {
                // #local mapbolts + #remote worker * #mapbolts * #senderbolts(#reducebolts)
                int numOfWorkers = stormConf.get("workerList").split(",").length;
                int numOfLocalMapBolts = Integer.parseInt(stormConf.get("mapExecutors"));
                int numOfReduceBolts = Integer.parseInt(stormConf.get("reduceExecutors"));
                neededVotesToComplete = numOfLocalMapBolts + (numOfWorkers - 1) * numOfLocalMapBolts * numOfReduceBolts;
            } catch (Exception e) {
                System.out.println("Integer parsing error");
            }
        }
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized void execute(Tuple input) {
    	if (sentEof) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
		} else if (input.isEndOfStream()) {
            neededVotesToComplete--;
            log.debug("reduce bolt gets EOF, left: " + neededVotesToComplete);
            //TODO: only if at EOS do we trigger the reduce operation and output all state
            if (neededVotesToComplete == 0) {
                context.setStatus("REDUCING");
                log.debug(executorId + " is setting REDUCING");
                //TODO: modify db, maybe create different dbs for each executor
                List<String> allKeys = db.getAllKeys();
                if (allKeys != null) {
                    for (String key: allKeys) {
                        log.debug(executorId + " checking: " + key);
                        reduceJob.reduce(key, db.get(key).iterator(), this.collector);
                    }
                }
                context.setStatus("IDLE");
                log.debug(executorId + " is setting IDLE");

                // close db instance
                if (db != null) {
                    db.close();
                }

                // clear the temp file in db
                File envHome = new File(dbEnv);
                if (envHome != null && envHome.exists() && envHome.isDirectory()) {
                    for (File file: envHome.listFiles()) {
                        file.delete();
                    }
                    envHome.delete();
                }
                sentEof = true;
            }
    	} else {
    		//TODO: this is a plain ol' hash map, replace it with BerkeleyDB
            // all executors on this node share the same db?
            context.incKeysRead();
            String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);

	        db.put(key, value);
    	}
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
        // close db
        if (db != null) {
            db.close();
        }
       // clear the temp file
        File envHome = new File(dbEnv);
        if (envHome != null && envHome.exists() && envHome.isDirectory()) {
            for (File file: envHome.listFiles()) {
                file.delete();
            }
            envHome.delete();
        }
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
