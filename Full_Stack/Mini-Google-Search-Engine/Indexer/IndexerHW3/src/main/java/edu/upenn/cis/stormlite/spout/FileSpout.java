package edu.upenn.cis.stormlite.spout;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
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
public abstract class FileSpout implements IRichSpout {
	static Logger log = Logger.getLogger(FileSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;
	
	/**
	 * This is a simple file reader
	 */
	String inputDir;
    BufferedReader reader;
	Random r = new Random();
	
	int inx = 0;
	boolean sentEof = false;

    public FileSpout() {
        //this.inputDir = getFilename();
    }

    public abstract String getFilename();

    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.inputDir = (String) conf.get("inputDir");
        log.debug("Starting spout for " + this.inputDir);
        log.debug(getExecutorId() + " opening file reader");
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	if (reader != null)
	    	try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
    }

    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     */
    @Override
    public synchronized void nextTuple() {
        File envHome = new File(WorkerServer.getEnvDirectory() + inputDir);
        if (!envHome.exists() || sentEof)  return;
        log.debug("Read files in folder: " + WorkerServer.getEnvDirectory() + inputDir);

        for (File file : envHome.listFiles()) {
            if (!file.isDirectory() && !file.isHidden()) {
                String filename = file.getName();
                try {
                    reader = new BufferedReader(new FileReader(envHome + "/" + filename));
                } catch (IOException e) {
                    System.out.println("Error reading file");
                    continue;
                }

                if (reader != null) {
                    try {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            log.debug(getExecutorId() + " read from file " + filename + ": " + line);
                            this.collector.emit(new Values<Object>(String.valueOf(inx++), line));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        log.info(getExecutorId() + " finished file " + getFilename() + " and emitting EOS");
        this.collector.emitEndOfStream();
        sentEof = true;
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}
}
