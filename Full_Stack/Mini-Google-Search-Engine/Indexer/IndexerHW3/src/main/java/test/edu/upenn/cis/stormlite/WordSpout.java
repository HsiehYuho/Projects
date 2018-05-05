package test.edu.upenn.cis.stormlite;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
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
public class WordSpout implements IRichSpout {
	static Logger log = Logger.getLogger(WordSpout.class);

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
	 * This is a simple file reader for words.txt
	 */
    BufferedReader reader;
	Random r = new Random();
	
	String[] words = {"big", "bad", "wolf", "little", "red", "riding", "hood"};

    public WordSpout() {
    	log.debug("Starting spout");
    }


    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        
        try {
        	log.debug(getExecutorId() + " opening file reader");
			reader = new BufferedReader(new FileReader("words.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
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
    public void nextTuple() {
    	if (reader != null) {
	    	try {
		    	String line = reader.readLine();
		    	if (line != null) {
		        	log.debug(getExecutorId() + " read from file " + line);
		    		String[] words = line.split("[ \\t\\,.]");
		
		    		for (String word: words) {
		            	log.debug(getExecutorId() + " emitting " + word);
		    	        this.collector.emit(new Values<Object>(word));
		    		}
		    	} else {
		    		int pos = r.nextInt(words.length);
		    		String word = words[pos];
	            	log.debug(getExecutorId() + " emitting " + word);
	    	        this.collector.emit(new Values<Object>(word));
		    	}
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}
    	}
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
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
