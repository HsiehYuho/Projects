package test.edu.upenn.cis.stormlite;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * Simple word counter, largely derived from
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

public class WordCounter implements IRichBolt {
	static Logger log = Logger.getLogger(WordCounter.class);
	
	Fields schema = new Fields("word", "count");
	
	/**
	 * This is a simple map from word to a running count.
	 * If we have multiple "sharded" copies of the WordCounter
	 * we'll need to make sure they get different words, otherwise
	 * we'll have multiple partial counts instead of a single unified
	 * one.
	 * 
	 */
	private Map<String, Integer> wordCounter = new HashMap<>();
    
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    public WordCounter() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        int count;
        log.debug(getExecutorId() + " received " + word);
        if (wordCounter.containsKey(word)) {
            count = wordCounter.get(word) + 1;
            wordCounter.put(word, wordCounter.get(word) + 1);
        } else {
            count = 1;
        }

        wordCounter.put(word, count);
        collector.emit(new Values<Object>(word, String.valueOf(count)));
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	System.out.println("WordCount executor " + getExecutorId() + " has words: " + wordCounter.keySet());

    	wordCounter.clear();
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
