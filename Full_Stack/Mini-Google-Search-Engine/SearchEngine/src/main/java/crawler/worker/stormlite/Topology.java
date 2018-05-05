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
package crawler.worker.stormlite;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import crawler.worker.stormlite.bolt.BoltDeclarer;
import crawler.worker.stormlite.bolt.IRichBolt;
import crawler.worker.stormlite.spout.IRichSpout;
import crawler.worker.stormlite.tuple.Fields;

public class Topology {
	/**
	 * Spouts are the inputs, and each has a stream ID and a parallelism
	 */
	Map<String,Pair<Class<? extends IRichSpout>,Integer>> spouts = new HashMap<>();
	
	/**
	 * Bolts are the operators, and each has a stream ID
	 * disjoint from the spouts
	 */
	Map<String, Pair<Class<? extends IRichBolt>, Integer>> bolts = new HashMap<>();
	
	/**
	 * Bolts have multiple inputs connected to spouts (or other bolts)
	 */
	Map<Pair<String,Integer>, String> boltConnectors = new HashMap<>();
	
	/**
	 * Each Stream has a set of fields, i.e., a schema
	 */
	Map<String, Fields> streamSchemas = new HashMap<>();

	/**
	 * Each stream also has a grouping type
	 */
	Map<String, BoltDeclarer> boltGrouping = new HashMap<>();

	public Map<String, Pair<Class<? extends IRichSpout>, Integer>> getSpouts() {
		return spouts;
	}
	
	public Pair<Class<? extends IRichSpout>, Integer> getSpout(String key) {
		return spouts.get(key);
	}

	public void setSpouts(String name, Class<? extends IRichSpout> spoutClass, Integer parallel) {
		this.spouts.put(name, Pair.<Class<? extends IRichSpout>, Integer>of(spoutClass, Integer.valueOf(parallel)));
	}

	public Map<String, Pair<Class<? extends IRichBolt>, Integer>> getBolts() {
		return bolts;
	}
	
	public Pair<Class<? extends IRichBolt>, Integer> getBolt(String key) {
		return bolts.get(key);
	}

	public void setBolts(String bolt, Class<? extends IRichBolt> boltClass, Integer parallel) {
		this.bolts.put(bolt, Pair.<Class<? extends IRichBolt>, Integer>of(boltClass, Integer.valueOf(parallel)));
	}

	public Map<Pair<String, Integer>, String> getBoltConnectors() {
		return boltConnectors;
	}

	public void setBoltConnectors(Map<Pair<String, Integer>, String> boltConnectors) {
		this.boltConnectors = boltConnectors;
	}

	public Map<String, Fields> getStreamSchemas() {
		return streamSchemas;
	}

	public void setStreamSchemas(Map<String, Fields> streamSchemas) {
		this.streamSchemas = streamSchemas;
	}

	public Map<String, BoltDeclarer> getBoltGrouping() {
		return boltGrouping;
	}

	public void setBoltGrouping(String streamID, BoltDeclarer boltDeclarer) {
		this.boltGrouping.put(streamID, boltDeclarer);
	}

	public BoltDeclarer getBoltDeclarer(String stream) {
		return getBoltGrouping().get(stream);
	}
	
	
}
