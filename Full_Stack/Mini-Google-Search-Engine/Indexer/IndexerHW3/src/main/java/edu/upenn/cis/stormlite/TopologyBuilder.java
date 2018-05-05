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
package edu.upenn.cis.stormlite;

import edu.upenn.cis.stormlite.bolt.BoltDeclarer;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.spout.IRichSpout;

public class TopologyBuilder {
	Topology topo = new Topology();
	
	public void setSpout(
			String streamID, 
			IRichSpout spout, 
			int parallelism) {
		
		topo.setSpouts(streamID, spout.getClass(), parallelism);
	}
	
	public BoltDeclarer setBolt(String streamID, IRichBolt bolt, int parallelism) {
		topo.setBolts(streamID, bolt.getClass(), parallelism);
		
		topo.setBoltGrouping(streamID, new BoltDeclarer());
		
		return topo.boltGrouping.get(streamID);
	}
	
	public Topology createTopology() {
		return topo;
	}
}
