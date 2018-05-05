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
import java.util.Queue;
import java.util.Set;

import crawler.worker.stormlite.bolt.BoltDeclarer;
import crawler.worker.stormlite.routers.IStreamRouter;

/**
 * Information about the execution of a topology, including
 * the stream routers
 * 
 * @author zives
 *
 */
public class TopologyContext {
	Topology topology;
	
	Queue<Runnable> taskQueue;
	
	/**
	 * Mappings from stream IDs to routers
	 */
	Map<String,IStreamRouter> next = new HashMap<>();
	
	public TopologyContext(Topology topo, Queue<Runnable> theTaskQueue) {
		Set<String> boltStreams = topo.getBoltGrouping().keySet();
		for(String streamId : boltStreams){
			BoltDeclarer boltDec = topo.getBoltDeclarer(streamId);
			next.put(streamId, boltDec.getRouter());
		}
		topology = topo;
		taskQueue = theTaskQueue;
	}
	
	public Topology getTopology() {
		return topology;
	}
	
	public void setTopology(Topology topo) {
		this.topology = topo;
	}
	
	public void addStreamTask(Runnable next) {
		taskQueue.add(next);
	}
	public IStreamRouter getRouter(String streamId){
		return next.get(streamId);
	}
	
}
