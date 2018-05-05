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
package edu.upenn.cis.stormlite.spout;

import java.util.Map;

import edu.upenn.cis.stormlite.IStreamSource;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;

/**
 * ISpout is the core interface for implementing spouts. 
 * A Spout is responsible for feeding messages into the topology for 
 * processing. For every tuple emitted by a spout, Storm will track 
 * the (potentially very large) DAG of tuples generated based on a 
 * tuple emitted by the spout.
 * @author zives
 *
 */
public interface IRichSpout extends IStreamSource {
	/**
	 * Called when a task for this component is initialized within a 
	 * worker on the cluster. It provides the spout with the environment 
	 * in which the spout executes.
	 * 
	 * @param config The Storm configuration for this spout. This is 
	 * 		  the configuration provided to the topology merged in
	 *        with cluster configuration on this machine.
	 * @param topo 
	 * @param collector The collector is used to emit tuples from 
	 *        this spout. Tuples can be emitted at any time, including 
	 *        the open and close methods. The collector is thread-safe 
	 *        and should be saved as an instance variable of this spout 
	 *        object.
	 */
	public void open(Map<String,String> config, TopologyContext topo, SpoutOutputCollector collector);

	/**
	 * Called when an ISpout is going to be shutdown. 
	 * There is no guarantee that close will be called, because the 
	 * supervisor kill -9’s worker processes on the cluster.
	 */
	public void close();
	
	/**
	 * When this method is called, Storm is requesting that the Spout emit 
	 * tuples to the output collector. This method should be non-blocking, 
	 * so if the Spout has no tuples to emit, this method should return. 
	 */
	public void nextTuple();

	public void setRouter(StreamRouter router);

}
