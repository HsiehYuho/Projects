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
package edu.upenn.cis.stormlite.bolt;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.stormlite.IOutputCollector;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;

/**
 * Simplified version of Storm output queues
 * 
 * @author zives
 *
 */
public class OutputCollector implements IOutputCollector, Context {
	StreamRouter router;
	TopologyContext context;
	
	public OutputCollector(TopologyContext context) {
		this.context = context;
	}

	@Override
	public void setRouter(StreamRouter router) {
		this.router = router;
	}
	
	/**
	 * Emits a tuple to the stream destination
	 * @param tuple
	 */
	public void emit(List<Object> tuple) {
		router.execute(tuple, context);
	}

	public void emitEndOfStream() {
		router.executeEndOfStream(context);
	}

	public StreamRouter getRouter() {
		return router;
	}

	@Override
	public void write(String key, String value) {
        context.incKeysWritten();
        context.addResult(key, value);
		List<Object> values = new ArrayList<>();
		values.add(key);
		values.add(value);
		emit(values);
	}
}
