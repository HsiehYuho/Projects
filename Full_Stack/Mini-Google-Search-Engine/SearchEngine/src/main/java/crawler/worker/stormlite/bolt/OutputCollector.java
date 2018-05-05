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
package crawler.worker.stormlite.bolt;

import java.util.List;

import crawler.worker.stormlite.IOutputCollector;
import crawler.worker.stormlite.TopologyContext;
import crawler.worker.stormlite.routers.IStreamRouter;

/**
 * Simplified version of Storm output queues
 * 
 * @author zives
 *
 */
public class OutputCollector implements IOutputCollector {
	IStreamRouter router;
	TopologyContext context;
	
	public OutputCollector(TopologyContext context) {
		this.context = context;
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.router = router;
	}
	
	/**
	 * Emits a tuple to the stream destination
	 * @param tuple
	 */
	public void emit(List<Object> tuple) {
		router.execute(tuple, context);
	}

	public void emit(String stream, List<Object> tuple) {
		this.context.getRouter(stream).execute(tuple,context);
	}
	public IStreamRouter getRouter() {
		return router;
	}
}
