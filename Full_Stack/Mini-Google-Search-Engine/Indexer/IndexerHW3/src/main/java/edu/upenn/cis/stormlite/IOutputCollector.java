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

import java.util.List;

import edu.upenn.cis.stormlite.routers.StreamRouter;

/**
 * A stream propagation interface, used by a spout or bolt to send
 * tuples to the next stage
 * 
 * @author zives
 *
 */
public interface IOutputCollector {
	/**
	 * Propagates a tuple (list of objects) to a particular
	 * stream
	 * 
	 * @param tuple
	 */
	public void emit(List<Object> tuple);
	
	public void setRouter(StreamRouter router);

}
