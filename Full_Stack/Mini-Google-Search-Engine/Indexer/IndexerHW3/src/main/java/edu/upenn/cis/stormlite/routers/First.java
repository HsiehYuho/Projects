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
package edu.upenn.cis.stormlite.routers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.bolt.IRichBolt;

/**
 * Does round-robin among the destination bolts
 * 
 * @author zives
 *
 */
public class First extends StreamRouter {
	static Logger log = Logger.getLogger(First.class);
	
	int inx = 0;
	List<IRichBolt> children;
	
	public First() {
		children = new ArrayList<IRichBolt>();
	}
	
	public First(IRichBolt child) {
		children = new ArrayList<IRichBolt>();
		children.add(child);
	}
	
	public First(List<IRichBolt> children) {
		this.children = children;
	}
	

	/**
	 * Only the first bolt
	 * 
	 */
	@Override
	protected IRichBolt getBoltFor(List<Object> tuple) {
		
		if (getBolts().isEmpty()) {
			log.error("Could not find destination for " + tuple.toString());
			return null;
		}
		
		IRichBolt bolt = getBolts().get(0);
		
		log.debug("Routing " + tuple.toString() + " to " + bolt.getExecutorId());
		
		return bolt;
	}


}
