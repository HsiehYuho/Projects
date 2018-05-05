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
package crawler.worker.stormlite.routers;

import java.util.ArrayList;
import java.util.List;

import crawler.worker.stormlite.bolt.IRichBolt;
import crawler.worker.stormlite.tuple.Fields;

/**
 * Does hash partitioning on the tuple to determine
 * a destination
 * 
 * @author zives
 *
 */
public class FieldBased extends IStreamRouter {
	List<Integer> fieldsToHash;
	List<String> shardFields;
	
	public FieldBased(List<String> shardFields) {
		fieldsToHash = new ArrayList<Integer>();
		this.shardFields = shardFields;
	}
	
	/**
	 * Adds an index field of an attribute that's used to shard the data
	 * @param field
	 */
	public void addField(Integer field) {
		fieldsToHash.add(field);
	}
	
	/**
	 * Determines which bolt to route tuples to
	 */
	public IRichBolt getBoltFor(List<Object> tuple) {
		
		int hash = 0;
		
		if (fieldsToHash.isEmpty())
			throw new IllegalArgumentException("Field-based grouping without a shard attribute");
		
		for (Integer i: fieldsToHash)
			hash ^= tuple.get(i).hashCode();
		
		hash = hash % getBolts().size();
		if (hash < 0)
			hash = hash + getBolts().size();

		return getBolts().get(hash);
	}

	/**
	 * Handler that, given a schema, looks up the index positions used
	 * for sharding fields
	 */
	@Override
	public void declare(Fields fields) {
		super.declare(fields);

		if (shardFields != null) {
			for (String name: shardFields) {
				Integer pos = fields.indexOf(name);
				if (pos < 0)
					throw new IllegalArgumentException("Shard field " + name + " was not found in " + fields);
				if (!fieldsToHash.contains(pos))
					fieldsToHash.add(pos);
			}
		}
	}
}
