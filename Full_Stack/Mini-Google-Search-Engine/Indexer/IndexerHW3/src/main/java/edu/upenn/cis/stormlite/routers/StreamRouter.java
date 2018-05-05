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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.distributed.SenderBolt;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tasks.BoltTask;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

/**
 * A StreamRouter is an internal class used to determine where
 * an item placed on a stream should go.  It doesn't actually
 * run the downstream bolt, but rather queues it up as a task.
 * 
 * @author zives
 *
 */
public abstract class StreamRouter implements OutputFieldsDeclarer {
	static Logger log = Logger.getLogger(StreamRouter.class);
	
	List<IRichBolt> bolts;  // all bolts: local and remote
	List<HttpURLConnection> workers;
	Fields schema;
	Set<IRichBolt> remoteBolts = new HashSet<>();   // remote bolts
	
	public StreamRouter() {
		bolts = new ArrayList<>();
	}
	
	public StreamRouter(IRichBolt bolt) {
		this();
		bolts.add(bolt);
	}
	
	/**
	 * Add another bolt instance as a consumer of this stream
	 * 
	 * @param bolt
	 */
	public void addBolt(IRichBolt bolt) {
		bolts.add(bolt);
	}

	/**
	 * Add a sender bolt instance as a consumer of this stream
	 * 
	 * @param bolt
	 */
	public void addRemoteBolt(SenderBolt bolt) {
		bolts.add(bolt);
		remoteBolts.add(bolt);
	}
	
	/**
	 * Is this a remote (sender) bolt, or a local bolt?
	 * 
	 * @param bolt
	 * @return
	 */
	public boolean isRemoteBolt(IRichBolt bolt) {
		return remoteBolts.contains(bolt);
	}
	
	/**
	 * Add a worker node
	 * 
	 * @param worker
	 * @throws IOException
	 */
	public void addWorker(String worker) throws IOException {
		URL url = new URL(worker);
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "application/json");
		this.workers.add(conn);
	}
	
	/**
	 * The selector for the destination bolt
	 * 
	 * @param tuple
	 * @return
	 */
	protected abstract IRichBolt getBoltFor(List<Object> tuple);
	
	/**
	 * The destination bolts, as a list
	 * so we can assign each a unique position
	 * 
	 * @return
	 */
	public List<IRichBolt> getBolts() {
		return bolts;
	}
	
	/**
	 * Queues up a bolt task (for future scheduling) to process a single 
	 * stream tuple
	 * 
	 * @param tuple
	 */
	public synchronized void execute(List<Object> tuple, TopologyContext context) {
		IRichBolt bolt = getBoltFor(tuple);

		log.debug("Task queued: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): " + tuple.toString());

		if (bolt != null)
			context.addStreamTask(new BoltTask(bolt, new Tuple(schema, tuple)));
		else
			throw new RuntimeException("Unable to find a bolt for the tuple");
	}
	
	public synchronized void executeLocally(List<Object> tuple, TopologyContext context) {
		IRichBolt bolt = getBoltFor(tuple);

		int retries = 0;
		// If we got a remote bolt
		while (isRemoteBolt(bolt)) {
			bolt = getBoltFor(tuple);
			retries++;

			if (retries >= getBolts().size())
				throw new RuntimeException("Trying to route to a local bolt executor, but our router seems to only return remote ones");
		}

		log.debug("Task queued from other worker: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): " + tuple.toString());
		if (bolt != null)
			context.addStreamTask(new BoltTask(bolt, new Tuple(schema, tuple)));
		else
			throw new RuntimeException("Unable to find a bolt for the tuple");
	}

	/**
	 * Process a tuple with fields
	 * 
	 * @param tuple
	 */
	public void execute(Tuple tuple, TopologyContext context) {
		execute(tuple.getValues(), context);
	}

	/**
	 * Process a tuple with fields, locally only
	 * 
	 * @param tuple
	 */
	public void executeLocally(Tuple tuple, TopologyContext context) {
		executeLocally(tuple.getValues(), context);
	}

	/**
	 * Sets the schema of the object
	 */
	@Override
	public void declare(Fields fields) {
		schema = fields;
	}

	/**
	 * Executes the bolt over end-of-stream
	 * 
	 * @param context
	 */
	public synchronized void executeEndOfStream(TopologyContext context) {
		for (IRichBolt bolt: getBolts()) {
			log.debug("Task queued: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): (EOS)");
			context.addStreamTask(new BoltTask(bolt, Tuple.getEndOfStream()));
		}
	}

	public synchronized void executeEndOfStreamLocally(TopologyContext context) {
		for (IRichBolt bolt: getBolts())
			if (!isRemoteBolt(bolt)) {
				log.debug("Task queued from other worker: " + bolt.getClass().getName() + " (" + bolt.getExecutorId() + "): (EOS)");
				context.addStreamTask(new BoltTask(bolt, Tuple.getEndOfStream()));
			}
	}

	public String getKey(List<Object> input) {
		return input.toString();
	}
}
