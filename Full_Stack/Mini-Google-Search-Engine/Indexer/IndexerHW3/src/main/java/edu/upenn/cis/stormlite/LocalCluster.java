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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.bolt.BoltDeclarer;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tasks.SpoutTask;

/**
 * Use multiple threads to simulate a cluster of worker nodes.
 * Emulates a distributed environment.
 * 
 * A thread pool (the executor) executes runnable tasks.  Each
 * task involves calling a nextTuple() or execute() method in
 * a spout or bolt, then routing its tuple to the router. 
 * 
 * @author zives
 *
 */
public class LocalCluster implements Runnable {
	static Logger log = Logger.getLogger(LocalCluster.class);
	
	static AtomicBoolean quit = new AtomicBoolean(false);
	
	String theTopology;
	
	Map<String, List<IRichBolt>> boltStreams = new HashMap<>();
	Map<String, List<IRichSpout>> spoutStreams = new HashMap<>();
	
	Map<String, StreamRouter> streams = new HashMap<>();
	
	TopologyContext context;
	
	ObjectMapper mapper = new ObjectMapper();
	
	ExecutorService executor = Executors.newFixedThreadPool(5);//creating a pool of 5 threads  
	Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<Runnable>();
	

	public void submitTopology(String name, Config config, 
			Topology topo) throws ClassNotFoundException {
		theTopology = name;
		
		context = new TopologyContext(topo, taskQueue);
		
		createSpoutInstances(topo, config);
		scheduleSpouts();
		
		createBoltInstances(topo, config);
		
		createRoutes(topo, config);
		
		// Put the run method in a background thread
		new Thread(this).start();;
	}
	
	public void run() {
		while (!quit.get()) {
			Runnable task = taskQueue.poll();
			if (task == null)
				Thread.yield();
			else {
				executor.execute(task);
			}
		}
	}
	
	private void scheduleSpouts() {
		for (String key: spoutStreams.keySet())
			for (IRichSpout spout: spoutStreams.get(key)) {
				taskQueue.add(new SpoutTask(spout, taskQueue));
			}
	}
	
	/**
	 * For each spout in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 * @throws ClassNotFoundException 
	 */
	private void createSpoutInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String key: topo.getSpouts().keySet()) {
			StringIntPair spout = topo.getSpout(key);
			
			SpoutOutputCollector collector = 
					new SpoutOutputCollector(context);

			spoutStreams.put(key, new ArrayList<IRichSpout>());
			for (int i = 0; i < spout.getRight(); i++)
				try {
					IRichSpout newSpout = (IRichSpout)Class.forName(spout.getLeft()).newInstance();
					
					newSpout.open(config, context, collector);
					spoutStreams.get(key).add(newSpout);
					log.debug("Created a spout executor " + key + "/" + newSpout.getExecutorId() + " of type " + spout.getLeft());
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}


	/**
	 * For each bolt in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 * @throws ClassNotFoundException 
	 */
	private void createBoltInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String key: topo.getBolts().keySet()) {
			StringIntPair bolt = topo.getBolt(key);
			
			OutputCollector collector = new OutputCollector(context);
			
			boltStreams.put(key, new ArrayList<IRichBolt>());
			for (int i = 0; i < bolt.getRight(); i++)
				try {
					IRichBolt newBolt = (IRichBolt)Class.forName(bolt.getLeft()).newInstance();
					newBolt.prepare(config, context, collector);
					boltStreams.get(key).add(newBolt);
					log.debug("Created a bolt executor " + key + "/" + newBolt.getExecutorId() + " of type " + bolt.getLeft());
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	/**
	 * Link the output streams to input streams, ensuring that the right kinds
	 * of grouping + routing are accomplished
	 * 
	 * @param topo
	 * @param config
	 */
	private void createRoutes(Topology topo, Config config) {
		// Add destination streams to the appropriate bolts
		for (String stream: topo.getBolts().keySet()) {
			BoltDeclarer decl = topo.getBoltDeclarer(stream);
			
			StreamRouter router = decl.getRouter();
			
			for (IRichBolt bolt: boltStreams.get(stream)) {
				router.addBolt(bolt);
				log.debug("Adding a route from " + decl.getStream() + " to " + bolt);
			}
			
			if (topo.getBolts().containsKey(decl.getStream())) {
				for (IRichBolt bolt: boltStreams.get(decl.getStream())) {
					bolt.setRouter(router);
					bolt.declareOutputFields(router);
				}
			} else {
				for (IRichSpout spout: spoutStreams.get(decl.getStream())) {
					spout.setRouter(router);
					spout.declareOutputFields(router);
				}
			}
		}
	}

	/**
	 * For each bolt in the topology, clean up objects
	 * 
	 * @param topo Topology
	 */
	private void closeBoltInstances() {
		for (List<IRichBolt> boltSet: boltStreams.values())
			for (IRichBolt bolt: boltSet)
				bolt.cleanup();
	}

	/**
	 * For each spout in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 */
	private void closeSpoutInstances() {
		for (List<IRichSpout> spoutSet: spoutStreams.values())
			for (IRichSpout spout: spoutSet)
				spout.close();
	}

	/**
	 * Shut down the cluster
	 * 
	 * @param string
	 */
	public void killTopology(String string) {
		if (quit.getAndSet(true) == false) {
			while (!quit.get())
				Thread.yield();
		}
		System.out.println(context.getMapOutputs() + " local map outputs and " + 
				context.getReduceOutputs() + " local reduce outputs");
	}

	/**
	 * Shut down the bolts and spouts
	 */
	public void shutdown() {
		closeSpoutInstances();
		closeBoltInstances();

		System.out.println("Shutting down local cluster.");
	}

}
