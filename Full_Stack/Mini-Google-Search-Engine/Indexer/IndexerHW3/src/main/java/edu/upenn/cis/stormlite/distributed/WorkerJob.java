package edu.upenn.cis.stormlite.distributed;

import java.io.Serializable;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;

/**
 * Simple object to pass along topology and
 * config info
 * 
 * @author zives
 *
 */
public class WorkerJob implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Topology topology;
	
	Config config;
	
	public WorkerJob() {
		
	}
	
	public WorkerJob(Topology topology, Config config) {
		super();
		this.topology = topology;
		this.config = config;
	}

	public Topology getTopology() {
		return topology;
	}

	public void setTopology(Topology topology) {
		this.topology = topology;
	}

	public Config getConfig() {
		return config;
	}

	public void setConfig(Config config) {
		this.config = config;
	}

}
