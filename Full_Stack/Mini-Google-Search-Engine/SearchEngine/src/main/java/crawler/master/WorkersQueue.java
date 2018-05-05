package crawler.master;

import java.util.*;

/**
 * Store mod idx and its corresponding url queue
 * @author Xiaoyu
 * 
 */
public class WorkersQueue {
	// fields
	public List<List<String>> workersQueue; // workerID: jobID
	
	// constructor
	public WorkersQueue() {
		this.workersQueue  = new ArrayList<>();
	}


	public List<String> getUrlQueue(int idx) {
		this.updateWorkerQueueSize(idx);
		List<String> urlQueue = new ArrayList<>();
		for(String url : workersQueue.get(idx))
			urlQueue.add(url);
		workersQueue.get(idx).clear();
		return urlQueue;
	}

	public void addUrlToQueue(int idx, String url){
		this.updateWorkerQueueSize(idx);
		this.workersQueue.get(idx).add(url);
	}
	//
	public List<String> validateNumOfWQueueIsSameAsNumOfW(int currentWorkersSize){
		if(currentWorkersSize == workersQueue.size())
			return null;
		List<String> urls = new ArrayList<>();
		for (List<String> queue : workersQueue){
			for(String url : queue)
				urls.add(url);
		}
		workersQueue = new ArrayList<>();
		return urls;
	}
	// check the idx is not out of bound due to the number of alive worker is not aligned with number of worker queue
	private void updateWorkerQueueSize(int idx){
		if(idx >= workersQueue.size()){
			for(int i = workersQueue.size(); i <= idx; i++)
				workersQueue.add(new ArrayList<>());
		}
		return;
	}
}
