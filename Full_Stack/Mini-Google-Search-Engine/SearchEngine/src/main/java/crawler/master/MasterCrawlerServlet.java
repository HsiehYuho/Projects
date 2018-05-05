package crawler.master;

//import javax.servlet.http.HttpServlet;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.Enumeration;
//
//public class MasterCrawlerServlet extends HttpServlet {
//    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
//        String pathInfo = request.getPathInfo();
//        if(pathInfo!= null && pathInfo.equals("/report")){
//            String id = request.getParameter("id") ;
//            String qSize = request.getParameter("qSize");
//            String port = request.getParameter("port");
//            return;
//        }
//    }
//}

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import crawler.master.store.DBWrapper;
import crawler.master.store.UrlBatchFile;
import crawler.worker.info.URLInfo;
import crawler.worker.storage.UrlObj;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.*;

/**
 * This is the main class of master, who dispatch job to workers
 * and put unseen url into Berkeley DB, which serves as the disk
 * 
 * @author Xiaoyu 
 *
 */
public class MasterCrawlerServlet extends HttpServlet {
	private static Logger log = Logger.getLogger(MasterCrawlerServlet.class);

	// fields
	public int queueSizeLimit = 500; // if > 500, store them in BDB in a file
	public static Queue<String> frontierQueue = new LinkedList<>(); // size = queueSize
	public int threshold = 250; // threshold for dispatching job to certain worker
	public long minReportInteval = 30000; // Worker must report in certain to be considered as alive, 30s
	public boolean working = true; // flag of whether to shutdown master

	// dispatch urls list to workers
	private WorkersQueue workersQueue = new WorkersQueue();
	private List<WorkerInfo> workers = new ArrayList<WorkerInfo>(); // store alive worker object
	private AtomicInteger totalSentOut = new AtomicInteger(0);

	DBWrapper dbStore = DBWrapper.getInstance("./MasterDB");

	// TODO extract dbFileFrom local db at first in case master is shutted down

    /**
     * Get size, id and location (ip + port number) of all alive workers;
     * Dispatch corresponding job to active workers with size < threshold
     * 
     * @param request, response
     */
	@Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
			String pathInfo = request.getPathInfo();
			if (pathInfo.equals("/report")) {
				int qSize = Integer.parseInt(request.getParameter("qSize")); // size of per worker
				String workerID = request.getParameter("id"); // worker id
				String workerPort = request.getParameter("port"); // worker port
				String ip = request.getRemoteAddr(); // worker ip

				// update worker last report time or create new worker
				boolean found = false;
				for(WorkerInfo w : workers){
					if(w.getId().equals(workerID)){
						w.updateLastReport();
						w.setQSize(qSize);
						found = true;
					}
				}
				if(!found){
					WorkerInfo worker = new WorkerInfo(workerID,workerPort, ip, qSize);
					workers.add(worker);

				}

				// remove the inactive worker
				removeInactiveWorker();

				// dispatch urls to corresponding workers
				if(qSize < threshold)
					dispatchJob(workerID);
				else
					log.info(workerID + " is still working on its on urls");
				return;
			}
			// for remote - monitoring
			if(pathInfo.equals("/getReport")){
				PrintWriter pw = response.getWriter();
				StringBuilder sb = new StringBuilder();
				int totalAliveWorker = workers.size();
				sb.append("Total alive worker : " + totalAliveWorker + "\n");
				sb.append("Have sent out urls number: " + totalSentOut.get() + "\n");
				sb.append("Current db files number: " + dbStore.getFilekeyNumber() + "\n");
				for(int i = 0; i < workers.size(); i++){
					sb.append("Idx: " + i + " worker has " + workers.get(i).getQSize() + "\n");
				}
				pw.println(sb.toString());
				return;
			}
	}
	
    /**
     * Put unseen URLs from workers into BDB or frontierQueue;
     * 
     * @param request, response
     */
	@Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
    	       throws java.io.IOException {
		InputStream inputStream = request.getInputStream();
		PrintWriter pw = response.getWriter();
		String pathInfo = request.getPathInfo();
		if(pathInfo.equals("/sendBackUrls")){
			if (inputStream != null) {
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				String url;
				while ((url = br.readLine()) != null) {
					// already exist TODO: we can implement cache for more efficient look-up
					UrlObj urlObj = new UrlObj(url);
					if (dbStore.hasSeen(urlObj)) continue;

					// add into frontierQueue
					if (frontierQueue.size() < queueSizeLimit)
					    frontierQueue.add(url);
					// or put into BDB
					else {
						StringBuilder sb = new StringBuilder();
						for(String urlFromFq : frontierQueue)
							sb.append(urlFromFq);
						Long key = Calendar.getInstance().getTime().getTime();
						UrlBatchFile urlBatchFile = new UrlBatchFile(key);
						urlBatchFile.addConent(sb.toString());
						frontierQueue.clear();
						dbStore.storeFileToDb(urlBatchFile);
					}
					log.debug("Receive url: " + url + " from worker sendBackUrls");
				}
			}
			pw.println("Receive post, thank you");
			return;
		}
		pw.println("Not received post, sorry");

	}
    

    /**
     * Retrieve urls from DB or frontier queue, add them to corresponding queue,
	 * then dispatch the certain queue to certain worker
     * @param workerID
     */
    public void dispatchJob(String workerID) {
    	WorkerInfo worker = null;
    	int workerIdx = -1;
    	for(int i = 0; i < workers.size(); i++){
			WorkerInfo w = workers.get(i);
    		if(w.getId().equals(workerID)) {
				worker = w;
				workerIdx = i;
				break;
			}
		}
		if(worker == null || workerIdx == -1){
    		log.error("WorkerID: " + workerID + " is not in workersInfo");
    		return;
		}

		int totalAliveWorker = workers.size();

		// before adding urls to queue, we need to validate the size of queue and alive workers
		List<String> extractUrls = workersQueue.validateNumOfWQueueIsSameAsNumOfW(totalAliveWorker);
		if(extractUrls != null){
			for(String url : extractUrls){
				URLInfo urlInfo = new URLInfo(url);
				int idx = urlInfo.getHostName().hashCode() % totalAliveWorker;
				workersQueue.addUrlToQueue(idx,url);
			}
		}

		// if key extracted from db size == 0, directly retrieve urls from frontier queue
		// may only happen during very early stage
        int dispatchUrlNum = 0;
		Long key = dbStore.getFirstFileKey();
		if(key == null){
			for(String url : frontierQueue){
				URLInfo urlInfo = new URLInfo(url);
				int idx = urlInfo.getHostName().hashCode() % totalAliveWorker;
				workersQueue.addUrlToQueue(idx,url);
			    dispatchUrlNum++;
			}
			totalSentOut.addAndGet(frontierQueue.size());
			frontierQueue.clear();
			log.debug("Get urls from master frontier queue");
		}
		// or get files from db then added to corresponding queue
		else{
			String contentFromDb = dbStore.getAndRemoveFile(key);
			Scanner scanner = new Scanner(contentFromDb);
			while (scanner.hasNextLine()) {
				String url = scanner.nextLine();
				URLInfo urlInfo = new URLInfo(url);
				int idx = urlInfo.getHostName().hashCode() % totalAliveWorker;
				workersQueue.addUrlToQueue(idx,url);
				totalSentOut.addAndGet(1);
                dispatchUrlNum++;
			}
			scanner.close();
            log.debug("Get urls from bdb");
		}

		// dispatch job
		String wholeIp = worker.getWholeIP();
		StringBuilder sb = new StringBuilder();
        List<String> urlList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        List<String> urlQueue = workersQueue.getUrlQueue(workerIdx);
		for(String url : urlQueue){
            urlList.add(url);
//			sb.append(url+"\n");
		}
//		String postBody = "\n" + sb.toString();
//        String postBody = "test";
//        log.debug("-----------");
//		log.debug(postBody);
//        log.debug("-----------");

        String postBody = null;
        try {
            postBody = mapper.writeValueAsString(urlList);
        } catch (JsonProcessingException e) {
            log.error("Write string as post body goes wrong");
            e.printStackTrace();
        }

        try{
            byte[] btyes = postBody.getBytes();
            URL urlToWorker = new URL("http://"+wholeIp+"/workerCrawler/assignJobs");
			HttpURLConnection conn = (HttpURLConnection) urlToWorker.openConnection();
			conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			conn.setRequestProperty("Content-Length", Integer.toString(btyes.length));
			conn.setDoOutput(true);
            conn.getOutputStream().write(btyes);
            int statusCode = conn.getResponseCode();
            conn.disconnect();
            if(statusCode != 200){
                log.error("Master dispatches  urls to " + workerIdx +  " worker fail, fail code + " + statusCode);
            }
			else
				log.info("Send "+ dispatchUrlNum + " to "+ workerIdx + " worker");
		}catch (IOException e){
			e.printStackTrace();
		}
	}


    // remove inactive worker
	private void removeInactiveWorker(){
    	List<Integer> removeIdx = new ArrayList<>();
    	for(int i = 0; i < workers.size(); i++){
    		WorkerInfo w = workers.get(i);
    		if((Calendar.getInstance().getTime().getTime() - w.getLastReport()) > minReportInteval)
				removeIdx.add(i);
		}
		// we have to move it backward to avoid mis-idx
		if(removeIdx.size() != 0){
    		for(int i = removeIdx.size()-1; i >= 0; i--){
    			int idx = removeIdx.get(i);
    			workers.remove(idx);
			}
		}
		return;
	}

   
    
}
