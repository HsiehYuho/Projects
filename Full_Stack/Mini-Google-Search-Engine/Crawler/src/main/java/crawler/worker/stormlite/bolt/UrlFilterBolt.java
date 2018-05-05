package crawler.worker.stormlite.bolt;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import crawler.master.store.DBWrapper;
import crawler.master.store.UrlBatchFileTmp;
import crawler.worker.Worker;
import crawler.worker.storage.FrontierQueue;
import crawler.worker.storage.UrlObj;
import org.apache.jasper.tagplugins.jstl.core.Url;
import org.apache.log4j.Logger;

import crawler.worker.stormlite.OutputFieldsDeclarer;
import crawler.worker.stormlite.TopologyContext;
import crawler.worker.stormlite.routers.IStreamRouter;
import crawler.worker.stormlite.tuple.Fields;
import crawler.worker.stormlite.tuple.Tuple;

public class UrlFilterBolt implements IRichBolt{
	static Logger log = Logger.getLogger(UrlFilterBolt.class);
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
	private int maxLevel = 10;
	private int folderDepth = 5;
	private String masterUrl;
	private AtomicInteger currentUrlsCount;
	private int maxUrls = 5;
	private StringBuilder sb;
	private FrontierQueue fq;
	private int fqMaxSize = 500;
	private DBWrapper dbw;
	private List<String> tmpFileBuffer;
	private AtomicInteger dbFileSize = new AtomicInteger(0);
	private HashMap<String,Integer> hostInBuffer;
	private int tmpFileBufferSize = 2500;
	private int individualHostInBufferMax = (int)(0.3 * tmpFileBufferSize);
	private Queue<Long> fileKeys;

	@Override
	public String getExecutorId() {
		return this.executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		return;
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		UrlObj urlObj = (UrlObj) input.getObjectByField(UrlObj.URL_OBJ);
		if(fq.getFqSize() < fqMaxSize){
			Long key = this.fileKeys.poll();
			if(key != null){
				List<String> newUrls = dbw.getAndRemoveTmpFile(key);
				for(String url : newUrls){
					UrlObj addUrlObj = new UrlObj(url);
					fq.addObj(addUrlObj);
					if(fq.getFqSize() > 5000)
						break;
				}
				dbFileSize.addAndGet(-1);
				log.info("Read file from local db, fq size: " + fq.getFqSize() + " dbFileSize: " + dbFileSize.get());
			}else{
				fq.addObj(urlObj);
			}
		}
		// control the percentage of different host in buffer file, cannot over 30%
		if(hostInBuffer.containsKey(urlObj.getHost())){
			if(hostInBuffer.get(urlObj.getHost()) >= individualHostInBufferMax)
				return;
		}
		// increase the host number by 1
		hostInBuffer.put(urlObj.getHost(),hostInBuffer.getOrDefault(urlObj.getHost(),0)+1);
		if(wantedFile(urlObj)){
			if(tmpFileBuffer.size() < tmpFileBufferSize){
				tmpFileBuffer.add(urlObj.getUrl());
			}
			else{
				Long key = Calendar.getInstance().getTime().getTime();
				this.fileKeys.offer(key);
				UrlBatchFileTmp urlBatchFileTmp = new UrlBatchFileTmp(key);
				synchronized (tmpFileBuffer){
					urlBatchFileTmp.addTmpConent(tmpFileBuffer);
					dbw.storeTmpFileToDb(urlBatchFileTmp);
					tmpFileBuffer.clear();
					hostInBuffer.clear();
					dbFileSize.addAndGet(1);
				}
				log.info("Put file into local db, dbFilesize: " + dbFileSize.get());
			}

			// if the sb is above max count
//			if(this.currentUrlsCount.get() >= maxUrls){
//				try {
//					URL url = new URL(this.masterUrl+"/masterCrawler/sendBackUrls");
//					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//					conn.setRequestMethod("POST");
//					conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
//					byte[] btyes = sb.toString().getBytes();
//					conn.setRequestProperty("Content-Length", String.valueOf(btyes.length));
//					conn.setDoOutput(true);
//					conn.getOutputStream().write(btyes);
//					int statusCode = conn.getResponseCode();
//					if(statusCode != 200)
//						log.error(sb.toString()+" send back to master fail");
//					else
//						log.info("send urls to master: " + sb.toString());
//					conn.disconnect();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//				sb = new StringBuilder();
//				currentUrlsCount = new AtomicInteger(0);
//			}
//			else{
//				String requestUrl = urlObj.getUrl();
//                sb.append(requestUrl+"\n");
//				currentUrlsCount.addAndGet(1);
//			}
		}
	}
	private boolean wantedFile(UrlObj urlObj){
		String url = urlObj.getUrl();
		// hasSeen
        if(dbw.hasSeen(urlObj)){
//        	log.info(urlObj.getUrl() + "has seen, not added");
            return false;
        }

	    // depth
		if(urlObj.getLevel() > maxLevel)
			return false;
		// not http or https
		if(url.startsWith("ftp"))
			return false;
		// to much folders
		if((url.length() - url.replaceAll("/", "").length()) > folderDepth)
			return false;
		
		// Get rid of certain country
		return true;
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.masterUrl = stormConf.get(Worker.MASTER_URL);
		if(!this.masterUrl.startsWith("http"))
			this.masterUrl = "http://" + this.masterUrl;
		this.currentUrlsCount = new AtomicInteger(0);
		this.sb = new StringBuilder();
		this.fq = FrontierQueue.getFqInstance();
		this.dbw = DBWrapper.getInstance("./WorkerDB");
		this.dbw.removeUrlBatchFileTmp();
		this.tmpFileBuffer = new ArrayList<>();
		this.hostInBuffer = new HashMap<>();
		this.fileKeys = new LinkedList<>();
	}

	@Override
	public Fields getSchema() {
		return null;
	}

}
