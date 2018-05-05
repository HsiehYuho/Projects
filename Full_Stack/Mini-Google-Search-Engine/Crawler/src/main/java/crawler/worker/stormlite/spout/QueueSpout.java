package crawler.worker.stormlite.spout;

import java.util.Map;
import java.util.UUID;
import crawler.worker.storage.FrontierQueue;
import crawler.worker.storage.UrlObj;
import org.apache.log4j.Logger;


import crawler.worker.stormlite.OutputFieldsDeclarer;
import crawler.worker.stormlite.TopologyContext;
import crawler.worker.stormlite.routers.IStreamRouter;
import crawler.worker.stormlite.tuple.Fields;
import crawler.worker.stormlite.tuple.Values;

public class QueueSpout implements IRichSpout{
	static Logger log = Logger.getLogger(QueueSpout.class);
    String executorId = UUID.randomUUID().toString();
	SpoutOutputCollector collector;
    Fields schema = new Fields(UrlObj.URL_OBJ,UrlObj.HOST_VALUE);
    FrontierQueue fq;
	public QueueSpout(){
		log.debug("Init Queue Spout");
	}
	
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);		
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.fq = FrontierQueue.getFqInstance();
	}

	@Override
	public void close() {
	}

	// should be no-blocking
	@Override
	public void nextTuple() {
        UrlObj obj = fq.getUrlObj();
		if(obj != null){
			this.collector.emit(new Values<Object>(obj,String.valueOf(obj.getHostHashVal())));
		}
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

}
