package crawler.master;

import java.util.Calendar;
import java.util.List;

/**
 * Class to store progress information about worker
 * @author Xiaoyu Deng
 *
 */
public class WorkerInfo {
	private long lastReport;
	private int qSize; // FQ size of worker
	private String wholeIp; // localhost:8080
	private String id;

	public WorkerInfo(String id, String port, String ip, int qSize) {
		this.id = id;
		this.wholeIp = ip + ":" + port;
		this.setQSize(qSize);
		updateLastReport();
	}
	public String getId(){
		return this.id;
	}

	public void setQSize(int qSzie) {
		this.qSize = qSize;
	}
	
	public int getQSize() {
		return qSize;
	}

	public String getWholeIP() {
		return wholeIp;
	}
	
	public long getLastReport() {
		return lastReport;
	}

	public void updateLastReport() {
		this.lastReport = Calendar.getInstance().getTime().getTime();
	}
	
	
}
