package crawler.worker.storage;

import crawler.worker.info.RobotsTxtInfo;

import java.util.*;

public class FrontierQueue {
    private static FrontierQueue fq = null;
    private Queue<UrlObj> bq;
    public static HashMap<String, Date> hostNextAvai = new HashMap<>();
    public static RobotsTxtInfo robotsTxtInfo = new RobotsTxtInfo();


    protected FrontierQueue(){
        this.bq = new PriorityQueue<UrlObj>(new PqComparator());

    }
    public static FrontierQueue getFqInstance(){
        if(fq == null)
            fq = new FrontierQueue();
        return fq;
    }
    public synchronized UrlObj getUrlObj(){
        return this.bq.poll();
    }
    public synchronized boolean addObj(UrlObj obj){
        if(obj != null){
            this.bq.offer(obj);
            return true;
        }
        else
            return false;
    }
    public int getFqSize(){
        return this.bq.size();
    }
    private class PqComparator implements Comparator<UrlObj> {

        @Override
        public int compare(UrlObj o1, UrlObj o2) {
            String host1 = o1.getHost();
            String host2 = o2.getHost();
            if (hostNextAvai.containsKey(host1) && hostNextAvai.containsKey(host2)) {
                if (hostNextAvai.get(host1).after(hostNextAvai.get(host2)))
                    return 1;
                else
                    return -1;
            }
            if (!hostNextAvai.containsKey(host1))
                return -1;

            if (!hostNextAvai.containsKey(host2))
                return 1;
            else
                return 0;
        }

    }
    public static void waitForCrawlDelay(String hostName) throws InterruptedException {
        if (hostNextAvai.containsKey(hostName)) {
            Date nextAvai = hostNextAvai.get(hostName);
            Date current = new Date();
            if (current.before(nextAvai)) {
                long millToSleep = nextAvai.getTime() - current.getTime();
                Thread.sleep(millToSleep);
            }
        }
        long crawlDelayMilliSecond = robotsTxtInfo.getCrawlDelay(hostName) * 1000;
        hostNextAvai.put(hostName, new Date(System.currentTimeMillis() + crawlDelayMilliSecond));
    }

}
