package crawler.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import crawler.JettyServer;
import crawler.worker.storage.FrontierQueue;
import crawler.worker.storage.UrlObj;
import crawler.worker.stormlite.Config;
import crawler.worker.stormlite.LocalCluster;
import crawler.worker.stormlite.Topology;
import crawler.worker.stormlite.TopologyBuilder;
import crawler.worker.stormlite.bolt.CrawlerBolt;
import crawler.worker.stormlite.bolt.ParserBolt;
import crawler.worker.stormlite.bolt.UrlFilterBolt;
import crawler.worker.stormlite.spout.QueueSpout;
import crawler.worker.stormlite.tuple.Fields;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker {
    private static Logger log = Logger.getLogger(Worker.class);
    private String executorId = UUID.randomUUID().toString();

    // private field
    private Map<String,String> workererConfig;
    public static AtomicInteger downloadCount = new AtomicInteger(0);
    private int contentMaxLength = 2 * 1024 * 1024; //byte
    private int maxDownloadNum = 30000; // download 100 files

    // spouts & bolts
    private static final String QUEUE_SPOUT = "QUEUE_SPOUT";
    private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
    private static final String PARSER_BOLT = "PARSER_BOLT";
    private static final String MATCHER_BOLT = "MATCHER_BOLT";
    private static final String URL_FILTERBOLT = "URL_FILTERBOLT";
    // public stormConfig variable
    public static final String CONTENT_MAX_LENGTH ="CONTNET_MAX_LENGTH";
    public static final String MASTER_URL = "MasterUrl";
    public static final String LOCAL_PORT = "LocalPort";


    public Worker(Map<String,String> workerConfig) throws InterruptedException {
        this.workererConfig = workerConfig;

    }

    public void start() throws Exception {
        // start reporting
//        ReportThread reportThread = new ReportThread(workererConfig);
//        reportThread.start();

        // start the server
        ServerThread serverThread = new ServerThread(workererConfig);
        serverThread.start();

        // start crawling topology
        Config stormConfig = new Config();
        stormConfig.put(CONTENT_MAX_LENGTH, String.valueOf(contentMaxLength));
        stormConfig.put(MASTER_URL, workererConfig.get(MASTER_URL));
        QueueSpout queueSpout = new QueueSpout();
        CrawlerBolt crawlerBolt = new CrawlerBolt();
        ParserBolt parserBolt = new ParserBolt();
        UrlFilterBolt urlFilterBolt = new UrlFilterBolt();

        // Storm/cluster init
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(QUEUE_SPOUT, queueSpout, 1);
        // Four parallel crawling, each of which gets module of hash value of host name value

        builder.setBolt(CRAWLER_BOLT, crawlerBolt, 5).shuffleGrouping(QUEUE_SPOUT);

        builder.setBolt(PARSER_BOLT, parserBolt, 5).shuffleGrouping(CRAWLER_BOLT);


        builder.setBolt(URL_FILTERBOLT, urlFilterBolt, 1).shuffleGrouping(PARSER_BOLT);

        LocalCluster cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
        try {
            String str = mapper.writeValueAsString(topo);
            log.debug("The StormLite topology is:\n" + str);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        cluster.submitTopology("Crawler", stormConfig,builder.createTopology());
        while(true){
            Thread.sleep(100000);
            int downloadNum = downloadCount.get();
            // the downloaded file number is smaller than max number
            if(downloadNum >= maxDownloadNum)
                break;
        }
        cluster.killTopology("Crawler");
        cluster.shutdown();
        System.exit(0);
    }

    // Report to Master Instance Periodically
    private class ReportThread extends Thread{
        private Map<String,String> config;
        private String masterUrl;
        private String localPort;
        private FrontierQueue fq;
        public ReportThread(Map<String,String> config){
            this.config = config;
            this.masterUrl = config.get(MASTER_URL);
            this.localPort = config.get(LOCAL_PORT);
            this.fq = FrontierQueue.getFqInstance();
            if(!this.masterUrl.startsWith("http"))
                this.masterUrl = "http://" + this.masterUrl;
        }
        @Override
        public void run(){
            URL url = null;
            HttpURLConnection connection = null;
            String requestUrl = "";
            while(true) {
                try {
                    int qSize = fq.getFqSize();
                    requestUrl = masterUrl + getWorkerStatus(qSize,localPort);
                    url = new URL(requestUrl);
                    connection = (HttpURLConnection) url.openConnection();
                    if (connection.getResponseCode() != 200)
                        log.error("Report Fails");
                } catch (IOException e) {
                    log.debug("Connection to Master Fails");
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private class ServerThread extends Thread{
        private Map<String,String> workererConfig;
        public ServerThread(Map<String,String> workererConfig){
            this.workererConfig = workererConfig;
        }
        @Override
        public void run(){
            JettyServer js = new JettyServer();
            try {js.run((String) workererConfig.get(LOCAL_PORT));
            } catch (Exception e) { e.printStackTrace(); }
        }
    }
    private String getWorkerStatus(int qSize,String localPort){
        String id = getExecutorId();
        return "/masterCrawler/report?id="+id+"&port="+localPort+"&qSize="+qSize;
    }
    private String getExecutorId(){
        return this.executorId;
    }

    // param: masterServerIp:port localport ex 127.0.0.1:8080 8001
    public static void main(String[] args) throws Exception {
        Map<String,String> config = new HashMap<>();
        config.put(MASTER_URL,"127.0.0.1:8080");
        config.put("LocalPort","8001");
        Worker w = new Worker(config);
        w.start();
    }

}
