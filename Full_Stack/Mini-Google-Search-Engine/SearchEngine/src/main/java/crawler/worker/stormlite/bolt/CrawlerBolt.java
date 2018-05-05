package crawler.worker.stormlite.bolt;

import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import crawler.master.store.DBWrapper;
import crawler.worker.Worker;
import crawler.worker.info.RobotsTxtInfo;
import crawler.worker.info.URLInfo;
import crawler.worker.storage.FrontierQueue;
import crawler.worker.storage.UrlObj;
import org.apache.log4j.Logger;

import crawler.worker.stormlite.OutputFieldsDeclarer;
import crawler.worker.stormlite.TopologyContext;
import crawler.worker.stormlite.routers.IStreamRouter;
import crawler.worker.stormlite.tuple.Fields;
import crawler.worker.stormlite.tuple.Tuple;
import crawler.worker.stormlite.tuple.Values;

// need to be tested 1. robot.txt connect
//

public class CrawlerBolt implements IRichBolt {

    // stormlite
    private static Logger log = Logger.getLogger(CrawlerBolt.class);
    private String executorId = UUID.randomUUID().toString();
    private Fields schema = new Fields(UrlObj.URL_OBJ, UrlObj.HOST_VALUE);
    private OutputCollector collector;

    // crawler regulation
    private Object delayLock;
    private int contentMaxLength;
    public String[] responseHeaders =
            {"Content-Type", "Content-Length","Last-Modified","Content-Language"};
    private DBWrapper dbw;

    // worker thread
    private WorkerThread workerThread;
    private FrontierQueue fq;

    // monitoring
    private InetAddress monitorHost;
    private DatagramSocket datagramSocket;


    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.contentMaxLength = Integer.parseInt(stormConf.get(Worker.CONTENT_MAX_LENGTH));
        this.delayLock = new Object();
        this.fq = FrontierQueue.getFqInstance();
        this.dbw = DBWrapper.getInstance("./WorkerDB");

        // for monitoring
        try{this.monitorHost = InetAddress.getByName("cis455.cis.upenn.edu");}
        catch(UnknownHostException e){e.printStackTrace();}
        try {this.datagramSocket = new DatagramSocket();
        } catch (SocketException e) { e.printStackTrace();}

        // thread for executing
        this.workerThread = new WorkerThread();
        workerThread.start();
    }

    @Override
    public void execute(Tuple input) {
        UrlObj urlObj = (UrlObj) input.getObjectByField(UrlObj.URL_OBJ);
        if (urlObj != null) {
            synchronized (fq) {
                fq.addObj(urlObj);
            }
        }
    }

    // id can be used as the assgined the hash value of url
    public class WorkerThread extends Thread {
        private FrontierQueue fq;

        public WorkerThread() {
            this.fq = FrontierQueue.getFqInstance();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    this.crawling();
                } catch (IllegalStateException | InterruptedException | UnsupportedEncodingException
                        | ProtocolException | MalformedURLException e) {
                    log.debug("Catch crawling exception");
                }
            }
        }

        private void crawling() throws InterruptedException, MalformedURLException, ProtocolException, UnsupportedEncodingException {
            UrlObj urlObj = fq.getUrlObj();
            if (urlObj == null) {
                // the queue is empty
                Thread.sleep(500);
                return;
            }

            // robots at first
            crawlRobotTxt(urlObj);
            try {
                crawlPage(urlObj);
            } catch (ParseException | IOException e) {
                log.debug(urlObj.getUrl() + " crawls fail, do not crawl");
            }
        }
        private void crawlRobotTxt(UrlObj urlObj){
            String requestUrl = urlObj.getUrl();
            URLInfo urlInfo = new URLInfo(requestUrl);
            String hostName = urlInfo.getHostName();
            if (FrontierQueue.robotsTxtInfo.containsHost(hostName)) return;
            requestUrl = urlInfo.getProtocal() + urlInfo.getHostName() + "/robots.txt";
            try{
                URL url = new URL(requestUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setDoOutput(true);
                connection.setRequestMethod("GET");
                connection.setRequestProperty("User-Agent", "cis455crawler");
                connection.setRequestProperty("Host", hostName);
                if (connection.getResponseCode() != 200) {
                    log.debug("Get Robots.txt error, url: " + requestUrl);
                    saveRobotsInfo(hostName, "");
                    return;
                } else {
                    byte[] bytes = readBodyFromInputStream(connection.getInputStream());
                    String content = new String(bytes, "UTF-8");
                    saveRobotsInfo(hostName, content);
                    log.debug(hostName + " robots.txt save");
                    return;

                }
            }catch(IOException e){
                try {
                    saveRobotsInfo(hostName, "");
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

        }

        private void crawlPage(UrlObj urlObj) throws ParseException, InterruptedException, IOException {
            String requestUrl = urlObj.getUrl();
            URLInfo urlInfo = new URLInfo(requestUrl);
            String hostName = urlInfo.getHostName();

            // sendHead: return can crawl
            if (urlObj.getMethod().equals("HEAD")) {
                    if (!sendHead(urlObj)){
                        return;
                    }
                    else {
                        urlObj.setHeadToGet();
                        sendGet(urlObj);
                    }
            } else if (urlObj.isGetFromS3()) {
                // not yet implement for getting lastcrawl time from s3
                log.debug(requestUrl + " should never get in");
            }
//            else if (urlObj.getMethod().equals("GET")) {
//                sendGet(urlObj);
//                Worker.downloadCount.addAndGet(1);
//
//            }
            else {
                log.debug(requestUrl + " has exception not handled ");
            }
            // monitor
            byte[] data = ("G09"+";"+requestUrl).getBytes();
            DatagramPacket packet = new DatagramPacket(data, data.length, monitorHost, 10455);
            try {datagramSocket.send(packet);
            } catch (IOException e) {
                log.error("Cannot report to monitor");
            }
        }


        // save the robots.txt into robotsInfo object
        public void saveRobotsInfo(String hostName, String content) throws IOException {
            FrontierQueue.robotsTxtInfo.addHost(hostName);
            if (content == null || content.equals(""))
                return;
            //finding whether exists cis455crawler
            BufferedReader reader = new BufferedReader(new StringReader(content));
            String line = reader.readLine();
            while (line != null) {
                line = line.trim();
                if (line.contains("cis455crawler"))
                    break;
                line = reader.readLine();
            }

            // otherwise find *
            if (line == null) {
                reader = new BufferedReader(new StringReader(content));
                line = reader.readLine();
                while (line != null) {
                    String lowercaseLine = line.trim().toLowerCase();
                    if (lowercaseLine.startsWith("user-agent")) {
                        String agent = line.substring(line.indexOf(":") + 1).trim();
                        if (agent.equals("*"))
                            break;
                    }
                    line = reader.readLine();
                }
            }
            if (line == null)
                return;
            // Start parsing and saving to robotTxtInfo
            // read again is because now the line is at user line
            line = reader.readLine();
            if(line !=  null) line = line.trim();
            while (line != null && !line.equals("")) {
                line = line.trim();
                String lowercaseLine = line.toLowerCase().trim();
                if (lowercaseLine.startsWith("disallow")) {
                    String path = line.substring(line.indexOf(":") + 1).trim();
                    FrontierQueue.robotsTxtInfo.addDisallowedLink(hostName, path);
                }
                if (lowercaseLine.startsWith("crawl-delay")) {
                    int delay = Integer.parseInt(line.substring(line.indexOf(":") + 1).trim());
                    FrontierQueue.robotsTxtInfo.addCrawlDelay(hostName, delay);
                }
                if (lowercaseLine.startsWith("allow")) {
                    String path = line.substring(line.indexOf(":") + 1).trim();
                    FrontierQueue.robotsTxtInfo.addAllowedLink(hostName, path);
                }
                if (lowercaseLine.startsWith("sitemap")) {
                    String path = line.substring(line.indexOf(":") + 1).trim();
                    FrontierQueue.robotsTxtInfo.addSitemapLink(path);
                }
                line = reader.readLine();
            }
            return;
        }

        public void sendGet(UrlObj urlObj) throws IOException {
            String requestUrl = urlObj.getUrl();
            String hostName = urlObj.getHost();
            URL url = new URL(requestUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", "cis455crawler");
            connection.setRequestProperty("Host", hostName);
            int statusCode = connection.getResponseCode();
            if (statusCode == 200) {
                String contentType = connection.getHeaderField("Content-Type");
                byte[] content = readBodyFromInputStream(connection.getInputStream());
                UrlObj urlObjFromGet = new UrlObj(requestUrl, content, contentType);
                collector.emit("PARSER_BOLT", new Values<Object>(urlObjFromGet, String.valueOf(urlObjFromGet.getHostHashVal())));
            } else {
                log.debug(requestUrl + " fails to issue get, status code " + statusCode);
            }
            connection.disconnect();
        }


        // validate with 301
        public boolean sendHead(UrlObj urlObj) throws UnknownHostException, IOException, ParseException, InterruptedException {
            String requestUrl = urlObj.getUrl();
            // Date lastCheckDate = get url from s3
            Date lastCheckDate = null;
            URLInfo urlInfo = new URLInfo(requestUrl);
            String filePath = urlInfo.getFilePath();
            String hostName = urlInfo.getHostName();

            // test robot.txt canCrawl
            List<String> dLinks = FrontierQueue.robotsTxtInfo.getDisallowedLinks(hostName);
            if (dLinks != null && dLinks.size() != 0) {
                for (String dLink : dLinks) {
                    if (filePath.startsWith(dLink)) {
                        log.error(requestUrl + " is not allowed to crawled");
                        return false;
                    }
                }
            }
            // wait for crawl-delay
            FrontierQueue.waitForCrawlDelay(hostName);
            log.info(hostName + " needs to wait for crawl-delay");


            // issue head requeset
            URL url = new URL(requestUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("HEAD");
            connection.setRequestProperty("User-Agent", "cis455crawler");
            connection.setRequestProperty("Host", hostName);
            int statusCode = connection.getResponseCode();
            if (statusCode != 200 && statusCode != 301 && statusCode != 303 && statusCode != 302) {
                log.error(requestUrl + " fails to issue head, status code " + statusCode);
                return false;
            }

            Map<String, List<String>> map = connection.getHeaderFields();
            connection.disconnect();
            if (statusCode == 301 || statusCode == 303 || statusCode == 302) {
                String value = flattenHeaderList("Location", map);
                log.error("One page from " + hostName +  " is moved to other place, trace down");
                if(value != null){
                    UrlObj urlObjRedirect = new UrlObj(value);
                    urlObjRedirect.addLevel(urlObj.getLevel()+1);
                    if(urlObjRedirect.getLevel()<5){
                        if(dbw.hasSeen(urlObjRedirect))
                            return false;
                        else {
                            sendHead(urlObjRedirect);
                        }
                    }
                    else{
                        log.error(urlObjRedirect.getHost() + " too deep to redirect");
                        return false;
                    }
                }
                return false;
            }
            for (String header : responseHeaders) {
                String value = flattenHeaderList(header, map);
                // value == null does not mean it is not a good page
                //if (value == null){
                //    log.debug(requestUrl +" header: " + header + " value is null ");
                //    return false;
                //}
                if (header.equals("Last-Modified")) {
                    Date lastModified = transformStringToDate(value);
                    if (lastCheckDate != null && lastModified != null && lastCheckDate.after(lastModified)) {
                        urlObj.setGetFromS3();
                        log.error(requestUrl + " is not modified");
                        return false;
                    }
                }
                if (header.equals("Content-Type")) {
                    if (!isWantedType(value)) {
                        log.error(requestUrl + " is not the wanted type: " + value);
                        return false;
                    }
                }
                if (header.equals("Content-Length")) {
                    if (value != null && Integer.parseInt(value) > contentMaxLength) {
                        log.error(requestUrl + " is too beg to crawl");
                        return false;
                    }
                }
                if (header.equals("Content-Language")){
                    if(value != null && !value.contains("en")){
                        log.error(requestUrl + " is not english page");
                    }
                }
            }
            return true;
        }
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
    public void cleanup() {
    }

    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return this.schema;
    }

    // helper function for reading https
    private byte[] readBodyFromInputStream(InputStream inputStream) throws IOException {
        return readFully(inputStream).toByteArray();
    }

    private ByteArrayOutputStream readFully(InputStream inputStream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length = 0;
        while ((length = inputStream.read(buffer)) != -1) {
            baos.write(buffer, 0, length);
        }
        return baos;
    }

    private String flattenHeaderList(String key, Map<String, List<String>> map) {
        List<String> list = map.get(key);
        if (list == null) return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            if (i == list.size() - 1)
                sb.append(list.get(i));
            else
                sb.append(list.get(i) + ",");
        }
        return sb.toString();
    }

    public Date transformStringToDate(String s) throws ParseException {
        if(s == null) return null;
        DateFormat format = null;
        Date date = null;
        s = s.trim();
        /*Friday, 31-Dec-99 23:59:59 GMT*/
        if (s.indexOf('-') >= 0) {
            format = new SimpleDateFormat("EEEE, d-MMM-yy HH:mm:ss z", Locale.ENGLISH);
            date = format.parse(s);
        }
        /*Fri, 31 Dec 1999 23:59:59 GMT*/
        else if (s.indexOf(',') >= 0) {
            format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
            date = format.parse(s);
        }
        /*Fri Dec 31 23:59:59 1999*/
        else {
            format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy", Locale.ENGLISH);
            try {
                date = format.parse(s);
            } catch (ParseException e) {
                System.out.println("cannot parse, but ignore");
            }
        }
        return date;
    }

    public boolean isWantedType(String contentType) {
        if (contentType == null)
            return false;
        contentType = contentType.trim();
        String[] contentTypes = new String[]{"text/html", "image","pdf"};
        for (String c : contentTypes) {
            if (contentType.contains(c)) return true;
        }
        return false;
    }
}
