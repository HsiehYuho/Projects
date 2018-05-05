package edu.upenn.cis455.mapreduce.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.bolt.PrintBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import org.apache.log4j.Logger;
import edu.upenn.cis.stormlite.spout.WordFileSpout;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.*;
import javax.servlet.http.*;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class MasterServlet extends HttpServlet {

    // active works send heartbeat periodically
    private class WorkerInfo {
        int port;
        String status;
        String job;
        int keysRead;
        int keysWritten;
        String result;
        long timestamp;

        public WorkerInfo(int port, String status, String job, int keysRead, int keysWritten, String result, long time) {
            this.port = port;
            this.status = status;
            this.job = job;
            this.keysRead = keysRead;
            this.keysWritten = keysWritten;
            this.result = result;
            this.timestamp = time;
        }
    }

    // key: ipAndPortOfWorker, value: worker info
    private static ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    static final long serialVersionUID = 455555001;

    static Logger log = Logger.getLogger(MasterServlet.class);

    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCE_BOLT = "REDUCE_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    public static boolean workersReady(int expected) {
        if (workers.size() >= expected) {
            return true;
        } else {
            return false;
        }
    }

    /** Create topology for the new job. */
    static void createMapReduce(String jobClassName,
                                String inputDir,
                                String outputDir,
                                String numOfMapThreads,
                                String numOfReduceThreads) throws IOException {

        // create new config
        Config config = new Config();

        // Job name
        config.put("job", jobClassName);

        // Numbers of executors (per node)
        config.put("spoutExecutors", "1");
        config.put("mapExecutors", numOfMapThreads);
        config.put("reduceExecutors", numOfReduceThreads);

        // set input and output directory (relative)
        config.put("inputDir", inputDir);
        config.put("outputDir", outputDir);

        // Complete list of workers, comma-delimited
        StringBuilder workerList = new StringBuilder("[");
        String prefix = "";
        for (String worker: workers.keySet()) {
            workerList.append(prefix);
            workerList.append(worker);
            prefix = ",";
        }
        workerList.append("]");
        config.put("workerList", workerList.toString());

        if (workerList.toString().equals("[]")) {
            System.out.println("No worker now");
            return;
        }

        log.info("master add worker list: " + workerList.toString());
        log.info("************ Creating the job request ***************");

        FileSpout spout = new WordFileSpout();
        MapBolt mapbolt = new MapBolt();
        ReduceBolt reducebolt = new ReduceBolt();
        PrintBolt printer = new PrintBolt();

        TopologyBuilder builder = new TopologyBuilder();

        // Only one source ("spout") for the words
        builder.setSpout(WORD_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));

        // Parallel mappers, each of which gets specific words
        builder.setBolt(MAP_BOLT, mapbolt, Integer.valueOf(config.get("mapExecutors"))).fieldsGrouping(WORD_SPOUT, new Fields("value"));

        // Parallel reducers, each of which gets specific words
        builder.setBolt(REDUCE_BOLT, reducebolt, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));

        // Only use the first printer bolt for reducing to a single point
        //builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);
        builder.setBolt(PRINT_BOLT, printer, 1).shuffleGrouping(REDUCE_BOLT);

        Topology topo = builder.createTopology();

        WorkerJob job = new WorkerJob(topo, config);

        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        try {
            String[] addresses = WorkerHelper.getWorkers(config);

            int i = 0;
            for (String dest: addresses) {
                config.put("workerIndex", String.valueOf(i++));
                if (sendJob(dest, "POST", config, "definejob",
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() !=
                        HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job definition request failed");
                }
            }

            for (String dest: addresses) {
                if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() !=
                        HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job execution request failed");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Server internal error");
            //System.exit(0);
        }
    }

    /** Send request. */
    public static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
        URL url = new URL(dest + "/" + job);

        log.info("Sending request to " + url.toString());

        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);

        if (reqType.equals("POST")) {
            conn.setRequestProperty("Content-Type", "application/json");
            OutputStream os = conn.getOutputStream();
            byte[] toSend = parameters.getBytes();
            os.write(toSend);
            os.flush();
        } else
            conn.getOutputStream();

        return conn;
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

        PrintWriter out = response.getWriter();
        String uri = request.getRequestURI();

        if (uri.equals("/submitjob")) {
            String jobClassName = request.getParameter("classname").trim();
            String inputDir = request.getParameter("inputdir").trim();
            String outputDir = request.getParameter("outputdir").trim();
            String numOfMapThreads = request.getParameter("#mapthreads").trim();
            String numOfReduceThreads = request.getParameter("#reducethreads").trim();
            createMapReduce(jobClassName, inputDir, outputDir, numOfMapThreads, numOfReduceThreads);
            out.println("Job submitted!");
        }
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

        PrintWriter out = response.getWriter();
        String uri = request.getRequestURI();

        if (uri.equals("/workerstatus")) {
            log.info("Received heartbeat: " + request.getRemoteAddr() + ":" + request.getRemotePort() + new Date());
            int port, keysRead,keysWritten;
            String status, job, result;
            String workerIP = request.getRemoteAddr();  // get sender ip (= worker ip here)
            long time = new Date().getTime();
            try {
                port = Integer.parseInt(request.getParameter("port"));
                status = request.getParameter("status");
                job = request.getParameter("job");
                keysRead = Integer.parseInt(request.getParameter("keysRead"));
                keysWritten = Integer.parseInt(request.getParameter("keysWritten"));
                result = request.getParameter("result");
                WorkerInfo workerInfo = new WorkerInfo(port, status, job, keysRead, keysWritten, result, time);
                workers.put(workerIP + ":" + String.valueOf(port), workerInfo);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error parsing worker info");
            }
        } else if (uri.equals("/status")) {
            out.print(statusPage());
        } else if (uri.equals("/shutdown")) {
            for (String dest: workers.keySet()) {
                log.debug("closing: " + dest);
                if (!dest.startsWith("http")) {
                    dest = "http://" + dest;
                }
                try {
                    sendJob(dest, "POST", null, "shutdown", "").getResponseCode();
                } catch (Exception e) {
                    System.out.println("Worker shutdown!");
                }
            }
            workers.clear();
            out.println("Workers shutdown");
        } else {
            out.print(statusPage());
        }
    }

    /** Prepare status page. */
    private String statusPage() {
        String page = "<!DOCTYPE html><html><body>\n" +
                "<h2>Name: Lanqing Yang (lanqingy)</h2>\n" +
                "<h2>Worker Status</h2>\n" +
                "<p><table>\n" +
                "<col width=\"100\">\n" +
                "<col width=\"100\">\n" +
                "<col width=\"100\">\n" +
                "<col width=\"100\">\n" +
                "<col width=\"100\">\n" +
                "<col width=\"100\">\n" +
                "<col width=\"100\">\n" +
                "<br>" +
                "<head><tr>" +
                "<th><b>Worker</b></th>" +
                "<th><b>Port</b></th>" +
                "<th><b>Status</b></th>" +
                "<th><b>Job</b></th>" +
                "<th><b>KeysRead</b></th>" +
                "<th><b>KeysWritten</b></th>" +
                "<th><b>Results</b></th>" +
                "</tr></head>";
        long curTime = new Date().getTime();
        List<String> inactiveWorkers = new ArrayList<>();
        for (String w : workers.keySet()) {
            if (workers.get(w).timestamp < curTime - 30000) {
                inactiveWorkers.add(w);
            } else {
                page += "<tr><td><center>" + w + "</center></td>" +
                        "<td><center>" + workers.get(w).port + "</center></td>" +
                        "<td><center>" + workers.get(w).status + "</center></td>" +
                        "<td><center>" + workers.get(w).job + "</center></td>" +
                        "<td><center>" + workers.get(w).keysRead + "</center></td>" +
                        "<td><center>" + workers.get(w).keysWritten + "</center></td>" +
                        "<td><center>" + workers.get(w).result + "</center></td></tr>";
            }
        }

        // remove inactive workers
        if (inactiveWorkers != null && inactiveWorkers.size() > 0) {
            for (String w: inactiveWorkers) {
                workers.remove(w);
            }
        }
        page += "<br></table><p><br>" +
                "<h2>Submit Job</h2>\n" +
                "<form action=\"/submitjob\" method=\"post\">\n" +
                "Class name:<br>\n" +
                "<input type=\"text\" name=\"classname\" placeholder=\"edu.upenn.cis.cis455.mapreduce.job.MyJob\" required>\n" +
                "<br><br>\n" +
                "Input directory<br>\n" +
                "<input type=\"text\" name=\"inputdir\" placeholder=\"/input\" required>\n" +
                "<br><br>\n" +
                "Output directory<br>\n" +
                "<input type=\"text\" name=\"outputdir\" placeholder=\"/output\" required>\n" +
                "<br><br>\n" +
                "# Map threads<br>\n" +
                "<input type=\"text\" name=\"#mapthreads\" placeholder=\"1\" required>\n" +
                "<br><br>\n" +
                "# Reduce threads<br>\n" +
                "<input type=\"text\" name=\"#reducethreads\" placeholder=\"1\" required>\n" +
                "<br><br>\n" +
                "<input type=\"submit\" value=\"Submit\">\n" +
                "</form>\n" +
                "<br>\n" +
                "</form></body></html>";
        return page;
    }

}
  
