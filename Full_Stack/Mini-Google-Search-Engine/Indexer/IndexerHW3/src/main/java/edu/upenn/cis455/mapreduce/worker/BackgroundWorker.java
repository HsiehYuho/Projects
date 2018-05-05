package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis.stormlite.TopologyContext;

import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.net.HttpURLConnection;

public class BackgroundWorker implements Runnable {

    String masterIPAndPort;
    String myPort;
    List<TopologyContext> contexts;

    BackgroundWorker(String masterIPAndPort, String myPort) {
        this.myPort = myPort;
        if (!masterIPAndPort.startsWith("http")) {
            masterIPAndPort = "http://" + masterIPAndPort;
        }
        this.masterIPAndPort = masterIPAndPort;
    }

    public void setContexts(List<TopologyContext> contexts) {
        this.contexts = contexts;
    }

    @Override
    public void run() {
        while (true) {
            try {
                URL url;
                if (contexts == null || contexts.size() == 0) {
                    url = new URL(masterIPAndPort + "/workerstatus?" +
                            "port=" + myPort + "&" +
                            "status=IDLE&" +
                            "job=null&" +
                            "keysRead=0&" +
                            "keysWritten=0");
                } else {
                        url = new URL(masterIPAndPort + "/workerstatus?" +
                        "port=" + myPort + "&" +
                        "status=" + contexts.get(contexts.size() - 1).getStatus() + "&" +
                        "job=" + contexts.get(contexts.size() - 1).getJob() + "&" +
                        "keysRead=" + String.valueOf(contexts.get(contexts.size() - 1).getKeysRead()) + "&" +
                        "keysWritten=" + String.valueOf(contexts.get(contexts.size() - 1).getKeysWritten()));
                }

                HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                conn.setDoOutput(true);

                // set method
                conn.setRequestMethod("GET");

                conn.setRequestProperty("Content-Type", "application/json");

                if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job definition request failed");
                }

                System.out.println("Heartbeat: " + url);

                // send heartbeat every 10 seconds
                Thread.sleep(10000);
            } catch (Exception e) {
                // System.out.println("Error sending heartbeat to master");
            }
        }
    }
}
