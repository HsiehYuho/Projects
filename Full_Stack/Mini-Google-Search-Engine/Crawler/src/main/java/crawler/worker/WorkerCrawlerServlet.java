package crawler.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import crawler.worker.storage.FrontierQueue;
import crawler.worker.storage.UrlObj;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class WorkerCrawlerServlet extends HttpServlet {
    private static Logger log = Logger.getLogger(WorkerCrawlerServlet.class);
    private FrontierQueue fq = FrontierQueue.getFqInstance();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String pathInfo = request.getPathInfo();
        PrintWriter pw = response.getWriter();
        if(pathInfo.endsWith("/getReport")){
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            FrontierQueue fq = FrontierQueue.getFqInstance();
            pw.println(dateFormat.format(date));
            pw.println("The download count: " + Worker.downloadCount);
            pw.println("The size of fq: " + fq.getFqSize());
            return;
        }
        pw.println("Not match path, Oops!");



    }

        public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String pathInfo = request.getPathInfo();
        PrintWriter pw = response.getWriter();
        if(pathInfo!= null && pathInfo.equals("/assignJobs")){
            // need to read JSON object from master instance
            StringBuilder buffer = new StringBuilder();
            BufferedReader reader = request.getReader();
            String line;
            ObjectMapper mapper = new ObjectMapper();
            while ((line = reader.readLine()) != null) {
                if(line != null && line.trim().length() != 0){
                    UrlObj urlObj = new UrlObj(line);
                    fq.addObj(urlObj);
                    buffer.append(line);
                }
            }
            String data = buffer.toString();
            log.debug("Receive data: " + data);
            pw.println("Receive Data, Thanks");
            return;
        }
        pw.println("Not Receive Data, Oops!");

    }
}