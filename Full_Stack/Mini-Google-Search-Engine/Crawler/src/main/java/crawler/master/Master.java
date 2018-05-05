package crawler.master;

import crawler.JettyServer;
import crawler.master.store.DBWrapper;
import crawler.master.store.UrlBatchFile;
import crawler.worker.Worker;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class Master {
    private static Logger log = Logger.getLogger(Worker.class);

    public Master(){}
    public void start() throws Exception {
        // start the server
        JettyServer js = new JettyServer();
        js.run("8080");

    }

    public static void main(String[] args) throws Exception {
        // write the seed into database if given
        if(args.length != 0){
            DBWrapper dbStore = DBWrapper.getInstance("./MasterDB");
            String seed = readFile(args[0]);
            UrlBatchFile urlBatchFile = new UrlBatchFile(Calendar.getInstance().getTime().getTime());
            urlBatchFile.addConent(seed);
            dbStore.storeFileToDb(urlBatchFile);
            log.info("Successfully read the seed file");
        }
        Master m = new Master();
        m.start();

    }
    private static String readFile(String fileName){
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new FileReader(fileName));
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }
}
