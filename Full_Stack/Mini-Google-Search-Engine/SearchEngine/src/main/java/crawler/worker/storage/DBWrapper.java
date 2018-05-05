//package crawler.worker.storage;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//
//import com.sleepycat.je.DatabaseException;
//import com.sleepycat.je.Environment;
//import com.sleepycat.je.EnvironmentConfig;
//import com.sleepycat.persist.EntityCursor;
//import com.sleepycat.persist.EntityStore;
//import com.sleepycat.persist.PrimaryIndex;
//import com.sleepycat.persist.StoreConfig;
//
//
//
//
//public class DBWrapper {
//    private static String envDirectory = null;
//    private static Environment myEnv;
//    private static EntityStore store;
//    private PrimaryIndex<String,PageRankObj> prObjIdx;
//    public DBWrapper(){}
//    // run and shutdown
//    public void run(String envDirectory){
//        // if file not exists, create it
//        File f = new File(envDirectory);
//        if (!f.exists())
//            f.mkdirs();
//        EnvironmentConfig envConfig = new EnvironmentConfig();
//        StoreConfig storeConfig = new StoreConfig();
//        envConfig.setAllowCreate(true);
//        storeConfig.setAllowCreate(true);
//        this.myEnv = new Environment(new File(envDirectory),envConfig);
//        this.store = new EntityStore(myEnv, "cis555", storeConfig);
//        this.prObjIdx = this.store.getPrimaryIndex(String.class, PageRankObj.class);
//    }
//
//    public void shutdown(){
//        if(store != null) store.close();
//        if(myEnv != null) myEnv.close();
//    }
//    public void createPageRankObj(String url,List<String> links){
//        PageRankObj prObj = new PageRankObj(url);
//        for(String l : links)
//            prObj.addOutBoundLinks(l);
//        this.prObjIdx.put(prObj);
//        myEnv.sync();
//    }
//    public String[] getAllUrls(){
//        EntityCursor<String> keys = this.prObjIdx.keys();
//        List<String> urls = new ArrayList<>();
//        for(String key : keys){
//            urls.add(key);
//        }
//        keys.close();
//        return urls.toArray(new String[urls.size()]);
//
//    }
//
//
//}
