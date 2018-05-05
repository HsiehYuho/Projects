package crawler.master.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import crawler.worker.storage.UrlObj;


/**
 * Basic class of BDB instance
 * @author Xiaoyu
 *
 */
public class DBWrapper {
	private static String envDirectory = null;
	private static Environment myEnv; // environment
	private static EntityStore store; // BDB
	private static DBWrapper DBinstance = null;
	private PrimaryIndex<Long, UrlBatchFile> urlBatchIdx;
	private PrimaryIndex<String, UrlInDbByHost> urlInDbByHostIdx;
	private PrimaryIndex<Long, UrlBatchFileTmp> urlBatchTmpIdx;
	// constructor
	private DBWrapper(String envDirectory){
		this.envDirectory = envDirectory;
		try {
			// Create new entity environment object
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true); // Create new myEnv if it does not exist
			envConfig.setTransactional(true); // Allow transactions in new myEnv;
			File dir = new File(this.envDirectory); // specify directory where the environment resides
			if (!dir.exists()) {
				dir.mkdir();
				dir.setReadable(true); 
				dir.setWritable(true); 
			}
			myEnv = new Environment(dir, envConfig);
			
			/* Create new entity store object */
			StoreConfig storeConfig = new StoreConfig();
			storeConfig.setAllowCreate(true);
			storeConfig.setTransactional(true);
			store = new EntityStore(myEnv, "DBEntityStore", storeConfig);
			urlBatchIdx = store.getPrimaryIndex(Long.class, UrlBatchFile.class);
			urlInDbByHostIdx = store.getPrimaryIndex(String.class, UrlInDbByHost.class);
			urlBatchTmpIdx = store.getPrimaryIndex(Long.class, UrlBatchFileTmp.class);
		} catch(DatabaseException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public synchronized static DBWrapper getInstance(String envDirectory) {
		if (DBinstance == null) {
			close();
			DBinstance = new DBWrapper(envDirectory);
		}
		return DBinstance;
	}
	
	public void sync() {
		if (store != null) store.sync();
		if (myEnv != null) myEnv.sync();
	}
		
	public synchronized static void close() {
		// Close store first as recommended
		if (store != null) {
			try {
				store.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
		
		if (myEnv != null) {
			try {
				myEnv.close();
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
	}
	// retrieve random file from db and delete the key
	public Long getFirstFileKey(){
		EntityCursor<UrlBatchFile> cursor = urlBatchIdx.entities();
		Long key = null;
		try {
			for (UrlBatchFile b : cursor) {
				key = b.getKey();
				break;
			}
		} finally {
			cursor.close();
		}
		return key;
	}
	public int getFilekeyNumber(){
		EntityCursor<UrlBatchFile> cursor = urlBatchIdx.entities();
		int count = 0;
		for (UrlBatchFile b : cursor) {
			count++;
		}
		return count;
	}
	public String getAndRemoveFile(Long key){
		UrlBatchFile file = this.urlBatchIdx.get(key);
		String content = file.getContent();
		this.urlBatchIdx.delete(key);
		return content;
	}

	public void storeFileToDb(UrlBatchFile urlBatchFile){
		this.urlBatchIdx.put(urlBatchFile);
		sync();
	}
	public boolean hasSeen(UrlObj urlObj) {
		String hostName = urlObj.getHost();
		String hash = urlObj.getHash();
		UrlInDbByHost urlInDbByHost = this.urlInDbByHostIdx.get(hostName);
		if(urlInDbByHost == null){
			urlInDbByHost  = new UrlInDbByHost(hostName);
			this.urlInDbByHostIdx.put(urlInDbByHost);
		}
		// when test if equal, put the hash into it as well
		boolean result = urlInDbByHost.hashIsEqual(hash);
        this.urlInDbByHostIdx.put(urlInDbByHost);
		sync();
		return result;

	}


	//For temporary usage retrieve random file from db and delete the key
	public Long getFirstTmpFileKey(){
		EntityCursor<UrlBatchFileTmp> cursor = urlBatchTmpIdx.entities();
		Long key = null;
		try {
			for (UrlBatchFileTmp b : cursor) {
				key = b.getTmpKey();
				break;
			}
		} finally {
			cursor.close();
		}
		return key;
	}
	public List<String> getAndRemoveTmpFile(Long key){
		UrlBatchFileTmp file = this.urlBatchTmpIdx.get(key);
		List<String> urls = file.getTmpContent();
		this.urlBatchIdx.delete(key);
		sync();
		return urls;
	}
	public void storeTmpFileToDb(UrlBatchFileTmp urlBatchFileTmp){
		this.urlBatchTmpIdx.put(urlBatchFileTmp);
		sync();
	}

	public void removeUrlBatchFileTmp(){
		EntityCursor<UrlBatchFileTmp> cursor = urlBatchTmpIdx.entities();
		List<Long> keys = new ArrayList<>();
		try {
			for (UrlBatchFileTmp b : cursor) {
				keys.add(b.getTmpKey());
			}
		} finally {
			cursor.close();
		}
		if(keys.size() != 0){
			for(Long key : keys){
				this.urlBatchTmpIdx.delete(key);
			}
		}
		Long key = getFirstTmpFileKey();
		if(key != null)
			throw new RuntimeException("Batch file is not cleaned up");
		return;

	}

}
