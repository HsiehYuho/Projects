package edu.upenn.cis455.mapreduce;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Cursor;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class DBWrapper {
	
	private String envDirectory = null;
	private File envHome = null;
    private Environment myEnv = null;
	private Database db = null;

	public DBWrapper(String path) {
        envDirectory = path;
	    try {
            envHome = new File(envDirectory);
            if (!envHome.exists())  envHome.mkdirs();

            // Instantiate an environment and database configuration object
            EnvironmentConfig myEnvConfig = new EnvironmentConfig();
            DatabaseConfig myDbConfig = new DatabaseConfig();

            // If the environment is opened for write, then we want to be
            // able to create the environment and databases if
            // they do not exist.
            myEnvConfig.setAllowCreate(true);
            myDbConfig.setAllowCreate(true);

            // Make it deferred write
            myDbConfig.setDeferredWrite(true);

            // Instantiate the Environment. This opens it and also possible creates it.
            myEnv = new Environment(envHome, myEnvConfig);

            // Now create and open our databases.
            db = myEnv.openDatabase(null, "db", myDbConfig);
        } catch (Exception e) {
	        e.printStackTrace();
	        System.out.println("Error initiating database");
        }
    }

	public synchronized Environment getEnvironment() {
		return myEnv;
	}

	public synchronized Database getDB() {
        return db;
	}

	/** Append. */
	public synchronized void put(String key, String value) {
	    if (key == null || key.length() == 0 || value == null || value.length() == 0) return;
        try {
            String theOldValue = "";
            // retrieve the old data
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("utf-8"));
            DatabaseEntry theOldData = new DatabaseEntry();

            if (db.get(null, theKey, theOldData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                byte[] retData = theOldData.getData();
                theOldValue = new String(retData, "utf-8") + "###";
            }

            String theNewValue = theOldValue + value;
            DatabaseEntry theNewData = new DatabaseEntry(theNewValue.getBytes("utf-8"));
            db.put(null, theKey, theNewData);
            flush();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("DB write fails.");
        }
    }

    public synchronized List<String> get(String key) {
        if (key == null || key.length() == 0) return null;
        try {
            // Create a pair of DatabaseEntry objects. theKey is used to perform the search. theData is used
            // to store the data returned by the get() operation.
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("utf-8"));
            DatabaseEntry theData = new DatabaseEntry();

            if (db.get(null, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                byte[] retData = theData.getData();
                String value = new String(retData, "UTF-8");
                return new ArrayList<>(Arrays.asList(value.split("###")));
            } else {
                return null;
            }
        } catch (Exception e) {
            System.out.println("user info get error");
            return null;
        }
    }

    public synchronized List<String> getAllKeys() {
        List<String> list = new ArrayList<>();
        Cursor cursor = null;
        try {
            // open the cursor
            cursor = db.openCursor(null, null);

            // Cursors need a pair of DatabaseEntry objects to operate. These hold
            // the key and data found at any given position in the database.
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            // Read until we no longer see OperationStatus.SUCCESS
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                String keyString = new String(foundKey.getData(), "utf-8");
                list.add(keyString);
            }
            return list;
        } catch (Exception e) {
            System.out.println("Error scan db. ");
            return null;
        } finally {
            // Cursors must be closed.
            if (cursor != null) cursor.close();
        }
    }


    public void flush() {
        if (db != null) db.sync();
        if (myEnv != null) myEnv.sync();
    }

	public void close() {
	    // first flush everything to disk
        flush();

        // close db
        if (db != null) {
            try {
                db.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing User Info");
            }
        }

        // close environment
        if (myEnv != null) {
            try {
                myEnv.removeDatabase(null, "db");
                myEnv.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing Environment");
            }
        }
    }

}