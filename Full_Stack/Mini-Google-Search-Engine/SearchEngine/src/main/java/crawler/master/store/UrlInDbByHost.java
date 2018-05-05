package crawler.master.store;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import crawler.worker.info.URLInfo;

import java.util.HashSet;

@Entity
public class UrlInDbByHost {
    @PrimaryKey
    private String hostName;
    private HashSet<String> hashes;
    public UrlInDbByHost(){}
    public UrlInDbByHost(String hostName){
        this.hostName = hostName;
        this.hashes = new HashSet<>();

    }
    public boolean hashIsEqual(String cmpHash){
        if(hashes.contains(cmpHash)){
            return true;
        }
        else{
            hashes.add(cmpHash);
            return false;
        }
    }
}
