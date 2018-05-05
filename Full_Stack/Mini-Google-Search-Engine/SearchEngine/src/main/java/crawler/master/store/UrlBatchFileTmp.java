package crawler.master.store;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import java.util.ArrayList;
import java.util.List;

@Entity
public class UrlBatchFileTmp {
    @PrimaryKey
    private Long key; // key is time stamp that the file is created
    private List<String> urls;
    public UrlBatchFileTmp(){}
    public UrlBatchFileTmp(long key){
        this.key = key;
        this.urls = new ArrayList<>();
    }
    public void addTmpConent(List<String> addUrls){
        for(String url : addUrls){
            urls.add(url);
        }
    }
    public Long getTmpKey(){
        return this.key;
    }
    public List<String> getTmpContent(){
        return this.urls;
    }

}
