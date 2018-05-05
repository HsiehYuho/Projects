package crawler.master.store;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class UrlBatchFile {
    @PrimaryKey
    private Long key; // key is time stamp that the file is created
    private String content;
    public UrlBatchFile(){}
    public UrlBatchFile(long key){
        this.key = key;
    }
    public void addConent(String content){
        this.content = content;
    }
    public Long getKey(){
        return this.key;
    }
    public String getContent(){
        return this.content;
    }
}
