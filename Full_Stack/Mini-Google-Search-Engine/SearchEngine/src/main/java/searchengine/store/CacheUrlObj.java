package searchengine.store;

import crawler.worker.storage.UrlObj;

public class CacheUrlObj {
    private UrlObj urlObj;
    private int count;
    public CacheUrlObj(UrlObj urlObj){
        this.urlObj = urlObj;
        this.count = 0;
    }
    public UrlObj getUrlObj(){
        return this.urlObj;
    }
    public int getCount(){
        return this.count;
    }
    public void addCount(){
        this.count++;
    }
}
