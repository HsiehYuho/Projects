package crawler.worker.storage;

import crawler.worker.info.URLInfo;

import java.util.ArrayList;
import java.util.List;

public class PageRankObj {
    private String hash;
    private String url;
    private List<String> outBoundUrls;
    private double prVal;
    public PageRankObj(){}
    public PageRankObj(String url){
        this.hash = URLInfo.hash(url);
        this.url = url;
        this.outBoundUrls = new ArrayList<>();
        this.prVal = 1.0;
    }
    public void addOutBoundLinks(String link){
        this.outBoundUrls.add(link);
    }
    public String getUrl(){
        return this.url;
    }
    public String getHash(){
        return this.hash;
    }
    public List<String> getOutBoundUrls() {return this.outBoundUrls;}
    public void setPrVal(double d){
        this.prVal = d;
    }
    public double getPrVal(){
        return this.prVal;
    }
}
