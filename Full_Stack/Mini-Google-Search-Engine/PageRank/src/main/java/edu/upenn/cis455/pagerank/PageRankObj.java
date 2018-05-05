package edu.upenn.cis455.pagerank;

import java.util.ArrayList;
import java.util.List;
import crawler.URLInfo;


public class PageRankObj {
    private String hash;
    private String url;
    private List<String> outBoundUrls;
    private Double prVal;

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

    public List getOutBoundUrls(){
        return outBoundUrls;
    }

    public void setPrVal(Double prVal) {
        this.prVal = prVal;
    }

    public Double getPrVal() {
        return prVal;
    }
}
