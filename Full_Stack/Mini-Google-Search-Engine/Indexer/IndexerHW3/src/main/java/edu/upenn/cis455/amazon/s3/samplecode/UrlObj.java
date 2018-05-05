package edu.upenn.cis455.amazon.s3.samplecode;

import java.util.Date;

public class UrlObj {

    // for schema
    public static String URL_OBJ = "Url_Obj";
    public static String HOST_VALUE = "Host_Value";

    private String url;
    private byte[] content;
    private Date lastCrawl;
    private String contentType;
    private int hostHashVal;
    private int level;
    private String host;
    private String method;
    private boolean getFromS3 = false;
    private String hash;

    public UrlObj(){}

    // from spout to crawl bolt
    public UrlObj(String url){
        this.url = url;
        this.createHostHashVal();
        this.level = 1;
        this.host = new URLInfo(url).getHostName();
        this.hash = URLInfo.hash(url);
        this.method = "HEAD";
    }

    // from crawl bolt to parser bolt
    public UrlObj(String url, byte[] content, String contentType){
        this.url = url;
        this.createHostHashVal();
        this.level = 1;
        this.content = content;
        this.contentType = contentType;
        this.lastCrawl = new Date();
        this.host = new URLInfo(url).getHostName();
        this.hash = URLInfo.hash(url);
        this.method = null;
    }

    public void addLevel(int add){
        this.level += add;
    }
    public int getLevel(){
        return this.level;
    }
    public void createHostHashVal(){
        URLInfo urlInfo = new URLInfo(this.url);
        this.hostHashVal = Math.abs(urlInfo.getHostName().hashCode());
    }
    public int getHostHashVal(){
        return this.hostHashVal;
    }
    public Date getLastCrawl(){
        if(this.lastCrawl == null)
            return null;
        return this.lastCrawl;
    }
    public void updateContent(byte[] newContent){
        this.content = newContent;
        this.lastCrawl = new Date();
    }
    public byte[] getContent(){
        return this.content;
    }
    public String getUrl(){
        return this.url;
    }
    public String getContentType(){
        return this.contentType;
    }
    public String getHost(){
        return this.host;
    }
    public void setHeadToGet(){
        this.method = "GET";
    }
    public String getMethod(){
        return this.method;
    }
    public void setGetFromS3(){
        this.getFromS3 = true;
    }
    public boolean isGetFromS3(){
        return this.getFromS3;
    }
    public String getHash(){
        return this.hash;
    }
}