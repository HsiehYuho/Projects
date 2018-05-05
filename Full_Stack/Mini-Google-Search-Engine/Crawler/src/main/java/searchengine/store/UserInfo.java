package searchengine.store;

import crawler.worker.storage.UrlObj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UserInfo {
    private String sessionId;
    private int currentPage;
    private List<DisplayUrlObj> displayUrlObjList;
    private List<String> keys;
    private int totalDoc;
    private double searchTime;
    private String query;
    private String searchType;
    private String newWord;
    public  UserInfo(String sessionId, List<String> keys, double searchTime, int totalDoc, String query, String searchType,String newWord){
        this.sessionId = sessionId;
        this.currentPage = 0;
        this.displayUrlObjList = new ArrayList<>();
        this.keys = keys;
        this.searchTime = searchTime;
        this.totalDoc = totalDoc;
        this.query = query;
        this.newWord = newWord;
    }
    public String getSessionId(){
        return this.sessionId;
    }
    public int getCurrentPageNum(){
        return this.currentPage;
    }
    public List<DisplayUrlObj> getDisplayUrlObjList(){
        return this.displayUrlObjList;
    }
    public void addDisplayUrlObjList(List<DisplayUrlObj> newDisplayUrlObjectList){
        if(newDisplayUrlObjectList == null)
            return;
        for(DisplayUrlObj d : newDisplayUrlObjectList)
            displayUrlObjList.add(d);
    }
    public  List<DisplayUrlObj> getCurBatch(){
        List<DisplayUrlObj> curBatch = new ArrayList<>();
        for(int i = currentPage*10; i < currentPage*10 + 10 && i < displayUrlObjList.size(); i++){
            curBatch.add(displayUrlObjList.get(i));
        }
        return curBatch;
    }

    public List<DisplayUrlObj> getNextBatch(){
        currentPage++;
        return getCurBatch();
    }
    public List<DisplayUrlObj> getPreBatch(){
        currentPage--;
        return getCurBatch();
    }
    public List<String> getKeys(){
        return this.keys;
    }
    public double getSearchTime(){
        return this.searchTime;
    }
    public int getTotalDoc(){
        return this.totalDoc;
    }
    public void setCurrentPage(int newPage){
        this.currentPage = newPage;
    }
    public String getQuery(){
        return this.query;
    }
    public String getSearchType(){
        return this.searchType;
    }
    public String getNewWord(){
        return this.newWord;
    }
}
