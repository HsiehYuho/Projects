package searchengine.store;

import org.apache.hadoop.util.hash.Hash;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexerObj {
    private Map<String, List<String>> docIDs;
    private Map<String, Double> docIDsToScore;
    public IndexerObj(Map<String, List<String>> docIDs, Map<String, Double> docIDsToScore){
        this.docIDs = docIDs;
        this.docIDsToScore = docIDsToScore;
    }
    public Map<String, List<String>> getDocIDs(){
        return this.docIDs;
    }
    public Map<String, Double> getDocIDsToScore(){
        return this.docIDsToScore;
    }
}
