package searchengine;

import amazon.s3.samplecode.DBConnection;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import crawler.worker.storage.UrlObj;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import org.apache.jasper.tagplugins.jstl.core.Url;
import org.apache.log4j.Logger;
import searchengine.CreateHtmlFiles.BasicComponent;
import searchengine.Extra.AutoCompleteTrie;
import searchengine.Extra.SpellCheck;
import searchengine.store.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class MasterServlet extends HttpServlet{
    private static Logger log = Logger.getLogger(MasterServlet.class);
    private HashMap<String,UserInfo> userInfos = new HashMap<>();
    private static PriorityQueue<CacheUrlObj> cachePq = new PriorityQueue<>(new Comparator<CacheUrlObj>(){
        @Override
        public int compare(CacheUrlObj o1, CacheUrlObj o2) {
            return o1.getCount()-o2.getCount();
        }
    });
    private String prTable = "PageRank";
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException{

		String pathInfo = request.getPathInfo();
		PrintWriter out = response.getWriter();
		String content = "None";
		String title = "None";
        HttpSession session = request.getSession(true);
        String sessionId = session.getId();
		boolean isSearchImg = false, isChildProof = false, isSearchPdf = false;

		// root pages - the search page
		if(pathInfo.equals("/") || pathInfo.equals("/pdf") || pathInfo.equals("/img")){
			response.setContentType("text/html");
			if(pathInfo.equals("/pdf"))
    			content = BasicComponent.createMainPage("PDF");
			else if (pathInfo.equals("/img")) {
                isSearchImg = true;
                content = BasicComponent.createMainPage("IMG");
            }
			else
			    content = BasicComponent.createMainPage(null);
			out.println(content);
		}

		if(pathInfo.equals("/search")){
			String query = request.getParameter("query");
			String city = request.getParameter("city");

            // doing the spell check
            String autocomplete = request.getParameter("autocomplete");
            String childProofValue = request.getParameter("childproof");
            String newWord = "";

            if(!isNumeric(query)){
                String correct;
                if(autocomplete != null) {
                    AutoCompleteTrie trie = new AutoCompleteTrie();
                    ArrayList<String> results = trie.getAutoSuggestions(query, 10);
                    if(results != null) {
                        System.out.println(results.get(0));
                        correct = results.get(0);
                    }else {
                        correct = SpellCheck.Correct(query);
                    }
                }else {
                    correct = SpellCheck.Correct(query);
                }



                // auto completenotFound
                if(!correct.equals(query)){
                    newWord = correct;
                }
            }

            if(childProofValue != null && childProofValue.trim().length()!=0) {
                isChildProof = true;
            }

            query += city;
			String searchType = request.getParameter("searchType");
			// search for certain type
//			if(searchType == null) searchType = "HTML";
            String stemmingQuery = query.toLowerCase();
            if((searchType != null && searchType.equals("PDF")) || stemmingQuery.contains("pdf"))
                searchType = "PDF";
            if((searchType != null && searchType.equals("IMG")) || stemmingQuery.contains("jpg") || stemmingQuery.contains("image")) {
                isSearchImg = true;
                searchType = "IMG";
            }
			List<String> keys = parseQuery(query);

			// return 404
			if(keys == null || keys.size() == 0){
                content = BasicComponent.create404Page();
                out.println(content);
                return;
            }
			// sort the key based on length, for later use of highlight
			keys.sort(new keyCmp());
			// later split keys into subkeys to accelerate
            IndexerObj indexerObj = queryDynamo(keys,searchType);
			Map<String, List<String>> docIDs = indexerObj.getDocIDs();
            Map<String, Double> docIDsScoreMap = indexerObj.getDocIDsToScore();

			if (keys == null || keys.size() == 0) {
			    content = "No valid query";
            }
            // get flat and sorted docId
            List<String> sortedDocIDs = flatAndsortDocIDs(docIDs);
            System.out.println("Total extract data: " + sortedDocIDs.size());

            List<String> firstPartDocIDs = getFirstPartOfDocIDs(sortedDocIDs);
            System.out.println("First Part of Id size: " + firstPartDocIDs.size());
            List<String> secondPartDocIDs = getSecondPartOfDocIDs(sortedDocIDs);
            System.out.println("Second Part of Id size: " + secondPartDocIDs.size());

            Date start = new Date();

            // get and sorted
            List<UrlObj> firstUrlObjs = getUrlObjFromS3(firstPartDocIDs,isSearchImg);
            sortUrlObj(firstUrlObjs,docIDsScoreMap);

			// get the visible url obj, also check for duplicate and childproof
            List<DisplayUrlObj> displayUrlObjs = null;

            displayUrlObjs = getDisplayUrlObj(firstUrlObjs,isChildProof,searchType);
            Date end  = new Date();

            // add display url obj to userInfo, and new it every search bottom is toggled
            double searchTime = (double)(end.getTime() - start.getTime())/1000;
            System.out.println("Load first batch : " + searchTime);
            userInfos.put(sessionId,new UserInfo(sessionId,keys,searchTime,sortedDocIDs.size(),query,searchType,newWord));
            userInfos.get(sessionId).addDisplayUrlObjList(displayUrlObjs);

            // display current batch
            List<DisplayUrlObj> batchDisplayUrls = userInfos.get(sessionId).getCurBatch();
            for(DisplayUrlObj d: batchDisplayUrls){
                for(String key : keys)
                    d.setStrongFont(key);
            }
            // curNum : current page to determine whether we should use /pre
            content = BasicComponent.createFullPage("Result", batchDisplayUrls,userInfos.get(sessionId), searchType);
            out.println(content);
            out.close();

            // load the other pages, and sorted
            Date secondStart = new Date();
            List<UrlObj> secondUrlObjs = getUrlObjFromS3(secondPartDocIDs,isSearchImg);
            sortUrlObj(secondUrlObjs,docIDsScoreMap);


            // get the visible url obj, also check for duplicate and childproof
            displayUrlObjs = getDisplayUrlObj(secondUrlObjs,isChildProof,searchType);
            userInfos.get(sessionId).addDisplayUrlObjList(displayUrlObjs);
            Date secondEnd = new Date();
            searchTime = (double)(secondEnd.getTime() - secondStart.getTime())/1000;
            System.out.println("Load second batch : " + searchTime);
            return;
		}
		if(pathInfo.equals("/pre")){
            UserInfo userInfo =  userInfos.get(sessionId);
		    // display pre batch
            List<DisplayUrlObj> batchDisplayUrls =userInfo.getPreBatch();
            for(DisplayUrlObj d: batchDisplayUrls){
                for(String key : userInfo.getKeys())
                    d.setStrongFont(key);
            }
            // curNum : current page to determine whether we should use /pre
            content = BasicComponent.createFullPage("Result", batchDisplayUrls,userInfo, null);
            out.println(content);
            return;
        }
        if(pathInfo.equals("/next")){
            UserInfo userInfo =  userInfos.get(sessionId);
            // display next batch
            List<DisplayUrlObj> batchDisplayUrls =userInfo.getNextBatch();
            for(DisplayUrlObj d: batchDisplayUrls){
                for(String key : userInfo.getKeys())
                    d.setStrongFont(key);
            }
            // curNum : current page to determine whether we should use /pre
            content = BasicComponent.createFullPage("Result", batchDisplayUrls,userInfo, null);
            out.println(content);
            return;
        }
        if(pathInfo.equals("/goto")){
            int newPage = Integer.parseInt(request.getParameter("page"))-1;
            UserInfo userInfo =  userInfos.get(sessionId);
            userInfo.setCurrentPage(newPage);
            List<DisplayUrlObj> batchDisplayUrls =userInfo.getCurBatch();
            int count = 0;
            while(batchDisplayUrls.size() == 0 && count < 5){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                count++;
            }
            for(DisplayUrlObj d: batchDisplayUrls){
                for(String key : userInfo.getKeys())
                    d.setStrongFont(key);
            }
            content = BasicComponent.createFullPage("Result", batchDisplayUrls,userInfo, null);
            out.println(content);
            return;
        }

	}

	private class HTML {
		private String openHTML;
		private String closeHTML;
		public HTML(String title, String content){
			this.openHTML = "<HTML><HEAD><TITLE>" + title +  "</TITLE></HEAD>";
			this.closeHTML = "<BODY>" + content + "</BODY></HTML>";
		}
		public String getHTML(){
			return this.openHTML + this.closeHTML;
		}
	}

	private String createSearchBox(){
		String content = "<form method = \"get\">" +
				"<input type=\"text\" name =\"query\" placeholder=\"What you want to know?\">" +
				"<input type = \"submit\" formaction = /search value = \"search\"><br></form>";
		return content;
	}

    /**
     * Process text: tokenize(unigram, bigram, trigram), stem and lemmatize
     */
    private static List<String> parseQuery(String query) {

        if (query == null || query.length() == 0 || SearchEngine.stopwordsLst.contains(query)) {
            return null;
        }

        List<String> keys = new ArrayList<>();

        // create a document object
        CoreDocument document = new CoreDocument(query);

        // annnotate the document
        SearchEngine.pipeline.annotate(document);

        // get raw token size
        int len = document.tokens().size();

        // process index key
        for (int i = 0; i < len; i++) {

            // get current raw token
            CoreLabel token = document.tokens().get(i);

            // get raw unigram, bigram and trigram
            String unigram, bigram = null, trigram = null;
            unigram = document.tokens().get(i).word();
            if (i + 1 < len) {
                bigram = unigram + " " + document.tokens().get(i + 1).word();
                if (i + 2 < len) {
                    trigram = bigram + " " + document.tokens().get(i + 2).word();
                }
            }

            // unigram: index all unigram except for
            // 1) stop words
            // 2) contains character other than a-z A-Z 0-9 . - @ & _
            if (!SearchEngine.stopwordsLst.contains(unigram.toLowerCase()) && unigram.matches("[a-zA-Z0-9]+[-.@&]*[a-zA-Z0-9-.@&]+")) {

                // Stanford Lemmatizer:
                // 1) basic stemming: are, is -> be
                // 2) lemmatizing: providing, provides -> provide
                String lemma = token.lemma().toLowerCase();
                keys.add(lemma);
            }

            // bigram (case-incensitive, not lemmatized)
            if (bigram != null) {
                bigram = bigram.toLowerCase().trim();
                if (SearchEngine.bigramLst.contains(bigram)) {
                   keys.add(bigram);
                }
            }

            // trigram (case-incensitive, not lemmatized)
            if (trigram != null) {
                trigram = trigram.toLowerCase().trim();
                if (SearchEngine.trigramLst.contains(trigram)) {
                    keys.add(trigram);
                }
            }
        }
        return keys;
    }

    // search type can be PDF,IMG,TXT,HTML
    private static IndexerObj queryDynamo(List<String> keys, String searchType) {

        if (keys == null || keys.size() == 0) {
            return null;
        }

        Map<String, List<String>> docIDs = new HashMap<>();
        Map<String, List<Double>> docIDtoScoreTmp = new HashMap<>();

        Map<String, Double> docIDsToScore = new HashMap<>();
        for (String key: keys) {
            docIDs.put(key, new ArrayList<>());
            docIDtoScoreTmp.put(key, new ArrayList<>());
            HashMap<String, String> nameMap = new HashMap<>();
            nameMap.put("#key", "key");

            HashMap<String, Object> valueMap = new HashMap<>();
            valueMap.put(":string", key);

            if(searchType != null){
                nameMap.put("#type", "ContentType");
                valueMap.put(":type", searchType);
            }

            QuerySpec querySpec = null;
            if(searchType != null){
                querySpec = new QuerySpec()
                        //.withProjectionExpression("docID")
                        .withKeyConditionExpression("#key = :string")
                        .withFilterExpression("#type = :type")
                        .withScanIndexForward(false)
                        .withNameMap(nameMap)
                        .withValueMap(valueMap)
                        .withMaxResultSize(30);
            }
            else{
                querySpec = new QuerySpec()
                        //.withProjectionExpression("docID")
                        .withKeyConditionExpression("#key = :string")
                        .withScanIndexForward(false)
                        .withNameMap(nameMap)
                        .withValueMap(valueMap)
                        .withMaxResultSize(30);
            }


            ItemCollection<QueryOutcome> items = null;
            Iterator<Item> iterator = null;
            Item item = null;

            try {
                System.out.println("Index key is: " + valueMap.get(":string"));
                for (Table table: SearchEngine.tables) {
                    System.out.println("Checking table: " + table.getTableName());
                    items = table.query(querySpec);

                    iterator = items.iterator();
                    while (iterator.hasNext()) {
                        item = iterator.next();
                    docIDs.get(key).add(item.getString("docID"));
                    docIDtoScoreTmp.get(key).add(item.getNumber("score").doubleValue());
                    }
                }
            } catch (Exception e) {
                System.err.println("Unable to query cookie as index key");
                System.err.println(e.getMessage());
            }
        }
        // map the docID to docIDScore
        for(String key : docIDs.keySet()){
            List<String> docIDsList = docIDs.get(key);
            List<Double> docIDsScoreList = docIDtoScoreTmp.get(key);
            for(int i = 0; i < docIDsList.size(); i++){
                docIDsToScore.put(docIDsList.get(i),docIDsScoreList.get(i));
            }
        }
        IndexerObj indexerObj = new IndexerObj(docIDs,docIDsToScore);

        return indexerObj;
    }
    /**
     * Retrieve doc from s3
     */
    private static List<UrlObj> getUrlObjFromS3(List<String> docIDs,boolean isSearchImg){
        String bucketName = "2018-spring-cis555-g09-htmlbucket";
        if(isSearchImg)
            bucketName = "2018-spring-cis555-g09-img-bucket";
        List<UrlObj> urlObjs = new ArrayList<>();

        for(String docID : docIDs) {
            UrlObj urlObj = null;
            // search from cache at first
            boolean found = false;
            for(CacheUrlObj c : cachePq){
                if(c.getUrlObj().getHash().equals(docID)){
                    c.addCount();
                    urlObjs.add(c.getUrlObj());
                    found = true;
                }
            }
            // search from s3
            if(!found){
                try {
                    urlObj = DBConnection.getElementFromS3(docID,bucketName);
                    if(urlObj != null){
                        urlObjs.add(urlObj);
                        // store in cache
                        if(cachePq.size() >100){
                            cachePq.poll();
                        }
                        cachePq.add(new CacheUrlObj(urlObj));
                    }
                } catch (IOException e) {
                    System.out.println("Fail to retrieve, docID" + docID);
                    e.printStackTrace();
                }
            }
        }
        return urlObjs;

    }
    // sort the urlObjs based on joint
    private static List<String> flatAndsortDocIDs(Map<String,List<String>> docIDs){
        Map<String,Integer> docIdScoreMap = new HashMap<>();

        // remove the duplicate urlObj
        Set<String> urlHashSet = new HashSet<>();

        List<String> docIDList = new ArrayList<>();

        for(String key : docIDs.keySet()){
            for(String docID : docIDs.get(key)){
                int keyLength = key.trim().split("\\s+").length;
                // calculate the accumulated score, if string is one words, give credit 1, two words, credit = 2
                if( keyLength == 1){
                    docIdScoreMap.put(docID,docIdScoreMap.getOrDefault(docID,0)+1);
                }
                else if(keyLength == 2){
                    docIdScoreMap.put(docID,docIdScoreMap.getOrDefault(docID,0)+2);
                }
                else if(keyLength == 3){
                    docIdScoreMap.put(docID,docIdScoreMap.getOrDefault(docID,0)+3);
                }

                // store the list of urlObj
                if(!urlHashSet.contains(docID)){
                    urlHashSet.add(docID);
                    docIDList.add(docID);
                }
            }
        }

        Collections.sort(docIDList, new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return docIdScoreMap.get(s2) - docIdScoreMap.get(s1);
            }
        });
        return docIDList;
    }
    private List<DisplayUrlObj> getDisplayUrlObj(List<UrlObj> urlObjs, boolean childProof, String searchType){
        List<DisplayUrlObj> displayUrlObjsCandidates = new ArrayList<>();
        // it is a little be waste of space, but the candidate should not be very much
        List<DisplayUrlObj> displayUrlObjs = new ArrayList<>();

        for(UrlObj urlObj : urlObjs){
            DisplayUrlObj displayUrlObj = new DisplayUrlObj(urlObj);
            displayUrlObjsCandidates.add(displayUrlObj);
        }
        // O(n^2) to check duplicate and childproof only for html
        if(searchType == null || searchType.equals("HTML")){

            for(int i = 0; i < displayUrlObjsCandidates.size(); i++){
                DisplayUrlObj primary = displayUrlObjsCandidates.get(i);
                boolean isPrimaryOkay = true;
                if(i == displayUrlObjsCandidates.size()-1){
                    if(!primary.isOkToDisplay(null,childProof));
                        isPrimaryOkay = false;
                }
                for(int j = i+1; j < displayUrlObjsCandidates.size(); j++){
                    DisplayUrlObj secondary = displayUrlObjsCandidates.get(j);
                    if(!primary.isOkToDisplay(secondary,childProof)){
                        isPrimaryOkay = false;
                        log.debug("Primary " + primary.getUrl() + " is the same as Secondary: " + secondary.getUrl());
                        break;
                    }
                }
                // dp not check the last one, because we can not test last one individually
                if(isPrimaryOkay)
                    displayUrlObjs.add(primary);
            }
            return displayUrlObjs;
        }
        // the difference is html need to check duplicate, and the pdf and img do not need to check pdf
        return displayUrlObjsCandidates;
    }

    private class keyCmp implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            return o2.split("\\s+").length  - o1.split("\\s+").length;
        }
    }
    // using pagerank
    private void sortUrlObj(List<UrlObj> urlObjs, Map<String, Double> docIDsScoreMap){
        PrValueCmp prValueCmp = new PrValueCmp(docIDsScoreMap);
        urlObjs.sort(prValueCmp);
    }

    private List<String> getFirstPartOfDocIDs(List<String> keys){
        List<String> firstPartOfKeys = new ArrayList<>();
        for(int i = 0; i < 10 && i < keys.size(); i++){
            firstPartOfKeys.add(keys.get(i));
        }
        return firstPartOfKeys;
    }
    private List<String> getSecondPartOfDocIDs(List<String> keys){
        List<String> secondPartOfKeys = new ArrayList<>();
        for(int i = 10; i < 30 && i < keys.size(); i++){
            secondPartOfKeys.add(keys.get(i));
        }
        return secondPartOfKeys;
    }

    private class PrValueCmp implements Comparator<UrlObj>{
        Map<String,Double> docIDsScore;
        public PrValueCmp( Map<String,Double> docIDsScore){
            super();
            this.docIDsScore = docIDsScore;
        }
        @Override
        public int compare(UrlObj o1, UrlObj o2) {
            String host1 = o1.getHost();
            String host2 = o2.getHost();
            Double idxVal1 = docIDsScore.get(o1.getHash());
            Double idxVal2 = docIDsScore.get(o2.getHash());
            if(idxVal1 == null) idxVal1 = 0.1;
            if(idxVal2 == null) idxVal1 = 0.2;

            double host1PrValue = PageRankRetriever.getPRScore(prTable,host1).doubleValue() * idxVal1;
            double host2PrValue = PageRankRetriever.getPRScore(prTable,host2).doubleValue() * idxVal2;
            if(host1PrValue > host2PrValue)
                return -1;
            else if (host2PrValue > host1PrValue)
                return 1;
            else
                return 0;
        }
    }
    private static boolean isNumeric(String str)
    {
        try
        {
            double d = Double.parseDouble(str);
        }
        catch(NumberFormatException nfe)
        {
            return false;
        }
        return true;
    }
}
