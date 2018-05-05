package crawler.worker.stormlite.bolt;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import crawler.worker.Worker;
import crawler.worker.info.URLInfo;
//import crawler.worker.storage.DBWrapper;
import crawler.worker.storage.PageRankObj;
import crawler.worker.storage.UrlObj;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import crawler.worker.stormlite.OutputFieldsDeclarer;
import crawler.worker.stormlite.TopologyContext;
import crawler.worker.stormlite.routers.IStreamRouter;
import crawler.worker.stormlite.tuple.Fields;
import crawler.worker.stormlite.tuple.Tuple;
import crawler.worker.stormlite.tuple.Values;

import com.amazonaws.services.s3.AmazonS3;

import javax.swing.text.html.HTML;

public class ParserBolt implements IRichBolt{
	static Logger log = Logger.getLogger(ParserBolt.class);
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    private List<String> linksList;
	private Fields schema = new Fields(UrlObj.URL_OBJ, UrlObj.HOST_VALUE);

	// store data to s3
	private AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
	private final AmazonS3 s3 = new AmazonS3Client(credentials);

	private String HTML_BUCKET = "2018-spring-cis555-g09-html-guess-bucket";
	private String IMG_BUCKET = "2018-spring-cis555-g09-img-bucket";
	private String PR_BUCKET = "2018-spring-cis555-g09-pr-guess-bucket";
	private String NEWS_HTML_BUCKET = "2018-spring-cis555-g09-news-html-bucket";
	private String NEWS_PR_BUCKET = "2018-spring-cis555-g09-news-pr-bucket";
	//	private String HTML_BUCKET = "2018-spring-cis555-g09-html-bucket";
// 	private String PR_BUCKET = "2018-spring-cis555-g09-pr-bucket";
//	private String PDF_BUCKET = "2018-spring-cis555-g09-pdf-bucket";

	private ObjectMapper mapper = new ObjectMapper();

	// store page rank object
	private String envDirectory = "./Database";
	private String downloadUrlFile = "./downloadUrls.txt";

	// parse regulation
    private int sampleCount = 20;
    private Random rand;

    //	private DBWrapper dbw;


	public ParserBolt(){
		log.debug("Init ParserBolt");
	}
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.linksList = new LinkedList<>();
		this.rand = new Random();
        s3.createBucket(HTML_BUCKET);
        s3.createBucket(PR_BUCKET);
        s3.createBucket(IMG_BUCKET);
		File f = new File(downloadUrlFile);
		// if download exists, remove and then recreate
		if(f.exists()){
			try {
				Files.deleteIfExists(f.toPath());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {f.createNewFile(); } catch (IOException e) {e.printStackTrace();}
		// create the buckets or connect the buckets
//		this.createBucketIfNotExist(HTML_BUCKET);
//		this.createBucketIfNotExist(IMG_BUCKET);

		// store page rank object
//		dbw = new DBWrapper();
//		dbw.run(envDirectory);
	}
    @SuppressWarnings("Duplicates")
    @Override
	public void execute(Tuple input) {
		UrlObj urlObj = (UrlObj) input.getObjectByField(UrlObj.URL_OBJ);
		String requestUrl = urlObj.getUrl();
		byte[] bytes = urlObj.getContent();
		String contentType = urlObj.getContentType();
		int currentLevel = urlObj.getLevel();
		URLInfo urlInfo = new URLInfo(requestUrl);

        // Parse html
        if(contentType.contains("html")){
            //parse document according different content types and store in different s3 bucket
            String content = null;
            try {
                content = new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                log.debug(requestUrl + " parse content but encoding method not expected");
            }


            //normalized url and extract link to filter bolt
            Document doc = Jsoup.parse(content,requestUrl);

            // check the content is english or not
            String contentText = doc.text();
            double nonEnCharCount = 0;
            int contentSize = contentText.length();
            for(int i = 0; i < sampleCount; i++){
                int randomNum = rand.nextInt(contentSize);
                if(!Character.isLetter(contentText.charAt(randomNum))){
                    nonEnCharCount++;
                }
            }
            if(nonEnCharCount / sampleCount > 0.7){
                log.debug(requestUrl + " is not english page");
                return;
            }

            Elements aLinks = doc.select("a[href]");
            for(Element e : aLinks){
                String eLink = e.attr("href");
                String absUrl = e.absUrl("href");
                // sometimes jsoup cannot extract abs url
                synchronized (linksList){
                    if(absUrl != null && absUrl.length() != 0)
                        linksList.add(absUrl);
                    else if (eLink != null && eLink.length() != 0)
                        linksList.add(normalizeUrl(requestUrl,urlInfo,eLink));
                }
            }

            // store url and outbound link to page rank Obj in s3, the link does not include pdf and image
            PageRankObj prObj = new PageRankObj(requestUrl);
            synchronized (linksList){
                for(String l : linksList)
                    prObj.addOutBoundLinks(l);
            }

            //noinspection Duplicates
            String prBucketName = PR_BUCKET;
//            s3.createBucket(prBucketName);
            if(prBucketName != null){
                File tempFile = null;
                try {tempFile = File.createTempFile("prefix", "suffix");
                    mapper.writeValue(tempFile, prObj);
                    String s3FileName = String.valueOf(urlObj.getHash());
                    s3.putObject(new PutObjectRequest(prBucketName, s3FileName, tempFile));
                } catch (IOException e) { e.printStackTrace();}
                tempFile.deleteOnExit();
                log.debug("Save page rank of url " + requestUrl + " to " + prBucketName);
            }

            // Add image parser for src and added back to the queue
            Elements imageElements = doc.select("[src]");
            for(Element e : imageElements){
                if (e.tagName().equals("img")){
                    String src = e.attr("abs:src");
                    synchronized (linksList){
                        if(src != null && src.length() != 0){
                            linksList.add(src);
                        }
                    }
                }
            }
        }
        // Parse image
        if(contentType.contains("image")){
            // do nothing
        }

        if(contentType.contains("pdf")){
			// do nothing
		}

		// save document to s3, include url, content, contentType,
		String bucketName = contentTypeToBucket(contentType);
//		s3.createBucket(bucketName);
		if(bucketName != null){
			File tempFile = null;
			try {tempFile = File.createTempFile("prefix", "suffix");
				mapper.writeValue(tempFile, urlObj);
				String s3FileName = String.valueOf(urlObj.getHash());
				s3.putObject(new PutObjectRequest(bucketName, s3FileName, tempFile));
			} catch (IOException e) { e.printStackTrace();}
			//File tempFile = File.createTempFile("MyAppName-", ".tmp");
			tempFile.deleteOnExit();
            Worker.downloadCount.addAndGet(1);
            log.info("Save " + urlObj.getHost() + ", current count: " + Worker.downloadCount.get());
			try {
				String writedownUrl = requestUrl + "\n";
				Files.write(Paths.get(downloadUrlFile), writedownUrl.getBytes(), StandardOpenOption.APPEND);
			}catch (IOException e) {
				//exception handling left as an exercise for the reader
			}
		}


        //dbw.createPageRankObj(requestUrl, linksList);

		// extract correspond url html file from database
		synchronized (linksList){
            for(String link : linksList){
                // TODO: emit links to filter bolt, also have to add
                if(link != null && link.trim().length() != 0){
                	try{
						UrlObj urlObjFromDocument = new UrlObj(link);
						urlObjFromDocument.addLevel(currentLevel);
						collector.emit(new Values<Object>(urlObjFromDocument,String.valueOf(urlObjFromDocument.getHostHashVal())));
					}
					catch (NullPointerException e){
                        log.debug("Null Pointer appears");
                        continue;
					}
				}
            }
        }
		linksList = new LinkedList<>();
	}

	
	@Override
	public String getExecutorId() {
		return this.executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);			
	}

	@Override
	public void cleanup() {
//		dbw.shutdown();
	}


	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

	@Override
	public Fields getSchema() {
		return this.schema;
	}

//	private void createBucketIfNotExist(String bucketName){
//		s3.createBucket(bucketName);
//		log.debug("does not create bucket: " + bucketName);
//		return;
//	}

	private String contentTypeToBucket(String contentType){
		if(contentType.contains("html") || contentType.contains("pdf"))
			return HTML_BUCKET;
		if(contentType.contains("image"))
			return IMG_BUCKET;
//		if(contentType.contains("pdf"))
//			return PDF_BUCKET;

		return null;
	}

	private String normalizeUrl(String requestUrl, URLInfo urlInfo, String eLink){
		// absolute path
		if( eLink.startsWith("http://") || eLink.startsWith("https://")){
			return eLink;
		}
		// relative path
		else if (eLink.startsWith("/"))
			return urlInfo.getProtocal()+urlInfo.getHostName()+eLink;
		else if (eLink.startsWith("www"))
			return urlInfo.getProtocal() + eLink;
		else{
			// create abs path
			if(requestUrl.endsWith(".html")){
				return (requestUrl.substring(0,requestUrl.lastIndexOf("/")+1) + eLink);
			}
			else if (requestUrl.endsWith("/")){
				return (requestUrl+eLink);
			}
			else{
				return (requestUrl+"/"+eLink);
			}
		}
	}
}
