package searchengine;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import crawler.JettyServer;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.log4j.Logger;
import searchengine.Extra.SpellCheck;
import searchengine.store.DisplayUrlObj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class SearchEngine {

    private static Logger log = Logger.getLogger(SearchEngine.class);
    private Map<String,String> config;

    /** Load stop words list in-memory. */
    public static Set<String> stopwordsLst = new HashSet();

    /** Load high-frequency bigram list in-memory. */
    public static Set<String> bigramLst = new HashSet();

    /** Load high-frequency trigram list in-memory. */
    public static Set<String> trigramLst = new HashSet();

    /** Set up Stanford coreNLP parser. */
    public static StanfordCoreNLP pipeline = null;

    /** Set up S3 on AWS. */
    public static String html_bucket_name = "2018-spring-cis555-g09-htmlbucket";
    private static AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials(); // get amazon credentials from ~/.aws */
    public static final AmazonS3 s3 = new AmazonS3Client(credentials);    // connect to S3

    /** Set up dynamodb. */
    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider("lanqingy"))
            .withRegion("us-east-1").build();

    private static DynamoDB dynamoDB = new DynamoDB(client);
    static List<Table> tables = new ArrayList<>();

    /**
     * Read in stopwords.txt, 2_gram.txt and 3_gram.txt
     */
    private static void init() {
        // read in stop words txt
        File stopwordsFile = new File("./corpus/stopwords.txt");
        try {
            BufferedReader br = new BufferedReader(new FileReader(stopwordsFile));
            String line;
            while ((line = br.readLine()) != null) {
                stopwordsLst.add(line);
            }
        } catch (Exception e) {
            System.out.println("Error reading stopwords.txt");
        }

        // read in bigram list
        File bigramFile = new File("./corpus/2_gram.txt");
        try {
            BufferedReader br = new BufferedReader(new FileReader(bigramFile));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\\s");
                if (tokens.length == 3) {
                    bigramLst.add(tokens[1].trim() + " " + tokens[2].trim());
                } else {
                    System.out.println(line);
                }
            }
        } catch (Exception e) {
            System.out.println("Error reading 2_gram.txt");
        }

        // read in trigram list
        File trigramFile = new File("./corpus/3_gram.txt");
        try {
            BufferedReader br = new BufferedReader(new FileReader(trigramFile));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\\s");
                if (tokens.length == 4) {
                    bigramLst.add(tokens[1].trim() + " " + tokens[2].trim() + " " + tokens[3].trim());
                } else {
                    System.out.println(line);
                }
            }
        } catch (Exception e) {
            System.out.println("Error reading 3_gram.txt");
        }

        // set up pipeline properties
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        props.setProperty("tokenize.language", "English");

        // build pipeline
        pipeline = new StanfordCoreNLP(props);
        tables.add(dynamoDB.getTable("IndexerTest"));
        tables.add(dynamoDB.getTable("Index1"));
        tables.add(dynamoDB.getTable("Index2"));
        tables.add(dynamoDB.getTable("Index3"));
        tables.add(dynamoDB.getTable("Index4"));
        tables.add(dynamoDB.getTable("Index5"));
        tables.add(dynamoDB.getTable("Index6"));
        tables.add(dynamoDB.getTable("Index7"));
        tables.add(dynamoDB.getTable("Index8"));
        tables.add(dynamoDB.getTable("Index9"));
    }

    public SearchEngine(Map<String,String> config){
        this.config = config;
    }

    public void start() throws Exception {
        JettyServer js = new JettyServer();
        js.run((String)config.get("LocalPort"));
    }

    public static void main(String[] args) throws Exception {
        // prepare for query understanding
        init();
        // read from dictionary
        SpellCheck.CreateDictionary("20k.txt");

        Map<String,String> config = new HashMap<>();
        config.put("LocalPort","8000");
        log.info("SearchEngine Start");
        SearchEngine s = new SearchEngine(config);
        s.start();
    }
}
