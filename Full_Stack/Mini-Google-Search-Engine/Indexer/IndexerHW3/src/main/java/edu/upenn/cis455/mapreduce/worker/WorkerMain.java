package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.log4j.BasicConfigurator;

public class WorkerMain {

    private static String masterIPAndPort = "127.0.0.1:8080";
    private static String envDirectory = ".";  // e.g. ~/node13/
    private static String myPort = "8001";
    static BackgroundWorker w = null;

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
//        File trigramFile = new File("./corpus/3_gram.txt");
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(trigramFile));
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] tokens = line.split("\\s");
//                if (tokens.length == 4) {
//                    trigramLst.add(tokens[1].trim() + " " + tokens[2].trim() + " " + tokens[3].trim());
//                } else {
//                    System.out.println(line);
//                }
//            }
//        } catch (Exception e) {
//            System.out.println("Error reading 3_gram.txt");
//        }

        // set up pipeline properties
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        props.setProperty("tokenize.language", "English");

        // build pipeline
        pipeline = new StanfordCoreNLP(props);
    }

    /** Get parameters from command line. */
    private static void getParameters(String[] args) {
        if (args.length == 0) {
            System.out.println("*** Author: Lanqing Yang (lanqingy)");
            System.exit(1);
        } else {
            try {
                masterIPAndPort = args[0];
                envDirectory = args[1].trim();
                myPort = args[2];
            } catch (Exception e) {
                System.err.println("Please specify:\n" +
                        "1) IP address and port number of the master\n" +
                        "2) path to the storage directory of the worker\n" +
                        "3) port number on which the worker should listen for commands from the master.");
                System.exit(1);
            }
        }
    }

    public static void main(String[] args) {

        BasicConfigurator.configure();

        // get command-line arguments
        getParameters(args);

        // init
        init();

        // create background thread
        w = new BackgroundWorker(masterIPAndPort, myPort);
        Thread t = new Thread(w);
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t.start();

        // start worker server
        Map<String, String> config = new HashMap<String, String>();
        config.put("workerList", "[localhost:" + String.valueOf(myPort) + "]");
        config.put("workerIndex", "0");
        config.put("envDirectory", envDirectory);
        WorkerServer.createWorker(config);

    }

}
