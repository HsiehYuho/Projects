package edu.upenn.cis455;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public class Index {

    /** Indexer mapper class. */
    public static class IndexMapper extends Mapper<Object, BytesWritable, Text, Text> {

        /** Hold the states of index key. */
        public enum STATE {TITLE, URL, ANCHORTEXT, META, PLAIN, CAP}

        /** Hold the content-type of the document. */
        public enum CONTENT_TYPE {HTML, PDF, TXT, IMG}

        /** Determine how to get visible contents. </>*/
        public static double TEXT_THRESHOLD = 0.2;

        /** Serialize and deserialize object into JSON. */
        static ObjectMapper mapper = new ObjectMapper();

        /** Set up Stanford coreNLP parser. */
        public static StanfordCoreNLP pipeline = null;

        /** Load stop words list in-memory. */
        public static Set<String> stopwordsLst = new HashSet();

        /** Load high-frequency bigram list in-memory. */
        public static Set<String> bigramLst = new HashSet();

        /** Load high-frequency trigram list in-memory. */
        public static Set<String> trigramLst = new HashSet();

        static Log log = LogFactory.getLog(Index.class);

        // get amazon credentials from ~/.aws
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();

        // connect to S3
        final AmazonS3 s3 = new AmazonS3Client(credentials);

        private static String bucket_name = "2018-spring-cis555-g09-corpus";

        @Override
        public void setup(Context context) {

            try {
                // read in stop words txt
                S3Object object = s3.getObject(new GetObjectRequest(bucket_name, "stopwords.txt"));
                InputStream inputStream = object.getObjectContent();

                // process the objectData stream
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    stopwordsLst.add(line);
                }
                inputStream.close();
            } catch (Exception e) {
                System.out.println("Error reading stopwords.txt");
            }

            // read in bigram list
            try {
                S3Object object = s3.getObject(new GetObjectRequest(bucket_name, "2_gram.txt"));
                InputStream inputStream = object.getObjectContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while ((line = reader.readLine()) != null) {
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
            try {
                S3Object object = s3.getObject(new GetObjectRequest(bucket_name, "3_gram.txt"));
                InputStream inputStream = object.getObjectContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split("\\s");
                    if (tokens.length == 4) {
                        trigramLst.add(tokens[1].trim() + " " + tokens[2].trim() + " " + tokens[3].trim());
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
        }

        /**
         * Hash with SHA-256
         */
        private String hash(String text) {
            if (text == null || text.length() == 0) {
                return null;
            }
            try {
                // hash
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] encodedhash = digest.digest(text.getBytes(StandardCharsets.UTF_8));

                // convert byte to hex
                StringBuffer hexString = new StringBuffer();
                for (int i = 0; i < encodedhash.length; i++) {
                    String hex = Integer.toHexString(0xff & encodedhash[i]);
                    if(hex.length() == 1) hexString.append('0');
                    hexString.append(hex);
                }
                return hexString.toString();
            } catch (Exception e) {
                return null;
            }
        }

        /**
         * Get tokens from url
         */
        private String parseURLWords(String url) {
            if (url == null || url.length() == 0) {
                return null;
            }
            try {
                StringBuilder sb = new StringBuilder();
                String path = URLDecoder.decode(new URL(url).getPath(), "utf-8");
                for (char c: path.toCharArray()) {
                    if (!(Character.isAlphabetic(c))) {
                        sb.append(" ");
                    } else {
                        if (c >= 'A' && c <= 'Z') {
                            sb.append(" " + c);
                        } else {
                            sb.append(c);
                        }
                    }
                }
                return sb.toString().trim();
            } catch (Exception e) {
                return null;
            }
        }

        /**
         * Process text: tokenize(unigram, bigram, trigram), stem and lemmatize, emit
         * */
        private static void process(String docID, String text, STATE state, CONTENT_TYPE type, Context context) {

            if (docID == null || docID.length() != 64 || text == null || text.length() == 0) {
                return;
            }

            // create a document object
            CoreDocument document = new CoreDocument(text);

            // annnotate the document
            try {
                pipeline.annotate(document);
            } catch (Exception e) {
                log.debug("Error with NLP annotator");
            }

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
                if (!stopwordsLst.contains(unigram.toLowerCase()) && unigram.matches("[a-zA-Z0-9]+[-.@&]*[a-zA-Z0-9-.@&]*")) {
                    // check if capital
                    boolean isCapital = false;
                    if (unigram.matches("[A-Z]+[0-9-.@&_]*")) {
                        isCapital = true;
                    }

                    // Stanford Lemmatizer:
                    // 1) basic stemming: are, is -> be
                    // 2) lemmatizing: providing, provides -> provide
                    String lemma = token.lemma().toLowerCase();

                    // emit
                    try {
                        if (isCapital) {
                            context.write(new Text(lemma), new Text(docID + "," + STATE.CAP + "," + type + "," + i));
                        } else {
                            context.write(new Text(lemma), new Text(docID + "," + state + "," + type + "," + i));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Error mapper output");
                    }
                }

                // bigram (case-incensitive, not lemmatized)
                if (bigram != null) {
                    bigram = bigram.toLowerCase().trim();
                    if (bigramLst.contains(bigram)) {
                        try {
                            context.write(new Text(bigram), new Text(docID + "," + state + "," + type + "," + i));
                        } catch (Exception e) {
                            System.out.println("Error mapper output");
                        }
                    }
                }

                // trigram (case-incensitive, not lemmatized)
                if (trigram != null) {
                    trigram = trigram.toLowerCase().trim();
                    if (trigramLst.contains(trigram)) {
                        try {
                            context.write(new Text(trigram), new Text(docID + "," + state + "," + type + "," + i));
                        } catch (Exception e) {
                            System.out.println("Error mapper output");
                        }
                    }
                }
            }
        }

        /**
         * HTML: have formats
         */
        private void processHTML(String docID, byte[] docContent, String baseurl, Context context) {

            if (docID == null || docID.length() != 64 || baseurl == null || baseurl.length() == 0) {
                return;
            }

            // parse HTML with JSoup
            Document doc = Jsoup.parse(new String(docContent), baseurl);

            // get title
            String title = doc.title();
            process(docID, title, STATE.TITLE, CONTENT_TYPE.HTML, context);

            // get url
            process(docID, parseURLWords(baseurl), STATE.URL, CONTENT_TYPE.HTML, context);

            // get all links and its anchor text
            Elements links = doc.select("a[href]");
            for (Element link: links) {
                String url = link.attr("abs:href");
                String anchorText = link.text();    // key: url in href,
                process(hash(url), anchorText, STATE.ANCHORTEXT, CONTENT_TYPE.HTML, context);
            }

            // get metadata: description
            String description = "";
            Elements metaDescription = doc.select("meta[name=description]");
            for (Element d: metaDescription) {
                description += d.attr("content");
            }
            process(docID, description, STATE.META, CONTENT_TYPE.HTML, context);

            // get metadata: keywords
            String keywords = "";
            Elements metaKeywords = doc.select("meta[name=keywords]");
            for (Element k: metaKeywords) {
                keywords += k.attr("content");
            }
            process(docID, keywords, STATE.META, CONTENT_TYPE.HTML, context);

            // get all the visible contents (should work for most pages)
            String text1 = doc.text();

            // get all the meaningful contents (may not work for all pages)
            StringBuilder text2 = new StringBuilder();
            Elements plaintext = doc.select("p");
            for (Element p: plaintext) {
                text2.append(p.text());
            }

            // stop if the content is empty
            if (text1.length() == 0) { return; }

            // process the content body
            String text = text1;
            double ratio = text2.length() * 1.0 / text1.length();
            if (ratio > TEXT_THRESHOLD) {
                text = text2.toString();
            }
            process(docID, text, STATE.PLAIN, CONTENT_TYPE.HTML, context);

            // get images
            //TODO: index image
            // 1) simply store all the images with doc (how to do only once)
            // 2) index with keywords: alt, image_url, etc
            Elements images = doc.getElementsByTag("img");
            for (Element i: images) {

                String imageSrc = i.attr("abs:src");
                String imageDocID = hash(imageSrc);

                // link image to the title of this page
                process(imageDocID, title, STATE.TITLE, CONTENT_TYPE.IMG, context);

                // parse image url
                process(imageDocID, parseURLWords(imageSrc), STATE.URL, CONTENT_TYPE.IMG, context);

                // parse image alt
                String imageAlt = i.attr("alt");
                process(imageDocID, imageAlt, STATE.META, CONTENT_TYPE.IMG, context);

                // parse image anchor text
                String imageAnchorText = i.text();
                process(imageDocID, imageAnchorText, STATE.ANCHORTEXT, CONTENT_TYPE.IMG, context);
            }
        }

        /**
         * PDF: just plain contents? can we get title or something?
         */
        private void processPDF(String docID, byte[] docContent, String baseurl, Context context) {
            if (docID == null || docID.length() != 64 || baseurl == null || baseurl.length() == 0) {
                return;
            }

            String text = null;
            try (PDDocument document = PDDocument.load(docContent)) {
                document.getClass();
                if (!document.isEncrypted()) {
                    PDFTextStripperByArea stripper = new PDFTextStripperByArea();
                    stripper.setSortByPosition(true);
                    PDFTextStripper tStripper = new PDFTextStripper();
                    text = tStripper.getText(document);

                }
                document.close();
            } catch (Exception e) {
                System.out.println("Error loading pdf file");
            }

            // process url
            process(docID, parseURLWords(baseurl), STATE.URL, CONTENT_TYPE.PDF, context);

            // process the content body
            process(docID, text, STATE.PLAIN, CONTENT_TYPE.PDF, context);
        }


        /**
         * TXT: just plaintext, will not parse links and images from txt
         */
        private void processTXT(String docID, byte[] docContent, String baseurl, Context context) {
            if (docID == null || docID.length() != 64 || baseurl == null || baseurl.length() == 0) {
                return;
            }

            // process url
            process(docID, parseURLWords(baseurl), STATE.URL, CONTENT_TYPE.TXT, context);

            // process the content body
            process(docID, new String(docContent), STATE.PLAIN, CONTENT_TYPE.TXT, context);
        }

        @Override
        public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {

            // key: filename, value: bytes read from s3
            // sanity check
            if (key == null || value == null) {
                return;
            }

            // deserialize JSON data
            String docID, baseurl, contentType;
            byte[] docContent;
            try {
                UrlObj docObj = mapper.readValue(new String(value.getBytes()), UrlObj.class);
                docID = String.valueOf(key);
                docContent = docObj.getContent();
                baseurl = docObj.getUrl();
                contentType = docObj.getContentType();
                log.info("Processing: " + baseurl);
            } catch (Exception e) {
                System.out.println("Error deserialize json string.");
                return;
            }

            // parse based on content type
            if (contentType.contains("html")) {
                processHTML(docID, docContent, baseurl, context);
            } else if (contentType.contains("pdf")) {
                processPDF(docID, docContent, baseurl, context);
            } else if (contentType.contains("text/plain")) {
                processTXT(docID, docContent, baseurl, context);
            }
        }
    }

    /** Indexer reducer class */
    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

        /** Total number of docs in S3 to compute IDF. */
        public static int TOTAL_FILE_NUM = 280000;

        /** Serialize and deserialize object into JSON. */
        static ObjectMapper mapper = new ObjectMapper();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //key: docID, value: DocInfo
            Map<String, DocInfo> docs = new HashMap<>();

            for (Text val : values) {
                System.out.println(val);
                String value = val.toString();
                String[] tokens = value.split(",");
                if (tokens.length != 4) {
                    continue;
                }
                String docID = tokens[0];
                String state = tokens[1];
                String contentType = tokens[2];
                String posIdx = tokens[3];

                if (!docs.containsKey(docID)) {
                    docs.put(docID, new DocInfo(docID));
                    docs.get(docID).setContentType(contentType);
                }

                DocInfo info = docs.get(docID);
                info.incTF();

                switch (state) {
                    case "PLAIN":
                        info.addPosition(posIdx);
                        break;
                    case "TITLE":
                        info.incTITLE();
                        break;
                    case "URL":
                        info.incURLWORD();
                        break;
                    case "ANCHORTEXT":
                        info.incANCHORTEXT();
                        break;
                    case "META":
                        info.incMETA();
                        break;
                    case "CAP":
                        info.incCAP();
                        break;
                }
            }

            // write as [key, list of docInfo]
//            if (docs.size() > 0) {
//                StringBuilder sb = new StringBuilder();
//                sb.append("[");
//                for (String docID: docs.keySet()) {
//                    DocInfo info = docs.get(docID);
//                    info.setIDF(Math.log(TOTAL_FILE_NUM / docs.size()));
//                    info.computeScore();
//                    sb.append(info.toString() + ",\n");
//                }
//                sb.replace(sb.length() - 2, sb.length() - 1, "]");
//                context.write(key, new Text(sb.toString()));
//            }

            // write as [key, docInfo]
            if (docs.size() > 0) {
                for (String docID: docs.keySet()) {
                    DocInfo info = docs.get(docID);
                    info.setIDF(Math.log(TOTAL_FILE_NUM * 1.0 / docs.size()));
                    info.computeScore();

                    // serialize
                    String jsonStr;
                    try {
                        jsonStr = mapper.writeValueAsString(info);
                        context.write(key, new Text(jsonStr));
                    } catch (Exception e) {
                        System.out.println("Error serialize JSON string");
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // set credentials within conf
        conf.set("fs.s3n.awsAccessKeyId", "myKey");
        conf.set("fs.s3n.awsSecretAccessKey", "myAccessKey");

        Job job = new Job(conf, "Index Job");

        // set jar
        job.setJarByClass(Index.class);

        // set mapper
        job.setMapperClass(IndexMapper.class);

        // set reducer
        job.setReducerClass(IndexReducer.class);

        // set input format: read the whole file
        job.setInputFormatClass(WholeFileInputFormat.class);

        // set output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set input path
        String bucket_name = "2018-spring-cis555-g09-htmlbucket";
        FileInputFormat.addInputPath(job, new Path("s3n://" + bucket_name +  "/"));
        FileOutputFormat.setOutputPath(job, new Path("./output"));
        job.waitForCompletion(true);
    }

}
