/**********************************************************************/
/* This is an indexer for a search engine project
 * Implemented with Storm + MapReduce
 * Created April 2018
 * CIS 555 (Internet & Web Systems), Prof. Andreas
 * University of Pennsylvania
 * @version: 04/20/2018 */
/**********************************************************************/
package edu.upenn.cis455.mapreduce.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.upenn.cis455.amazon.s3.samplecode.UrlObj;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerMain;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.DocInfo;

public class IndexJob implements Job {

    // Indexer workflow:
    // Mapper:
    // 1. read in HTML, XML, PDF, TXT
    // 2. parse visible contents:
    //    HTML: JSoup
    //    XML: xml parser
    //    PDF: PDFBox
    //    TXT: plain string
    // 3. tokenize with NLP: stemming and lemmatizing
    // 4. emit(word, docID, title, 1) or
    //    emit(word, docID, anchor text, 1) or
    //    emit(word, docID, url, 1) or
    //    emit(word, docID, plain, 1) or
    //    emit(word, docID, cap, 1) or
    //    emit(word, docID, excerpt, paragraph with keyword) or
    //    emit(word, docID, font, 1) or (how do I know the font?)
    // Reducer:
    //    tf = title + anchor text + url + plain + cap
    //    idf
    //    bonus score = title * 0.5 +
    // just a normal reducer

    /** Hold the states of index key. */
    public static enum STATE {TITLE, URL, ANCHORTEXT, META, PLAIN, CAP}

    /** Hold the content-type of the document. */
    public static enum CONTENT_TYPE {HTML, PDF, TXT, IMG}

    /** Determine how to get visible contents. </>*/
    public static double TEXT_THRESHOLD = 0.2;

    /** Total number of docs in S3 to compute IDF. */
    public static int TOTAL_FILE_NUM = 280000;

    /** Serialize and deserialize object into JSON. */
    static ObjectMapper mapper = new ObjectMapper();

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
        if (docID == null || docID.length() == 0 || text == null || text.length() == 0) {
            return;
        }

        // create a document object
        CoreDocument document = new CoreDocument(text);

        // annnotate the document
        WorkerMain.pipeline.annotate(document);

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
            if (!WorkerMain.stopwordsLst.contains(unigram.toLowerCase()) && unigram.matches("[a-zA-Z0-9]+[-.@&]*[a-zA-Z0-9-.@&]+")) {
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
                if (isCapital) {
                    context.write(lemma, docID + "," + STATE.CAP + "," + type + "," + i);
                } else {
                    context.write(lemma, docID + "," + state + "," + type + "," + i);
                }
            }

            // bigram (case-incensitive, not lemmatized)
            if (bigram != null) {
                bigram = bigram.toLowerCase().trim();
                if (WorkerMain.bigramLst.contains(bigram)) {
                    context.write(bigram, docID + "," + state + "," + type + "," + i);
                }
            }

            // trigram (case-incensitive, not lemmatized)
//            if (trigram != null) {
//                trigram = trigram.toLowerCase().trim();
//                if (WorkerMain.trigramLst.contains(trigram)) {
//                    context.write(trigram, docID + "," + state + "," + type + "," + i);
//                }
//            }
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

    public void map(String key, String value, Context context) {

        // sanity check
        if (key == null || key.length() != 64 || value == null || value.length() == 0) {
            return;
        }

        // deserialize JSON data
        String docID, baseurl, contentType;
        byte[] docContent;
        try {
            UrlObj docObj = mapper.readValue(value, UrlObj.class);
            docID = key;
            docContent = docObj.getContent();
            baseurl = docObj.getUrl();
            contentType = docObj.getContentType();
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

    public void reduce(String key, Iterator<String> values, Context context) {
        Map<String, DocInfo> map = new HashMap<>(); //key: docID, value: DocInfo
        while (values.hasNext()) {
            String value = values.next();
            String[] tokens = value.split(",");
            if (tokens.length != 4) {
                continue;
            }
            String docID = tokens[0];
            String state = tokens[1];
            String contentType = tokens[2];
            String posIdx = tokens[3];

//            if (contentType.equals("IMG")) {
//                context.write(key, docID);
//                continue;
//            }

            if (!map.containsKey(docID)) {
                map.put(docID, new DocInfo(docID));
                map.get(docID).setContentType(contentType);
            }

            DocInfo info = map.get(docID);
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
//        if (map.size() > 0) {
//            StringBuilder sb = new StringBuilder();
//            for (String docID: map.keySet()) {
//                DocInfo info = map.get(docID);
//                info.setIDF(Math.log(TOTAL_FILE_NUM / map.size()));
//                info.computeScore();
//                sb.append(info.toString() + ",\n");
//            }
//            sb.replace(sb.length() - 2, sb.length(), "\n");
//            context.write(key, sb.toString());
//        }

        // write as [key, docInfo]
        if (map.size() > 0) {
            for (String docID: map.keySet()) {
                DocInfo info = map.get(docID);
                info.setIDF(Math.log(TOTAL_FILE_NUM * 1.0 / map.size()));
                info.computeScore();

                // serialize
                String jsonStr;
                try {
                    jsonStr = mapper.writeValueAsString(info);
                    context.write(key, jsonStr);
                } catch (Exception e) {
                    System.out.println("Error serialize JSON string");
                }
            }
        }
    }
}