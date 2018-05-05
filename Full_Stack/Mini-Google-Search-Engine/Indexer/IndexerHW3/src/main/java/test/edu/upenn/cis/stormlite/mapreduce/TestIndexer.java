/**********************************************************************/
/* This is a simple test script for indexer
 * which can be run to check mapper
 * Created April 2018
 * CIS 555 (Internet & Web Systems), Prof. Andreas
 * University of Pennsylvania
 * @version: 04/20/2018 */
/**********************************************************************/
package test.edu.upenn.cis.stormlite.mapreduce;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.log4j.BasicConfigurator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;

public class TestIndexer {

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

    //TODO: how to store excerpt info, use position?

    /** Determine how to get visible contents. </>*/
    static double TEXT_THRESHOLD = 0.2;

    /** Load stop words list in-memory. */
    private static Set<String> stopwordsLst = new HashSet();

    /** Load high-frequency bigram list in-memory. */
    private static Set<String> bigramLst = new HashSet();

    /** Load high-frequency trigram list in-memory. */
    private static Set<String> trigramLst = new HashSet();

    /** Set up Stanford coreNLP parser. */
    private static StanfordCoreNLP pipeline = null;

    /** Hold the states of index. */
    public enum STATE {TITLE, URL, ANCHORTEXT, META, PLAIN, CAP, IMAGE}

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
    }

    /**
     * Hash with SHA-256
     */
    private static String hash(String text) {
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
    private static String parseURLWords(String url) {
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
     */
    private static void process(String docID, String text, STATE state) {
        if (docID == null || docID.length() == 0 || text == null || text.length() == 0) {
            return;
        }
        // create a document object
        CoreDocument document = new CoreDocument(text);

        // annnotate the document
        pipeline.annotate(document);

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
            if (!stopwordsLst.contains(unigram.toLowerCase()) && unigram.matches("[a-zA-Z0-9]+[-.@&]*[a-zA-Z0-9-.@&]+")) {
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
                    System.out.println(lemma + ":" + docID + "," + STATE.CAP + "," + i);
                } else {
                    System.out.println(lemma + ":" + docID + ","+ state + "," + i);
                }
            }

            // bigram (case-incensitive, not lemmatized)
            if (bigram != null) {
                bigram = bigram.toLowerCase().trim();
                if (bigramLst.contains(bigram)) {
                    System.out.println(bigram + ":" + docID + ","+ state + "," + i);
                }
            }

            // trigram (case-incensitive, not lemmatized)
            if (trigram != null) {
                trigram = trigram.toLowerCase().trim();
                if (trigramLst.contains(trigram)) {
                    System.out.println(trigram + ":" + docID + ","+ state + "," + i);
                }
            }
        }
    }

    /**
     * HTML: have formats
     */
    private static void processHTML(File file, String baseurl) {

        if (file == null || baseurl == null || baseurl.length() == 0) {
            return;
        }

        String docID = hash(baseurl);
        if (docID == null) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (Exception e) {
            System.out.println("Error reading html file. ");
            return;
        }

        // parse HTML with JSoup
        Document doc = Jsoup.parse(sb.toString(), baseurl);

        // get title
        String title = doc.title();
        process(docID, title, STATE.TITLE);

        // get url
        process(docID, parseURLWords(baseurl), STATE.URL);

        // get all links and its anchor text
        Elements links = doc.select("a[href]");
        for (Element link: links) {
            String url = link.attr("abs:href");
            String anchorText = link.text();    // key: url in href,
            process(hash(url), anchorText, STATE.ANCHORTEXT);
        }

        // get metadata: description
        String description = "";
        Elements metaDescription = doc.select("meta[name=description]");
        for (Element d: metaDescription) {
            description += d.attr("content");
        }
        process(docID, description, STATE.META);

        // get metadata: keywords
        String keywords = "";
        Elements metaKeywords = doc.select("meta[name=keywords]");
        for (Element k: metaKeywords) {
            keywords += k.attr("content");
        }
        process(docID, keywords, STATE.META);

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
        //process(docID, text, STATE.PLAIN);

        // get images
        //TODO: simply store all the images or filter by keywords?
        Elements images = doc.getElementsByTag("img");
        for (Element i: images) {

            // parse image url
            String imageSrc = i.attr("abs:src");
            process(docID, parseURLWords(imageSrc), STATE.IMAGE);

            // parse image alt
            String imageAlt = i.attr("alt");
            process(docID, imageAlt, STATE.IMAGE);

            // parse image anchor text
            String imageAnchorText = i.text();
            process(docID, imageAnchorText, STATE.IMAGE);
        }

    }

    /**
     * PDF: just plain contents? can we get title or something?
     */
    private static void processPDF(File file, String baseurl) {

        if (file == null || baseurl == null || baseurl.length() == 0) {
            return;
        }

        String docID = hash(baseurl);
        if (docID == null) {
            return;
        }

        String text = null;
        try (PDDocument document = PDDocument.load(file)) {
            document.getClass();
            if (!document.isEncrypted()) {
                PDFTextStripperByArea stripper = new PDFTextStripperByArea();
                stripper.setSortByPosition(true);
                PDFTextStripper tStripper = new PDFTextStripper();
                text = tStripper.getText(document);
                //System.out.println("Text:" + text);

            }
            document.close();
        } catch (Exception e) {
            System.out.println("Error loading pdf file");
        }

        // get url
        process(docID, parseURLWords(baseurl), STATE.URL);

        // process the content body
        process(docID, text, STATE.PLAIN);
    }


    /**
     * TXT: just plaintext, will not parse links and images from txt
     */
    private static void processTXT(File file, String baseurl) {
        if (file == null || baseurl == null || baseurl.length() == 0) {
            return;
        }

        String docID = hash(baseurl);
        if (docID == null) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (Exception e) {
            System.out.println("Error reading txt file. ");
            return;
        }

        // get url
        process(docID, parseURLWords(baseurl), STATE.URL);

        // process the content body
        process(docID, sb.toString(), STATE.PLAIN);
    }

    /**
     * Test with html, pdf, txt files
     */
    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        // prepare stopwords list, bigram list and trigram list
        init();

        //TODO: read from S3 API
        // index HTML pages
        File htmlfile = new File("./Chocolate_Wikipedia.html");
        String htmlbaseurl = "https://en.wikipedia.org/wiki/Chocolate";
        processHTML(htmlfile, htmlbaseurl);
//
//        // index PDF pages
//        File pdffile = new File("./Chord.pdf");
//        String pdfbaseurl = "https://something/Chord.pdf";
//        processPDF(pdffile, pdfbaseurl);

        // read TXT pages
//        File txtfile = new File("./harry_potter.txt");
//        String txtbaseurl = "https://something/harry_potter.txt";
//        processTXT(txtfile, txtbaseurl);

    }
}
