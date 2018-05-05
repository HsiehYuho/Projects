package test.edu.upenn.cis455;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.Properties;

public class TestNLP {

    public static StanfordCoreNLP pipeline = null;

    public static void main(String[] args) {
        // set up pipeline properties
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        props.setProperty("tokenize.language", "English");

        // build pipeline
        pipeline = new StanfordCoreNLP(props);

        // create a document object
        CoreDocument document = new CoreDocument("eagles");

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
            String lemma = token.lemma().toLowerCase();
            System.out.println(lemma);
        }
    }
}
