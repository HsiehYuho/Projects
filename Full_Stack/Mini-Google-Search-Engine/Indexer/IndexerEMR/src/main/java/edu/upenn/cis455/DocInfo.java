package edu.upenn.cis455;

public class DocInfo {

    /** Weights of different IR parameter. */
    public static int WEIGHT_TFIDF = 1;
    public static int WEIGHT_TITLE = 1;
    public static int WEIGHT_URL = 1;
    public static int WEIGHT_ANCHORTEXT = 1;
    public static int WEIGHT_META = 1;
    public static int WEIGHT_CAP = 1;

    String docID;
    String contentType;
    int tf;
    double idf;
    int title;
    int urlWord;
    int meta;
    int anchorText;
    int cap;
    double score;
    StringBuilder positionalIndex;

    public DocInfo() {}

    public DocInfo(String docID) {
        this.docID = docID;
        this.contentType = "HTML";
        this.tf = 0;
        this.idf = 0;
        this.title = 0;
        this.urlWord = 0;
        this.meta = 0;
        this.anchorText = 0;
        this.cap = 0;
        this.score = 0;
        positionalIndex = new StringBuilder();
        positionalIndex.append("[");
    }

    public void incTF() {
        this.tf++;
    }

    public void incTITLE() {
        this.title++;
    }

    public void incURLWORD() {
        this.urlWord++;
    }

    public void incANCHORTEXT() {
        this.anchorText++;
    }

    public void incCAP() {
        this.cap++;
    }

    public void incMETA() {
        this.cap++;
    }

    public void setIDF(double idf) {
        this.idf = idf;
    }

    public void setContentType(String type) {
        this.contentType = type;
    }

    public void computeScore() {
        this.score = WEIGHT_TFIDF * tf * idf +
                WEIGHT_TITLE * title +
                WEIGHT_URL * urlWord +
                WEIGHT_ANCHORTEXT * anchorText +
                WEIGHT_META * meta +
                WEIGHT_CAP * cap;
    }

    public void addPosition(String pos) {
        this.positionalIndex.append(pos + ",");
    }

    // getter method for jackson
    public String getDocID() {
        return docID;
    }

    public String getContentType() {
        return contentType;
    }

    public int getTF() {
        return tf;
    }

    public double getIDF() {
        return idf;
    }

    public int getTitle() {
        return title;
    }

    public int getUrlWord() {
        return urlWord;
    }

    public int getMeta() {
        return meta;
    }

    public int getAnchorText() {
        return anchorText;
    }

    public int getCap() {
        return cap;
    }

    public double getScore() {
        return score;
    }

    public String getPositionalIndex() {
        if (positionalIndex.length() > 1) {
            positionalIndex.deleteCharAt(positionalIndex.length() - 1);
        }
        positionalIndex.append("]");
        return positionalIndex.toString();
    }

    @Override
    public String toString() {

        // generate JSON string
        StringBuilder sb = new StringBuilder();

        // version1: pretty + [key, list of docInfo]
//        sb.append("\t{\n" +
//                "\t\t\"docID\": \"" + docID + "\",\n" +
//                "\t\t\"Content-Type\": \"" + contentType + "\",\n" +
//                "\t\t\"TF\": " + tf + ",\n" +
//                "\t\t\"IDF\": " + idf + ",\n" +
//                "\t\t\"TITLE\": " + title + ",\n" +
//                "\t\t\"URL\": " + urlWord + ",\n" +
//                "\t\t\"ANCHORTEXT\": " + anchorText + ",\n" +
//                "\t\t\"META\": " + meta + ",\n" +
//                "\t\t\"CAP\": " + cap + ",\n" +
//                "\t\t\"score\": " + score + ",\n" +
//                "\t\t\"positions\": [" + positionalIndex + "]\n" +
//                "\t}");

        // version2: space efficient + [key, list of docInfo]
        sb.append("{\"docID\": \"" + docID + "\", " +
                "\"Content-Type\": \"" + contentType + "\", " +
                "\"TF\": " + tf + ", " +
                "\"IDF\": " + idf + ", " +
                "\"TITLE\": " + title + ", " +
                "\"URL\": " + urlWord + ", " +
                "\"ANCHORTEXT\": " + anchorText + ", " +
                "\"META\": " + meta + ", " +
                "\"CAP\": " + cap + ", " +
                "\"score\": " + score + ", " +
                "\"positions\": " + getPositionalIndex() + "}");

        // version3: space efficient + [key, docInfo]
//        sb.append("\"docID\": \"" + docID + "\", " +
//                "\"Content-Type\": \"" + contentType + "\", " +
//                "\"TF\": " + tf + ", " +
//                "\"IDF\": " + idf + ", " +
//                "\"TITLE\": " + title + ", " +
//                "\"URL\": " + urlWord + ", " +
//                "\"ANCHORTEXT\": " + anchorText + ", " +
//                "\"META\": " + meta + ", " +
//                "\"CAP\": " + cap + ", " +
//                "\"score\": " + score + ", " +
//                "\"positions\": [" + positionalIndex + "]");
        return sb.toString();
    }
}