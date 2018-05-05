package searchengine.store;

import crawler.worker.storage.UrlObj;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.util.List;

public class DisplayUrlObj {
    private String url;
    private String title;
    private String host;
    private Document doc;
    private String contentType;
    private String content;
    private byte[] rawContent;
    private String briefContent;
    public DisplayUrlObj(){}
    public DisplayUrlObj(UrlObj urlObj){
        this.url = urlObj.getUrl();
        this.host = urlObj.getHost();
        this.content = new String(urlObj.getContent());
        this.rawContent = urlObj.getContent();
        this.doc = Jsoup.parse(content);
        this.briefContent = "None";
        this.contentType = urlObj.getContentType();
        // parse urlobj according to contenttype
        if(contentType.contains("img") || contentType.contains("pdf")){
            this.title = urlObj.getUrl();
        }
        else{
            Elements metaOgTitle = doc.select("meta[property=og:title]");
            this.title = doc.title();
            if (this.title.trim().length() == 0)
                this.title = metaOgTitle.attr("content");
        }
        System.out.println(this.rawContent);
    }
    public Document getDoc(){
        return this.doc;
    }
    public boolean isOkToDisplay(DisplayUrlObj compareBase, boolean childProof){
        if(compareBase == null) return false;
        Document compareBasedoc = compareBase.getDoc();
        return !CheckDisplayUrlObj.sameDOMHTML(this.doc,compareBasedoc,childProof);

    }
    public String getTitle(){
        return this.title;
    }
    public String getHost(){
        return this.host;
    }
    public String getContentType(){
        return this.contentType;
    }
    public String getUrl(){
        return this.url;
    }
    // make the keyword highlighted
    public void setStrongFont(String keyword){
        if (!this.briefContent.equals("None")) {
            return;
        }
        int keyIdx = -1;

        if(this.contentType.contains("pdf")){
            String text = null;
            try (PDDocument document = PDDocument.load(this.rawContent)) {
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
            keyIdx = text.indexOf(keyword);
            this.briefContent = text;
        }
        else {
            keyIdx = this.doc.text().indexOf(keyword);
            this.briefContent = this.doc.text();
        }
        if(keyIdx == -1){
            this.briefContent = "None";
            return;
        }

        int spaceIdx = this.briefContent.indexOf(" ",keyIdx+keyword.length());
        int startIdx = Math.max(0,keyIdx - 50);
        int endIdx = Math.min(spaceIdx+50,this.briefContent.length());
        this.briefContent = "..." + this.briefContent.substring(startIdx,keyIdx) + "<Strong>"
                    + this.briefContent.substring(keyIdx,spaceIdx) + "</Strong>" + this.briefContent.substring(spaceIdx,endIdx) + "...";
        return;
    }
    public String getBriefContent(){
        return this.briefContent;
    }
}
