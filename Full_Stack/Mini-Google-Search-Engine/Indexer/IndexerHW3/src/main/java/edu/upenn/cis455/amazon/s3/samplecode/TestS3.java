package edu.upenn.cis455.amazon.s3.samplecode;

import java.io.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.BasicConfigurator;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;

public class TestS3 {

    //static String bucket_name = "2018-spring-cis555-g09-pdf-bucket";
    static String bucket_name = "2018-spring-cis555-g09-htmlbucket";
    //static String bucket_name = "2018-spring-cis555-g09-img-bucket";

    static String key_name = "527a881a650ecbbc2d6f59b7249d28865afac3b8d61ef0f13f8adbe8ac8d1a5c";

    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        //BasicConfigurator.configure();

        // get amazon credentials from ~/.aws
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();

        // connect to S3
        final AmazonS3 s3 = new AmazonS3Client(credentials);

        // get the value with the key from the specified bucket
        S3Object object = s3.getObject(new GetObjectRequest(bucket_name, key_name));
        InputStream inputStream = object.getObjectContent();

        // process the objectData stream
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        UrlObj docObj = mapper.readValue(sb.toString(), UrlObj.class);
        inputStream.close();

        // get content-type
        System.out.println(docObj.getContentType());

        // get base-url
        System.out.println(docObj.getUrl());

        // get content
        //System.out.println(new String(docObj.getContent()));

        // get pdf content
//        String text = null;
//        try (PDDocument document = PDDocument.load(docObj.getContent())) {
//            document.getClass();
//            if (!document.isEncrypted()) {
//                PDFTextStripperByArea stripper = new PDFTextStripperByArea();
//                stripper.setSortByPosition(true);
//                PDFTextStripper tStripper = new PDFTextStripper();
//                text = tStripper.getText(document);
//                System.out.println("Text:" + text);
//
//            }
//            document.close();
//        } catch (Exception e) {
//            System.out.println("Error loading pdf file");
//        }
    }
}