package amazon.s3.samplecode;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import crawler.worker.info.URLInfo;
import crawler.worker.storage.UrlObj;

import java.io.*;

public class TestGetObjectFromS3 {
    static String HTML_BUCKET = "2018-spring-cis555-g09-htmlbucket";
    static String PR_BUCKET = "2018-spring-cis555-g09-prbucket";

    static String IMG_BUCKET = "2018-spring-cis555-g09-imgbucket";
    static String HTML_NEWS_BUCKET = "2018-spring-cis555-g09-news-html-bucket";
    private ObjectMapper mapper = new ObjectMapper();

    public void listAllElementsFromS3(String bucketName){
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        final AmazonS3 s3 = new AmazonS3Client(credentials);
        // s3 will return limited number of keys at once, not all the keys
        final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
        int i = 0;
        do {
            result = s3.listObjectsV2(req);
            for (S3ObjectSummary objectSummary :
                    result.getObjectSummaries()) {
                System.out.println( i + " - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() +
                        ")");
                i ++;
            }
            System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
            req.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated() == true );
    }

    public void getElementFromS3(String key, String bucketName) throws IOException {
        key = URLInfo.hash(key);

        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        final AmazonS3 s3 = new AmazonS3Client(credentials);
        S3Object object = s3.getObject(
                new GetObjectRequest(bucketName, key));
        InputStream objectData = object.getObjectContent();
        // Process the objectData stream.
        BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
        }
        String jsonString = out.toString();
        UrlObj urlObj = mapper.readValue(jsonString,UrlObj.class);
        // html work
        if(bucketName.equals(HTML_BUCKET)){
            FileOutputStream stream = new FileOutputStream("/Users/Yuho/Desktop/test.txt");
            try {
                stream.write(urlObj.getContent());
            } finally {
                stream.close();
            }
        }
        // img works
        if(bucketName.equals(IMG_BUCKET)){
            FileOutputStream stream = new FileOutputStream("/Users/Yuho/Desktop/test.gif");
            try {
                stream.write(urlObj.getContent());
            } finally {
                stream.close();
            }
        }
        objectData.close();
    }


    public static void main(String[] args) throws InterruptedException {
        TestGetObjectFromS3 t = new TestGetObjectFromS3();
        // list all the elements
        t.listAllElementsFromS3(PR_BUCKET);

        // One of the key : 0005cb0de7d5a6950f6381c411f0f004f0ceb2339ed66910ed1c4c43054ee752
        // Another key : 	000896aac1fb4687916d44733fd4141803e837c917627eb77a354fe876b4333b
        // t.getElementFromS3("key","bucketName");

        // use the mapper to extract stuff out, cast to UrlObj.class

    }
}
