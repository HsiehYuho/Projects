package test.edu.upenn.cis.stormlite.mapreduce;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class TestListS3Bucket {

    public static String bucketName = "2018-spring-cis555-g09-news-html-b";
    //public static String bucketName = "2018-spring-cis555-g09-pdf-bucket";
    //public static String bucketName = "2018-spring-cis555-g09-test-indexer";
    //public static String bucketName = "2018-spring-cis555-g09-html-bucket-demo";

    //private static String docListPath = "./worker1/input/doclist.txt";

    public static void main(String[] args) throws IOException {
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider("default"));

//        File doclist = new File(docListPath);
//        if (doclist.exists()) {
//            doclist.delete();
//        }
//
//        BufferedWriter writer = new BufferedWriter(new FileWriter(doclist));

        int i = 0;
        final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
        do {
            result = s3client.listObjectsV2(req);
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                System.out.println(i++ + " - " + objectSummary.getKey());
        //        writer.write(objectSummary.getKey() + "\n");
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated() == true);

       // writer.close();
    }
}