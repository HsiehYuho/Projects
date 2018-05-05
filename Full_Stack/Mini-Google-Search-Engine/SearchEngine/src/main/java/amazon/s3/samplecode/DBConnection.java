package amazon.s3.samplecode;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import crawler.worker.storage.UrlObj;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class DBConnection {
    public static UrlObj getElementFromS3(String key, String bucketName) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        // key = URLInfo.hash(key);
        S3Object object = null;
        final AmazonS3 s3 = new AmazonS3Client(credentials);
        try{
            object  = s3.getObject(
                    new GetObjectRequest(bucketName, key));
        }
        catch(Exception e){
            System.out.println("No such key");
            return null;
        }
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
        objectData.close();
        return urlObj;
    }



}
