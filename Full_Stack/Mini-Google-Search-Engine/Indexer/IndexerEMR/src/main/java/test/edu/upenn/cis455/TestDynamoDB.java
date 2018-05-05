package test.edu.upenn.cis455;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.HashMap;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis455.UrlObj;

public class TestDynamoDB {

    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // connect to dynamodb
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider("lanqingy"))
                .withRegion("us-east-1").build();

        DynamoDB dynamoDB = new DynamoDB(client);

        // get amazon credentials from ~/.aws
        AWSCredentials credentials = new ProfileCredentialsProvider("default").getCredentials();

        // connect to S3
        final AmazonS3 s3 = new AmazonS3Client(credentials);

        // a simple example for querying: key == "sport" and descending order of score
        Table table = dynamoDB.getTable("Index1");

        HashMap<String, String> nameMap = new HashMap<>();
        nameMap.put("#key", "key");
        nameMap.put("#type", "ContentType");

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put(":string", "food");
        valueMap.put(":type", "IMG");

        QuerySpec querySpec = new QuerySpec()
                //.withProjectionExpression("docID")
                .withKeyConditionExpression("#key = :string")
                .withFilterExpression("#type = :type")
                .withScanIndexForward(false)
                .withNameMap(nameMap)
                .withValueMap(valueMap)
                .withMaxResultSize(100);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {
            System.out.println("Index key is: " + valueMap.get(":string"));
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                //System.out.println(item.toJSONPretty());
                System.out.println("found image: " + item.getString("docID"));

                // get the value with the key from the specified bucket
                try {
                    S3Object object = s3.getObject(new GetObjectRequest("2018-spring-cis555-g09-img-bucket", item.getString("docID")));
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
                    System.out.println("~~~" + docObj.getContentType());
                } catch (Exception e) {
                    //System.out.println("image not found in s3");
                }


                //System.out.println(item.getNumber("score"));
            }
        } catch (Exception e) {
            System.err.println("Unable to query " + valueMap.get(":string") + " as index key");
            System.err.println(e.getMessage());
        }
    }
}
