package test.edu.upenn.cis.stormlite.mapreduce;

import java.util.Iterator;
import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public class TestDynamoDB {

    public static void main(String[] args) throws Exception {

        // connect to dynamodb
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider("lanqingy"))
                .withRegion("us-east-1").build();

        DynamoDB dynamoDB = new DynamoDB(client);

        // a simple example for querying: key == "sport" and descending order of score
        Table table = dynamoDB.getTable("Index2");

        HashMap<String, String> nameMap = new HashMap<>();
        nameMap.put("#key", "key");

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put(":string", "syzygy");

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#key = :string")
                .withScanIndexForward(false)
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {
            System.out.println("Index key is: " + valueMap.get(":string"));
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getString("docID"));
            }
        } catch (Exception e) {
            System.err.println("Unable to query cookie as index key");
            System.err.println(e.getMessage());
        }
    }
}
