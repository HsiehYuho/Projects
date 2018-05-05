package edu.upenn.cis455.pagerank;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;

import java.math.BigDecimal;



public class PageRankRetriever {    

    /** DynamoDB for pagerank result. */
    private static DynamoDB dynamoDB = new DynamoDB(client);

    /**
    * Get pagerank score from dynamodb by hostname (key)
     */
    public static BigDecimal getPRScore(String tableName, String key){
        try{
            Table table = dynamoDB.getTable(tableName);
            GetItemSpec spec = new GetItemSpec().withPrimaryKey("key", key);
            Item outcome = table.getItem(spec);
            return (BigDecimal) outcome.get("score");
        } catch (NullPointerException e) {
            return BigDecimal.valueOf(0.01);
        }
    }

    /**
    * Get feedback score from dynamodb by hostname (key)
    * Default value is 1.
     */
    public static BigDecimal getFeedBack(String tableName, String key){
        try{
            Table table = dynamoDB.getTable(tableName);
            GetItemSpec spec = new GetItemSpec().withPrimaryKey("key", key);
            Item outcome = table.getItem(spec);
            return (BigDecimal) outcome.get("feedback");
        } catch (NullPointerException e) {
            return BigDecimal.valueOf(1);
        }
    }

    /**
    * Update the feedback score
     */
    public static void setFeedBack(String tableName, String key, int feedback){
        Table table = dynamoDB.getTable(tableName);
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("key", key).
                withUpdateExpression("set feedback = :val")
                .withValueMap(new ValueMap().withNumber(":val", feedback))
                .withReturnValues(ReturnValue.UPDATED_NEW);
        try {
            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
        }
        catch (Exception e) {
            System.err.println("Unable to update item");
            System.err.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception{

    }
}
