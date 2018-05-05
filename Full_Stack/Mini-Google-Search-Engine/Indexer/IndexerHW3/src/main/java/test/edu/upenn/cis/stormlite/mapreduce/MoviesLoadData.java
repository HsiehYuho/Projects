package test.edu.upenn.cis.stormlite.mapreduce;

import java.io.File;
import java.util.Iterator;
import java.util.Arrays;
import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MoviesLoadData {

    /** Create table. */
    private static void createTable(DynamoDB dynamoDB, String tableName) {
        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("year", KeyType.HASH), // Partition// key
                            new KeySchemaElement("title", KeyType.RANGE)), // Sort key
                    Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
                            new AttributeDefinition("title", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }

    /** Dump json data into dynamoDB table. */
    private static void dumpIntoTable(DynamoDB dynamoDB, String tableName, String filePath) {
        try {
            Table table = dynamoDB.getTable(tableName);

            JsonParser parser = new JsonFactory().createParser(new File(filePath));
            JsonNode rootNode = new ObjectMapper().readTree(parser);
            Iterator<JsonNode> iter = rootNode.iterator();

            ObjectNode currentNode;

            while (iter.hasNext()) {
                currentNode = (ObjectNode) iter.next();

                int year = currentNode.path("year").asInt();
                String title = currentNode.path("title").asText();

                try {
                    table.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON("info",
                            currentNode.path("info").toString()));
                    System.out.println("PutItem succeeded: " + year + " " + title);

                }
                catch (Exception e) {
                    System.err.println("Unable to add movie: " + year + " " + title);
                    System.err.println(e.getMessage());
                    break;
                }
            }
            parser.close();
        } catch (Exception e) {
            return;
        }
    }

    public static void main(String[] args) throws Exception {

        // connect to dynamodb
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider("sam"))
                .withRegion("us-east-1").build();

        DynamoDB dynamoDB = new DynamoDB(client);

        // create Movies table (already created!)
        // createTable(dynamoDB, "Movies");

        // dump json file into dynamo (already dumped!)
        // dumpIntoTable(dynamoDB, "Movies", "moviedata.json");

        // query data from dynamo
        Table table = dynamoDB.getTable("Movies");
        HashMap<String, String> nameMap = new HashMap<>();
        nameMap.put("#yr", "year");

        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put(":yyyy", 1985);

        QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#yr = :yyyy").withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {
            System.out.println("Movies from 1985");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") + ": " + item.getString("title"));
            }
        } catch (Exception e) {
            System.err.println("Unable to query movies from 1985");
            System.err.println(e.getMessage());
        }

        valueMap.put(":yyyy", 1992);
        valueMap.put(":letter1", "A");
        valueMap.put(":letter2", "L");

        querySpec.withProjectionExpression("#yr, title, info.genres, info.actors[0]")
                .withKeyConditionExpression("#yr = :yyyy and title between :letter1 and :letter2").withNameMap(nameMap)
                .withValueMap(valueMap);

        try {
            System.out.println("Movies from 1992 - titles A-L, with genres and lead actor");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") + ": " + item.getString("title") + " " + item.getMap("info"));
            }
        } catch (Exception e) {
            System.err.println("Unable to query movies from 1992:");
            System.err.println(e.getMessage());
        }
    }
}
