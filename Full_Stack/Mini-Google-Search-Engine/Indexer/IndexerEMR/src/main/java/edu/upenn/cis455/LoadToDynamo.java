package edu.upenn.cis455;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class LoadToDynamo {

    /** AWS credentials for DynamoDB. */
    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider("lanqingy"))
            .withRegion("us-east-1").build();

    /** DynamoDB for index result. */
    private static DynamoDB dynamoDB;

    /** Dynamo table for index result. */
    private static Table table;

    /** JSON serialize and deserialize. */
    private static ObjectMapper mapper = new ObjectMapper();

    private static String tableName = "Index3";

    private static String inputpath = "./output";

    /** Get parameters from command line. */
    private static void getParameters(String[] args) {
        if (args.length == 0) {
            System.out.println("*** Author: Lanqing Yang (lanqingy)");
            System.exit(1);
        } else {
            try {
                tableName = args[0];
                inputpath = args[1];
            } catch (Exception e) {
                System.err.println("Please specify:\n" +
                        "1) IP address and port number of the master\n" +
                        "2) path to the storage directory of the worker\n" +
                        "3) port number on which the worker should listen for commands from the master.");
                System.exit(1);
            }
        }
    }

    /** Create table. */
    private static Table createTable(String tableName) {
        try {
            TableCollection<ListTablesResult> tables = dynamoDB.listTables();
            Iterator it = tables.iterator();
            while (it.hasNext()) {
                Table table = (Table) it.next();
                if (table.getTableName().equals(tableName)) {
                    return dynamoDB.getTable(tableName);
                }
            }
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("key", KeyType.HASH), // Partition key
                            new KeySchemaElement("score", KeyType.RANGE)), // Sort key
                    Arrays.asList(new AttributeDefinition("key", ScalarAttributeType.S),
                            new AttributeDefinition("score", ScalarAttributeType.N)),
                    new ProvisionedThroughput(100L, 500L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
            return table;
        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) throws IOException {

        getParameters(args);

        dynamoDB = new DynamoDB(client);
        table = createTable(tableName);
        if (table == null) return;

        File envHome = new File(inputpath);
        if (!envHome.exists())  return;

        int counter = 0;
        for (File file : envHome.listFiles()) {
            if (!file.isDirectory() && !file.isHidden() && !file.getName().equals("_SUCCESS")) {
                try {
                    System.out.println("processing file: " + file);
                    BufferedReader br = new BufferedReader(new FileReader(file));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] pair = line.split("\t");
                        if (pair.length != 2) {
                            continue;
                        }

                        String key = pair[0];
                        String value =  pair[1];

                        try {
                            // deserialize
                            DocInfo jsonStr = mapper.readValue(value, DocInfo.class);

                            // Build the item
                            Item item = new Item()
                                    .withPrimaryKey("key", key, "score", jsonStr.getScore())
                                    .withString("docID", jsonStr.getDocID())
                                    .withString("ContentType",jsonStr.getContentType())
                                    .withNumber("TF", jsonStr.getTF())
                                    .withNumber("IDF", jsonStr.getIDF())
                                    .withNumber("TITLE", jsonStr.getTitle())
                                    .withNumber("URL", jsonStr.getUrlWord())
                                    .withNumber("ANCHORTEXT", jsonStr.getAnchorText())
                                    .withNumber("META", jsonStr.getMeta())
                                    .withNumber("CAP", jsonStr.getCap())
                                    .withString("positions", jsonStr.getPositionalIndex());

                            table.putItem(item);
                            System.out.println(counter++ + " PutItem succeeded: " + key);
                        } catch (Exception e) {
                            System.out.println("PutItem error: " + key);
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error reading output.txt");
                }
            }
        }
    }
}

