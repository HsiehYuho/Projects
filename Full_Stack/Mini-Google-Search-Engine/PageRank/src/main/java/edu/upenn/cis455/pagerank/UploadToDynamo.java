package edu.upenn.cis455.pagerank;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;


public class UploadToDynamo {

    /** DynamoDB for pagerank result. */
    private static DynamoDB dynamoDB;

    /** Dynamo table for pagerank result. */
    private static Table table;

    /** Create table. */
    private static Table createTable(String tableName) {

        try {
            TableCollection<ListTablesResult> tables = dynamoDB.listTables();
            Iterator it = tables.iterator();
            while (it.hasNext()) {
                Table table = (Table) it.next();
                System.out.println(table.getTableName());
                if (table.getTableName().equals(tableName)) {
                    return dynamoDB.getTable(tableName);
                }
            }
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("key", KeyType.HASH)), // Partition key
                             // Sort key
                    Arrays.asList(new AttributeDefinition("key", ScalarAttributeType.S)),
                    new ProvisionedThroughput(100L, 100L));
            table.waitForActive();
            return table;
        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) throws IOException {


        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path("hdfs:///user/hadoop/finalOutput-demo6"));  // you need to pass in your hdfs path

        dynamoDB = new DynamoDB(client);
        table = createTable("PageRank");
        if (table == null) return;

        int counter = 0;
        try {
        for (int i=0;i<status.length;i++) {

            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));

                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] pair = line.split("\t");
                        if (pair.length != 2) {
                            continue;
                        }

                        String key = pair[0];
                        Double value =  Double.parseDouble(pair[1]);
                        if (key.equals("")) continue;

                        Item item = new Item()
                                    .withPrimaryKey("key", key)
                                    .withNumber("score", value)
                                    .withNumber("feedback", 1);

                            table.putItem(item);
                            System.out.println(counter++ + " PutItem succeeded: " + key + " and " + value);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error reading finalOutput.txt");
                }
            }

}
